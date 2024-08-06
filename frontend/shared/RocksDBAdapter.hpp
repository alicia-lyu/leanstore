#pragma once
#include <rocksdb/statistics.h>
#include "Adapter.hpp"
#include "Types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <unordered_map>

struct RocksDB {
   union {
      rocksdb::DB* db = nullptr;
      rocksdb::TransactionDB* tx_db;
      rocksdb::OptimisticTransactionDB* optimistic_transaction_db;
   };
   static thread_local rocksdb::Transaction* txn;
   rocksdb::WriteOptions wo;
   rocksdb::ReadOptions ro;
   enum class DB_TYPE : u8 { DB, TransactionDB, OptimisticDB };
   const DB_TYPE type;
   // -------------------------------------------------------------------------------------
   RocksDB(DB_TYPE type = DB_TYPE::DB) : type(type)
   {
      wo.disableWAL = true;
      wo.sync = false;
      // -------------------------------------------------------------------------------------
      rocksdb::Options db_options;
      db_options.use_direct_reads = true;
      db_options.use_direct_io_for_flush_and_compaction = true;
      db_options.db_write_buffer_size = 0;  // disabled
      // db_options.write_buffer_size = 64 * 1024 * 1024; keep the default
      db_options.create_if_missing = true;
      db_options.manual_wal_flush = true;
      db_options.compression = rocksdb::CompressionType::kNoCompression;
      // db_options.OptimizeLevelStyleCompaction(FLAGS_dram_gib * 1024 * 1024 * 1024);
      db_options.row_cache = rocksdb::NewLRUCache(FLAGS_dram_gib * 1024 * 1024 * 1024);
      db_options.statistics = rocksdb::CreateDBStatistics();
      db_options.stats_dump_period_sec = 1;
      rocksdb::Status s;
      if (type == DB_TYPE::DB) {
         s = rocksdb::DB::Open(db_options, FLAGS_ssd_path, &db);
      } else if (type == DB_TYPE::TransactionDB) {
         s = rocksdb::TransactionDB::Open(db_options, {}, FLAGS_ssd_path, &tx_db);
      } else if (type == DB_TYPE::OptimisticDB) {
         s = rocksdb::OptimisticTransactionDB::Open(db_options, FLAGS_ssd_path, &optimistic_transaction_db);
      }
      if (!s.ok())
         cerr << s.ToString() << endl;
      assert(s.ok());
   }

   ~RocksDB() { delete db; }
   void startTX()
   {
      rocksdb::Status s;
      if (type == DB_TYPE::TransactionDB) {
         txn = tx_db->BeginTransaction(wo, {});
      } else if (type == DB_TYPE::OptimisticDB) {
         txn = optimistic_transaction_db->BeginTransaction({}, {});
      } else {
      }
   }
   void commitTX()
   {
      if (type != DB_TYPE::DB) {
         rocksdb::Status s;
         s = txn->Commit();
         delete txn;
         txn = nullptr;
      }
   }
   void prepareThread() {}

   std::unordered_map<std::string, uint64_t> getSizes()
   {
      rocksdb::TablePropertiesCollection props;
      db->GetPropertiesOfAllTables(&props);
      std::unordered_map<std::string, uint64_t> sizes;
      for (auto& prop : props) {
         sizes[prop.first] = prop.second->data_size;
         std::cout << prop.first << " " << prop.second->data_size << std::endl;
      }
      return sizes;
   }

   void startProfilingThread(std::atomic<u64>& running_threads_counter,
                             std::atomic<u64>& keep_running,
                             std::atomic<u64>* thread_committed,
                             std::atomic<u64>* thread_aborted,
                             bool& print_header)
   {
      std::thread profiling_thread([&]() {
         leanstore::utils::pinThisThread(((FLAGS_pin_threads) ? FLAGS_worker_threads : 0) + FLAGS_wal + FLAGS_pp_threads);
         running_threads_counter++;

         u64 time = 0;
         std::ofstream::openmode open_flags;
         if (FLAGS_csv_truncate) {
            open_flags = ios::trunc;
         } else {
            open_flags = ios::app;
         }
         std::ofstream csv(FLAGS_csv_path + "_sum.csv", open_flags);
         csv.seekp(0, ios::end);
         csv << std::setprecision(2) << std::fixed;

         if (print_header) {
            csv << "t,tag,oltp_committed,oltp_aborted,SSTReads/TX,SST/Writes/TX" << endl;
         }
         uint64_t sst_read_prev = 0, sst_write_prev = 0;
         while (keep_running) {
            csv << time++ << "," << FLAGS_tag << ",";
            u64 total_committed = 0, total_aborted = 0;
            // -------------------------------------------------------------------------------------
            for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
               total_committed += thread_committed[t_i].exchange(0);
               total_aborted += thread_aborted[t_i].exchange(0);
            }
            csv << total_committed << "," << total_aborted << endl;
            sleep(1);
            std::shared_ptr<rocksdb::Statistics> stats = db->GetDBOptions().statistics;
            rocksdb::HistogramData sst_read_hist;
            stats->histogramData(rocksdb::Histograms::SST_READ_MICROS, &sst_read_hist);
            rocksdb::HistogramData sst_write_hist;
            stats->histogramData(rocksdb::Histograms::SST_WRITE_MICROS, &sst_write_hist);
            csv << (sst_read_hist.sum - sst_read_prev) / (total_aborted + total_committed) << ","
                 << (sst_write_hist.sum - sst_write_prev) / (total_aborted + total_committed) << endl;
         }
         running_threads_counter--;
      });
      profiling_thread.detach();
   }
};
// -------------------------------------------------------------------------------------
template <class Record>
struct RocksDBAdapter : public Adapter<Record> {
   using SEP = u32;  // use 32-bits integer as separator instead of column family
   RocksDB& map;
   RocksDBAdapter(RocksDB& map) : map(map) {}
   // -------------------------------------------------------------------------------------
   template <typename T>
   rocksdb::Slice RSlice(T* ptr, u64 len)
   {
      return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Put(map.wo, RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
         ensure(s.ok());
      } else {
         s = map.txn->Put(RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
      }
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      } else {
         s = map.txn->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      }
      assert(s.ok());
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
   }

   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      } else {
         s = map.txn->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      }
      if (!s.ok()) {
         return false;
      }
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
      return true;
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& fn, leanstore::UpdateSameSizeInPlaceDescriptor&) final
   {
      Record r;
      lookup1(key, [&](const Record& rec) { r = rec; });
      fn(r);
      insert(key, r);
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Delete(map.wo, RSlice(folded_key, folded_key_len));
         if (s.ok()) {
            return true;
         } else {
            return false;
         }
      } else {
         s = map.txn->Delete(RSlice(folded_key, folded_key_len));
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
         return true;
      }
   }
   // -------------------------------------------------------------------------------------
   template <class T>
   uint32_t getId(const T& str)
   {
      return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(str.data())) ^ (1ul << 31);
   }
   //             [&](const neworder_t::Key& key, const neworder_t&) {
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = map.db->NewIterator(map.ro);
      for (it->Seek(RSlice(folded_key, folded_key_len)); it->Valid() && getId(it->key()) == Record::id; it->Next()) {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                 std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = map.db->NewIterator(map.ro);
      for (it->SeekForPrev(RSlice(folded_key, folded_key_len)); it->Valid() && getId(it->key()) == Record::id; it->Prev()) {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
   {
      Field local_f;
      bool found = false;
      lookup1(key, [&](const Record& record) {
         found = true;
         local_f = (record).*f;
      });
      assert(found);
      return local_f;
   }
};
