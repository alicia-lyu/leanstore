#pragma once

#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/rocksdb_namespace.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/wide_columns.h>

// -------------------------------------------------------------------------------------
#include "Types.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>

template <typename T>
inline rocksdb::Slice RSlice(T* ptr, u64 len)
{
   return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
}

using ROCKSDB_NAMESPACE::Cache;
using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::LRUCacheOptions;
using ROCKSDB_NAMESPACE::NewLRUCache;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::Range;
using ROCKSDB_NAMESPACE::Status;

struct RocksDB {
   rocksdb::TransactionDB* tx_db;
   std::vector<ColumnFamilyDescriptor> cf_descs;
   std::vector<ColumnFamilyHandle*> cf_handles;
   std::vector<std::function<void()>> get_handle_cbs;
   static thread_local rocksdb::Transaction* txn; // allows multiple transactions as long as called by different threads

   rocksdb::Options db_options;
   rocksdb::BlockBasedTableOptions table_opts;
   rocksdb::CacheUsageOptions cache_opts;
   rocksdb::WriteOptions wo;
   rocksdb::ReadOptions ro;
   rocksdb::ReadOptions iterator_ro;
   const u64 total_cache_bytes;
   double block_share;
   double memtable_share;
   std::shared_ptr<Cache> block_cache = nullptr;

   enum class DB_TYPE : u8 { DB, TransactionDB, OptimisticDB };
   const DB_TYPE type;
   // -------------------------------------------------------------------------------------
   RocksDB(DB_TYPE type = DB_TYPE::TransactionDB) : total_cache_bytes(FLAGS_dram_gib * 1024 * 1024 * 1024), type(type)
   {
      assert(type == DB_TYPE::TransactionDB);  // only allow TransactionDB for now
      // PERSIST & RECOVER
      if (FLAGS_trunc == false && std::filesystem::exists(FLAGS_ssd_path)) {
         FLAGS_recover = true;
         std::cout << "RocksDB: recovering from " << FLAGS_ssd_path << std::endl;
      } else {  // load DB from scratch
         if (FLAGS_trunc == true && std::filesystem::exists(FLAGS_ssd_path)) {
            std::cout << "RocksDB: truncating " << FLAGS_ssd_path << std::endl;
            std::filesystem::remove_all(FLAGS_ssd_path);
         }
         std::filesystem::create_directory(FLAGS_ssd_path);
      }
      FLAGS_persist = FLAGS_persist_file == "./leanstore.json" ? false : true;

      // SETUP CACHE
      if (!FLAGS_recover) {  // bulk loading, write heavy
         block_share = 0.4;
         memtable_share = 0.6;
      } else {  // running queries, read heavy
         block_share = 0.8;
         memtable_share = 0.2;
      }
      set_options();
   }

   void set_options();

   void open()
   {
      // Add default column family
      cf_descs.insert(cf_descs.begin(), ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, ColumnFamilyOptions()));
      cf_handles.insert(cf_handles.begin(), nullptr);

      rocksdb::Status s = rocksdb::TransactionDB::Open(db_options, {}, FLAGS_ssd_path, cf_descs, &cf_handles, &tx_db);  // will create absent cfs
      if (!s.ok())
         std::cerr << s.ToString() << std::endl;
      assert(s.ok());
      // Set the column family handles for all adapters
      for (std::function<void()>& cb : get_handle_cbs) {
         cb();
      }
   }

   ~RocksDB()
   {
      std::cout << "RocksDB::~RocksDB() ";
      if (!FLAGS_persist) {
         std::cout << "Obsolete. Always persisting rocksdb." << std::endl;
      }
      std::cout << "Waiting for compaction and flush..." << std::endl;
      // Flush and sync WAL
      rocksdb::FlushOptions fo;
      fo.wait = true;
      Status s_flush = tx_db->Flush(fo, cf_handles);
      Status s_wal = tx_db->FlushWAL(true);
      assert(s_flush.ok() && s_wal.ok());
      // destroy all column families
      for (ColumnFamilyHandle* cf_handle : cf_handles) {
         Status s = tx_db->DestroyColumnFamilyHandle(cf_handle);
         assert(s.ok());
      }
      // Wait for compaction to finish and close the DB
      rocksdb::WaitForCompactOptions wfc_options;
      wfc_options.close_db = true;
      rocksdb::Status s = tx_db->WaitForCompact(wfc_options);
      assert(s.ok());
      std::ofstream persist_file(FLAGS_persist_file, std::ios::trunc);
      persist_file << "Placeholder. Code logic only needs the file to exist." << std::endl;
   }

   void startTX()
   {
      assert(tx_db != nullptr);
      while (txn != nullptr) {
         auto thread_id = std::this_thread::get_id();
         std::cerr << "WARNING: RocksDB::startTX() called when txn != nullptr in thread " << thread_id
                   << ". Waiting for previous transaction to finish..." << std::endl;
         std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      rocksdb::Status s;
      txn = tx_db->BeginTransaction(wo, {});
   }
   void commitTX()
   {
      assert(tx_db != nullptr);
      rocksdb::Status s;
      s = txn->Commit();
      delete txn;
      txn = nullptr;
   }

   void rollbackTX()
   {
      assert(tx_db != nullptr);
      rocksdb::Status s;
      s = txn->Rollback();
      delete txn;
      txn = nullptr;
   }
   double default_cf_size = 0.0;
   double get_size(ColumnFamilyHandle* cf_handle, const std::string& name = "default");

   template <typename Record>
   double get_size()
   {
      // rocksdb::Slice min_key{0, 1};
      // rocksdb::Slice max_key{255, max_fold_len};
      std::vector<u8> min_key(1, Record::id);
      std::vector<u8> max_key(1, Record::id + 1);
      rocksdb::Slice min_slice(reinterpret_cast<const char*>(min_key.data()), min_key.size());
      rocksdb::Slice max_slice(reinterpret_cast<const char*>(max_key.data()), max_key.size());
      double default_full_size = get_size(cf_handles[0]);
      u64 size = 0;
      rocksdb::Range range(min_slice, max_slice);
      tx_db->GetApproximateSizes(cf_handles[0], &range, 1, &size);
      double size_in_mib = static_cast<double>(size) / (1024 * 1024);
      std::cout << "RocksDB: Approximate size for Record id " << Record::id << " is " << size_in_mib
                << " MiB (default full size: " << default_full_size << " MiB)" << std::endl;
      return size_in_mib;
   };

   bool Put(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, const rocksdb::Slice& value);

   bool Get(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, PinnableSlice* value);

   bool GetForUpdate(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, PinnableSlice* value);

   bool Delete(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key);

   template <typename Record>
   rocksdb::Slice fold_key(const typename Record::Key& key, std::string& key_buf, bool prefix_sep)
   {
      u8* record_begin = nullptr;
      unsigned pos = 0;
      if (prefix_sep) {
         u8 sep = static_cast<u8>(Record::id);
         key_buf.resize(Record::maxFoldLength() + 1);
         pos += fold(reinterpret_cast<u8*>(key_buf.data()), sep);
         record_begin = reinterpret_cast<u8*>(key_buf.data()) + pos;
      } else {
         key_buf.resize(Record::maxFoldLength());
         record_begin = reinterpret_cast<u8*>(key_buf.data());
      }
      pos += Record::foldKey(record_begin, key);
      return RSlice(reinterpret_cast<const char*>(key_buf.data()), pos);
   }

   template <typename Record>
   rocksdb::Slice fold_record(const Record& record)
   {
      // record's lifetime is managed by the caller
      return RSlice(&record, sizeof(record));
   }

   template <typename Record>
   void insert(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const Record& record, bool prefix_sep)
   {
      std::string key_buf;
      Put(cf_handle, fold_key<Record>(key, key_buf, prefix_sep), fold_record<Record>(record));
   }

   template <typename Record>
   void lookup1(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const std::function<void(const Record&)>& cb, bool prefix_sep)
   {
      std::string key_buf;
      rocksdb::Slice k_slice = fold_key<Record>(key, key_buf, prefix_sep);
      rocksdb::PinnableSlice value;
      Get(cf_handle, k_slice, &value);
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      cb(record);
      value.Reset();
   }

   template <typename Record>
   bool tryLookup(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const std::function<void(const Record&)>& cb, bool prefix_sep)
   {
      std::string key_buf;
      rocksdb::Slice k_slice = fold_key<Record>(key, key_buf, prefix_sep);
      rocksdb::PinnableSlice value;
      Status s = tx_db->Get(ro, cf_handle, k_slice, &value);  // force not as part of txn
      if (!s.ok()) {
         return false;
      }
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      cb(record);
      value.Reset();
      return true;
   }

   template <class Record>
   void update1(ColumnFamilyHandle* cf_handle,
                const typename Record::Key& key,
                const std::function<void(Record&)>& cb,
                leanstore::UpdateSameSizeInPlaceDescriptor&)
   {
      update1(cf_handle, key, cb);
   }

   template <class Record>
   void update1(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const std::function<void(Record&)>& cb, bool prefix_sep)
   {
      Record r;
      rocksdb::PinnableSlice r_slice;
      r_slice.PinSelf(RSlice(&r, sizeof(r)));
      std::string key_buf;
      rocksdb::Slice k_slice = fold_key<Record>(key, key_buf, prefix_sep);
      GetForUpdate(cf_handle, k_slice, &r_slice);
      Record r_lookedup = *reinterpret_cast<const Record*>(r_slice.data());
      cb(r_lookedup);
      Put(cf_handle, k_slice, fold_record<Record>(r_lookedup));
      r_slice.Reset();
   }

   template <class Record>
   bool erase(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, bool prefix_sep)
   {
      std::string key_buf;
      Delete(cf_handle, fold_key<Record>(key, key_buf, prefix_sep));
      return true;
   }

   template <class Field, class Record>
   Field lookupField(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, Field Record::* f)
   {
      Field local_f;
      bool found = false;
      lookup1(cf_handle, key, [&](const Record& record) {
         found = true;
         local_f = (record).*f;
      });
      assert(found);
      return local_f;
   }
};