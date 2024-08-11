#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>
#include <rocksdb/wide_columns.h>
#include "Types.hpp"
#include "../join/TPCCBaseWorkload.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/utils/Misc.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <string>
#include <ios>
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
      assert(txn == nullptr);
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

   template <int id_count>
   void logSizes(std::array<uint64_t, id_count - 10>* times = nullptr)
   {
      std::string file_name = id_count == 13 ? "join_size.csv" : "merged_size.csv";
      std::filesystem::path csv_path = std::filesystem::path(FLAGS_csv_path).parent_path().parent_path() / file_name;
      bool csv_exists = std::filesystem::exists(csv_path);
      std::ofstream csv_file(csv_path, std::ios::app);
      if (!csv_exists)
      {
         csv_file << "table(s),config,size";
         if (times) {
            csv_file << ",time";
         }
         csv_file << std::endl;
      }
         
      rocksdb::Range ranges[id_count];
      for (u32 i = 0; i < id_count; i++) {
         u8 start[sizeof(u32)];
         const u32 folded_key_len = fold(start, i);
         rocksdb::Slice start_slice((const char*)start, folded_key_len);
         u8 limit[sizeof(u32)];
         const u32 folded_limit_len = fold(limit, i + 1);
         rocksdb::Slice limit_slice((const char*)limit, folded_limit_len);
         ranges[i] = rocksdb::Range(start_slice, limit_slice);
      }
      uint64_t sizes[id_count];
      db->GetApproximateSizes(ranges, id_count, sizes);
      std::cout << "Sizes:";
      for (u32 i = 0; i < id_count; i++) {
         std::cout << " " << sizes[i];
      }
      std::cout << std::endl;
      uint64_t core_size = 0;
      for (u32 i = 0; i <= 10; i++) {
         core_size += sizes[i];
      }
      csv_file << "core," << FLAGS_target_gib << "|" << FLAGS_semijoin_selectivity << "|" << INCLUDE_COLUMNS << "," << core_size;
      if (times) {
         csv_file << "," << times->at(0) << std::endl;
      } else {
         csv_file << std::endl;
      }
      for (u32 i = 11; i < id_count; i++) {
         csv_file << "table" << i << "," << FLAGS_target_gib << "|" << FLAGS_semijoin_selectivity << "|" << INCLUDE_COLUMNS << "," << sizes[i];
         if (times) {
            csv_file << "," << times->at(i - 10) << std::endl;
         } else {
            csv_file << std::endl;
         }
      }
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

         leanstore::profiling::CPUTable cpu_table;
         cpu_table.open();
         cpu_table.next(); // Clear previous values

         u64 time = 0;
         std::ofstream::openmode open_flags;
         if (FLAGS_csv_truncate) {
            open_flags = std::ios::trunc;
         } else {
            open_flags = std::ios::app;
         }
         std::ofstream csv(FLAGS_csv_path + "_sum.csv", open_flags);
         csv.seekp(0, std::ios::end);
         csv << std::setprecision(2) << std::fixed;

         if (print_header) {
            csv << "t,tag,oltp_committed,oltp_aborted,SSTRead(ms)/TX,SSTWrite(ms)/TX,GHz,Cycles/TX" << endl;
         }
         uint64_t sst_read_prev = 0, sst_write_prev = 0;
         while (running_threads_counter - 1 > 0) {
            cpu_table.next();
            csv << time++ << "," << FLAGS_tag << ",";
            u64 total_committed = 0, total_aborted = 0;
            // -------------------------------------------------------------------------------------
            for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
               total_committed += thread_committed[t_i].exchange(0);
               total_aborted += thread_aborted[t_i].exchange(0);
            }
            csv << total_committed << "," << total_aborted << ",";

            std::shared_ptr<rocksdb::Statistics> stats = db->GetDBOptions().statistics;
            rocksdb::HistogramData sst_read_hist;
            stats->histogramData(rocksdb::Histograms::SST_READ_MICROS, &sst_read_hist);
            rocksdb::HistogramData sst_write_hist;
            stats->histogramData(rocksdb::Histograms::SST_WRITE_MICROS, &sst_write_hist);
            if (total_aborted + total_committed > 0) {
               csv << (sst_read_hist.sum - sst_read_prev) / (total_aborted + total_committed) << ","
                 << (sst_write_hist.sum - sst_write_prev) / (total_aborted + total_committed) << ",";
               sst_read_prev = sst_read_hist.sum;
               sst_write_prev = sst_write_hist.sum;
            } else {
               csv << "0,0,";
            }

            csv << cpu_table.workers_agg_events["GHz"] << ",";

            if (total_aborted + total_committed > 0) {
               csv << cpu_table.workers_agg_events["cycle"] / (total_aborted + total_committed) << endl;
            } else {
               csv << "0" << endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
         }
         running_threads_counter--;
      });
      profiling_thread.detach();
   }
};