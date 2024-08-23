#pragma once

#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/wide_columns.h>

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
   rocksdb::ReadOptions iterator_ro;
   enum class DB_TYPE : u8 { DB, TransactionDB, OptimisticDB };
   const DB_TYPE type;
   // -------------------------------------------------------------------------------------
   RocksDB(DB_TYPE type = DB_TYPE::DB) : type(type)
   {
      if (FLAGS_trunc == false && std::filesystem::exists(FLAGS_ssd_path)) {
         FLAGS_recover = true;
      } else if (FLAGS_trunc == true && std::filesystem::exists(FLAGS_ssd_path)) {
         std::filesystem::remove_all(FLAGS_ssd_path);
         std::filesystem::create_directory(FLAGS_ssd_path);
      } else if (!std::filesystem::exists(FLAGS_ssd_path)) {
         std::filesystem::create_directory(FLAGS_ssd_path);
      }
      if (FLAGS_persist_file == "./leanstore.json") {
         FLAGS_persist = false;
      }
      wo.disableWAL = true;
      wo.sync = false;
      iterator_ro.snapshot = nullptr;  // Snapshot from pinning resources
      // -------------------------------------------------------------------------------------
      rocksdb::Options db_options;
      db_options.use_direct_reads = true;
      db_options.use_direct_io_for_flush_and_compaction = true;
      db_options.db_write_buffer_size = 0;  // disabled
      // db_options.write_buffer_size = 64 * 1024 * 1024; keep the default
      db_options.create_if_missing = true;
      // db_options.manual_wal_flush = true;
      db_options.compression = rocksdb::CompressionType::kNoCompression;
      db_options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
      db_options.row_cache = rocksdb::NewLRUCache(FLAGS_dram_gib * 1024 * 1024 * 1024);
      rocksdb::BlockBasedTableOptions table_options;
      table_options.filter_policy.reset(
          rocksdb::NewBloomFilterPolicy(10, false));  // As of RocksDB 7.0, the use_block_based_builder parameter is ignored.
      db_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
      db_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(u32)));  // ID
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

   ~RocksDB() { 
      std::cout << "RocksDB::~RocksDB() ";
      if (FLAGS_persist) {
         std::cout << "Waiting for compaction to finish" << std::endl;
         rocksdb::WaitForCompactOptions wfc_options;
         wfc_options.close_db=true;
         rocksdb::Status s = db->WaitForCompact(wfc_options);
      }
      std::cout << std::endl;
      delete db;
   }

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

   void startProfilingThread(std::atomic<u64>& running_threads_counter,
                             std::atomic<u64>&,
                             std::vector<std::atomic<u64>>& thread_committed,
                             std::vector<std::atomic<u64>>& thread_aborted,
                             bool& print_header)
   {
      std::cout << "Started profiling in " << FLAGS_csv_path + "_sum.csv" << std::endl;
      std::thread profiling_thread([&]() {
         leanstore::utils::pinThisThread(((FLAGS_pin_threads) ? FLAGS_worker_threads : 0) + FLAGS_wal + FLAGS_pp_threads);
         running_threads_counter++;

         leanstore::profiling::CPUTable cpu_table;
         cpu_table.open();
         cpu_table.next();  // Clear previous values

         u64 time = 0;
         std::ofstream::openmode open_flags;
         if (FLAGS_csv_truncate) {
            open_flags = std::ios::trunc;
         } else {
            open_flags = std::ios::app;
         }
         std::ofstream csv(FLAGS_csv_path + "_sum.csv", open_flags);
         csv.seekp(0, std::ios::end);
         csv << std::setprecision(5) << std::fixed;

         if (print_header) {
            csv << "t,tag,OLTP TX,oltp_committed,oltp_aborted,SSTRead(ms)/TX,SSTWrite(ms)/TX,GHz,Cycles/TX,CPUTime/TX (ms),Utilized CPUs" << endl;
         }
         uint64_t sst_read_prev = 0, sst_write_prev = 0;
         uint64_t cycles_acc = 0, task_clock_acc = 0;
         while (running_threads_counter - 1 > 0) {
            cpu_table.next();
            csv << time++ << "," << FLAGS_tag << ",";
            u64 total_committed = 0, total_aborted = 0;
            // -------------------------------------------------------------------------------------
            for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
               total_committed += thread_committed[t_i].exchange(0);
               total_aborted += thread_aborted[t_i].exchange(0);
            }
            u64 tx = total_committed + total_aborted;
            csv << tx << ",";
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

            if (tx > 0) {
               csv << (cpu_table.workers_agg_events["cycle"] + cycles_acc) / tx << ",";
               cycles_acc = 0;

               csv << ((double)cpu_table.workers_agg_events["task"] + task_clock_acc) / tx * 1e-6 << ",";
               task_clock_acc = 0;
            } else {
               csv << "0,";
               cycles_acc += cpu_table.workers_agg_events["cycle"];

               csv << "0,";
               task_clock_acc += cpu_table.workers_agg_events["task"];
            }

            csv << cpu_table.workers_agg_events["CPU"] << endl;

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
         }
         running_threads_counter--;
      });
      profiling_thread.detach();
   }
};