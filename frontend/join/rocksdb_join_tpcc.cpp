#include "../shared/RocksDBAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"
#include "TPCCJoinWorkload.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <rocksdb/db.h>

#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
// -------------------------------------------------------------------------------------

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
DEFINE_string(rocks_db, "pessimistic", "none/pessimistic/optimistic");
DEFINE_bool(print_header, true, "");
// -------------------------------------------------------------------------------------
thread_local rocksdb::Transaction* RocksDB::txn = nullptr;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   using orderline_sec_t = typename std::conditional<INCLUDE_COLUMNS == 0, ol_sec_key_only_t, ol_join_sec_t>::type;
   using joined_t = typename std::conditional<INCLUDE_COLUMNS == 0, joined_ols_key_only_t, joined_ols_t>::type;
   gflags::SetUsageMessage("RocksDB TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   RocksDB::DB_TYPE type;
   if (FLAGS_rocks_db == "none") {
      type = RocksDB::DB_TYPE::DB;
   } else if (FLAGS_rocks_db == "pessimistic") {
      type = RocksDB::DB_TYPE::TransactionDB;
   } else if (FLAGS_rocks_db == "optimistic") {
      // TODO: still WIP
      UNREACHABLE();
      type = RocksDB::DB_TYPE::OptimisticDB;
   } else {
      UNREACHABLE();
   }
   RocksDB rocks_db(type);
   RocksDBAdapter<warehouse_t> warehouse(rocks_db);
   RocksDBAdapter<district_t> district(rocks_db);
   RocksDBAdapter<customer_t> customer(rocks_db);
   RocksDBAdapter<customer_wdl_t> customerwdl(rocks_db);
   RocksDBAdapter<history_t> history(rocks_db);
   RocksDBAdapter<neworder_t> neworder(rocks_db);
   RocksDBAdapter<order_t> order(rocks_db);
   RocksDBAdapter<order_wdc_t> order_wdc(rocks_db);
   RocksDBAdapter<orderline_t> orderline(rocks_db);
   RocksDBAdapter<item_t> item(rocks_db);
   RocksDBAdapter<stock_t> stock(rocks_db);
   RocksDBAdapter<orderline_sec_t> orderline_secondary(rocks_db);
   RocksDBAdapter<joined_t> joined_ols(rocks_db);
   // -------------------------------------------------------------------------------------
   TPCCWorkload<RocksDBAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock,
                                     FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove, true, true);
   TPCCJoinWorkload<RocksDBAdapter> tpcc_join(&tpcc, orderline_secondary, joined_ols);

   std::vector<thread> threads;
   std::atomic<u32> g_w_id = 1;
   if (!FLAGS_recover) {
      rocks_db.startTX();
      tpcc.loadItem();
      tpcc.loadWarehouse();
      rocks_db.commitTX();
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         threads.emplace_back([&]() {
            while (true) {
               const u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               jumpmuTry()
               {
                  rocks_db.startTX();
                  tpcc.loadStock(w_id);
                  tpcc.loadDistrict(w_id);
                  for (Integer d_id = 1; d_id <= 10; d_id++) {
                     tpcc.loadCustomer(w_id, d_id);
                     tpcc.loadOrders(w_id, d_id);
                  }
                  rocks_db.commitTX();
               }
               jumpmuCatch()
               {
                  UNREACHABLE();
               }
            }
         });
      }
      for (auto& thread : threads) {
         thread.join();
      }
      threads.clear();
      g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         threads.emplace_back([&]() {
            while (true) {
               const u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               jumpmuTry()
               {
                  rocks_db.startTX();
                  tpcc_join.loadOrderlineSecondary(w_id);
                  rocks_db.commitTX();
               }
               jumpmuCatch()
               {
                  UNREACHABLE();
               }
            }
         });
      }
      for (auto& thread : threads) {
         thread.join();
      }
      threads.clear();
      g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         threads.emplace_back([&]() {
            while (true) {
               const u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               jumpmuTry()
               {
                  rocks_db.startTX();
                  tpcc_join.joinOrderlineAndStock(w_id);
                  rocks_db.commitTX();
               }
               jumpmuCatch()
               {
                  UNREACHABLE();
               }
            }
         });
      }
      for (auto& thread : threads) {
         thread.join();
      }
      threads.clear();
   }
   // -------------------------------------------------------------------------------------
   rocks_db.getSizes();
   atomic<u64> running_threads_counter = 0;
   atomic<u64> keep_running = true;
   std::atomic<u64> thread_committed[FLAGS_worker_threads];
   std::atomic<u64> thread_aborted[FLAGS_worker_threads];
   rocks_db.startProfilingThread(running_threads_counter, keep_running, thread_committed, thread_aborted, FLAGS_print_header);
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      thread_committed[t_i] = 0;
      thread_aborted[t_i] = 0;
      // -------------------------------------------------------------------------------------
      threads.emplace_back([&, t_i]() {
         running_threads_counter++;
         leanstore::CPUCounters::registerThread("worker_" + std::to_string(t_i), false);
         if (FLAGS_pin_threads) {
            leanstore::utils::pinThisThread(t_i);
         }
         tpcc.prepare();
         while (keep_running) {
            jumpmuTry()
            {
               Integer w_id = tpcc.urand(1, FLAGS_tpcc_warehouse_count);
               rocks_db.startTX();
               tpcc_join.tx(w_id, FLAGS_read_percentage, FLAGS_scan_percentage, FLAGS_write_percentage, FLAGS_order_size);
               rocks_db.commitTX();
               thread_committed[t_i]++;
            }
            jumpmuCatch()
            {
               thread_aborted[t_i]++;
            }
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   sleep(FLAGS_run_for_seconds);
   keep_running = false;
   while (running_threads_counter) {
   }
   for (auto& thread : threads) {
      thread.join();
   }
   // -------------------------------------------------------------------------------------
   return 0;
}
