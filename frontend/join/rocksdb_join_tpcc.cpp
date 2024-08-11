#include "../shared/RocksDBAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "RocksDBExperimentHelper.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"
#include "TPCCJoinWorkload.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <rocksdb/db.h>

#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
thread_local rocksdb::Transaction* RocksDB::txn = nullptr;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   using orderline_sec_t = typename RocksDBExperimentHelper::orderline_sec_t;
   using joined_t = typename RocksDBExperimentHelper::joined_t;
   gflags::SetUsageMessage("RocksDB TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   auto helper = RocksDBExperimentHelper();
   auto context = helper.prepareExperiment();
   RocksDBAdapter<orderline_sec_t> orderline_secondary(context->rocks_db);
   RocksDBAdapter<joined_t> joined_ols(context->rocks_db);
   TPCCJoinWorkload<RocksDBAdapter> tpcc_join(&context->tpcc, orderline_secondary, joined_ols);

   std::vector<thread> threads;
   std::atomic<u32> g_w_id = 1;
   if (!FLAGS_recover) {
      std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
      helper.loadCore();
      std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
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
                  context->rocks_db.startTX();
                  tpcc_join.loadOrderlineSecondary(w_id);
                  context->rocks_db.commitTX();
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
      std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
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
                  context->rocks_db.startTX();
                  tpcc_join.joinOrderlineAndStock(w_id);
                  context->rocks_db.commitTX();
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
      std::chrono::steady_clock::time_point t3 = std::chrono::steady_clock::now();
      uint64_t core_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
      uint64_t secondary_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
      uint64_t join_time = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();
      std::array<uint64_t, 3> times = {core_time, secondary_time, join_time};
      context->rocks_db.logSizes<13>(&times);
   } else {
      context->rocks_db.logSizes<13>();
   }

   atomic<u64> keep_running = true;

   if (FLAGS_write_percentage > 0) {
      // First run read only TXs to warm up the system without increasing the data size
      std::cout << "Warming up the system with read only TXs" << std::endl;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         threads.emplace_back([&]() {
            while (keep_running) {
               Integer w_id = FLAGS_worker_threads % FLAGS_tpcc_warehouse_count + 1;
               tpcc_join.tx(w_id, 100, 0, 0);
            }
         });
      }
      sleep(20);
      keep_running = false;
      for (auto& thread : threads) {
         thread.join();
      }
      threads.clear();
   }
   // -------------------------------------------------------------------------------------
   atomic<u64> running_threads_counter = 0;
   keep_running = true;
   std::atomic<u64> thread_committed[FLAGS_worker_threads];
   std::atomic<u64> thread_aborted[FLAGS_worker_threads];
   context->rocks_db.startProfilingThread(running_threads_counter, keep_running, thread_committed, thread_aborted, FLAGS_print_header);
   
   helper.scheduleTransations(&tpcc_join, threads, keep_running, running_threads_counter, thread_committed, thread_aborted);

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
