#include "../shared/RocksDBAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"
#include "RocksDBExperimentHelper.hpp"
#include "RocksDBMergedAdapter.hpp"
#include "TPCCMergedWorkload.hpp"
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
   gflags::SetUsageMessage("RocksDB TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   auto helper = RocksDBExperimentHelper();
   auto context = helper.prepareExperiment();
   RocksDBMergedAdapter<11> merged_ols(context->rocks_db);
   TPCCMergedWorkload<RocksDBAdapter, RocksDBMergedAdapter<11>> tpcc_merged(&context->tpcc, merged_ols);

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
                  tpcc_merged.loadStockToMerged(w_id);
                  tpcc_merged.loadOrderlineSecondaryToMerged(w_id);
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
      uint64_t core_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
      uint64_t merged_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
      std::array<uint64_t, 2> times = {core_time, merged_time};
      context->rocks_db.logSizes<12>(&times);
   } else {
      context->rocks_db.logSizes<12>();
   }

   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   std::atomic<u64> thread_committed[FLAGS_worker_threads];
   std::atomic<u64> thread_aborted[FLAGS_worker_threads];
   context->rocks_db.startProfilingThread(running_threads_counter, keep_running, thread_committed, thread_aborted, FLAGS_print_header);
   
   helper.scheduleTransations(&tpcc_merged, threads, keep_running, running_threads_counter, thread_committed, thread_aborted);
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
