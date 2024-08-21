#include "../shared/RocksDBAdapter.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "Exceptions.hpp"
#include "RocksDBExperimentHelper.hpp"
#include "TPCCBaseWorkload.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <rocksdb/db.h>

#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
// -------------------------------------------------------------------------------------

#include <chrono>
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

   RocksDBAdapter<orderline_sec_t> orderline_secondary(context->rocks_db);
   
   TPCCBaseWorkload<RocksDBAdapter> tpcc_base(&context->tpcc, &orderline_secondary);

   std::vector<thread> threads;
   std::atomic<u32> g_w_id = 1;
   if (!FLAGS_recover) {
      std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
      helper.loadCore(false);
      std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
      g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         threads.emplace_back([&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               jumpmuTry()
               {
                  context->rocks_db.startTX();
                  tpcc_base.loadStock(w_id);
                  tpcc_base.loadOrderlineSecondary(w_id);
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
      context->rocks_db.logSizes<12>(&times); // Will force compaction
   } else {
      helper.verifyCore(&tpcc_base);
      context->rocks_db.logSizes<12>();
   }

   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   std::vector<std::atomic<u64>> thread_committed(FLAGS_worker_threads);
   std::vector<std::atomic<u64>> thread_aborted(FLAGS_worker_threads);

   // Initialize all elements to 0
   for (u32 i = 0; i < FLAGS_worker_threads; i++) {
      thread_committed[i].store(0);
      thread_aborted[i].store(0);
   }
   context->rocks_db.startProfilingThread(running_threads_counter, keep_running, thread_committed, thread_aborted, FLAGS_print_header);

   helper.scheduleTransations(&tpcc_base, threads, keep_running, running_threads_counter, thread_committed, thread_aborted);
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
