#include "leanstore_tpcc_helper.cpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <string>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Join TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   auto context = prepareExperiment();
   TPCCJoinWorkload<LeanStoreAdapter> tpcc_join(&context->tpcc,context->orderline_secondary, context->joined_ols);
   auto& crm = context->db.getCRManager();
   auto& db = context->db;
   auto& tpcc = context->tpcc;
   // -------------------------------------------------------------------------------------
   // Step 1: Load order_line and stock with specific scale factor
   if (!FLAGS_recover) {
      auto ret = loadCore(crm, tpcc);
      if (ret != 0) {
         return ret;
      }
      std::chrono::steady_clock::time_point sec_start = std::chrono::steady_clock::now();
      std::atomic<u32> g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
               tpcc_join.loadOrderlineSecondary(w_id);
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
      logSize("orderline_secondary", sec_start);
      std::chrono::steady_clock::time_point join_start = std::chrono::steady_clock::now();
      g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
               tpcc_join.joinOrderlineAndStock(w_id);
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
      logSize("joined_ols", join_start);
      // -------------------------------------------------------------------------------------
      if (FLAGS_tpcc_verify) {
         auto ret = verifyCore(crm, tpcc, &tpcc_join);
         if (ret != 0) {
            return ret;
         }
      }
   } else {  // verify and warm up
      auto ret = verifyCore(crm, tpcc, &tpcc_join);
      if (ret != 0) {
         return ret;
      }
   }

   // -------------------------------------------------------------------------------------

   // -------------------------------------------------------------------------------------
   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   db.startProfilingThread();
   u64 tx_per_thread[FLAGS_worker_threads];

   scheduleTransations(crm, tpcc, &tpcc_join, keep_running, running_threads_counter, tx_per_thread);

   {
      if (FLAGS_run_until_tx) {
         while (true) {
            if (db.getGlobalStats().accumulated_tx_counter >= FLAGS_run_until_tx) {
               cout << FLAGS_run_until_tx << " has been reached";
               break;
            }
            usleep(500);
         }
      } else {
         // Shutdown threads
         sleep(FLAGS_run_for_seconds);
      }
      keep_running = false;

      while (running_threads_counter) {
      }
      crm.joinAll();
   }
   cout << endl;
   {
      u64 total = 0;
      cout << "TXs per thread = ";
      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         total += tx_per_thread[t_i];
         cout << tx_per_thread[t_i] << ", ";
      }
      cout << endl;
      cout << "Total TPC-C TXs = " << total << endl;
   }
}