#include "LeanStoreExperimentHelper.hpp"
#include "TPCCBaseWorkload.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
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
   LeanStoreExperimentHelper helper;
   auto context = helper.prepareExperiment();
   auto& crm = context->db.getCRManager();
   auto& db = context->db;
   auto& tpcc = context->tpcc;

   LeanStoreAdapter<orderline_sec_t> orderline_secondary;

   crm.scheduleJobSync(0, [&]() {
      orderline_secondary = LeanStoreAdapter<orderline_sec_t>(db, "orderline_secondary");
   });
   
   TPCCBaseWorkload<LeanStoreAdapter> tpcc_base(&tpcc, &orderline_secondary);
   // -------------------------------------------------------------------------------------
   // Step 1: Load order_line and stock with specific scale factor
   if (!FLAGS_recover) {
      std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
      auto ret = helper.loadCore(false);
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
               tpcc_base.loadStock(w_id);
               tpcc_base.loadOrderlineSecondary(w_id);
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
      auto sec_end = std::chrono::steady_clock::now();
      tpcc_base.logSizes(t0, sec_start, sec_end, crm);
      // -------------------------------------------------------------------------------------
      if (FLAGS_tpcc_verify) {
         auto ret = helper.verifyCore(&tpcc_base);
         if (ret != 0) {
            return ret;
         }
      }
   } else {
      auto ret = helper.verifyCore(&tpcc_base);
      if (ret != 0) {
         return ret;
      }
      tpcc_base.logSizes(crm);
   }


   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   db.startProfilingThread();
   std::vector<u64> tx_per_thread(FLAGS_worker_threads, 0);

   helper.scheduleTransations(&tpcc_base, keep_running, running_threads_counter, tx_per_thread);

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