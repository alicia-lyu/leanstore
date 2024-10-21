#include "../shared/LeanStoreMergedAdapter.hpp"
#include "../join-workload/TPCCMergedWorkload.hpp"
#include "LeanStoreExperimentHelper.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <cmath>
#include <string>
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

   LeanStoreMergedAdapter merged;
   // -------------------------------------------------------------------------------------
   crm.scheduleJobSync(0, [&]() { merged = LeanStoreMergedAdapter(db, "merged"); });

   TPCCMergedWorkload<LeanStoreAdapter, LeanStoreMergedAdapter> tpcc_merge(&tpcc, merged);
   // -------------------------------------------------------------------------------------
   // Step 1: Load order_line and stock with specific scale factor
   if (!FLAGS_recover) {
      std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
      auto ret = helper.loadCore(false);
      if (ret != 0) {
         return ret;
      }
      std::chrono::steady_clock::time_point merged_start = std::chrono::steady_clock::now();
      std::atomic<u32> g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
               tpcc_merge.loadStockToMerged(w_id);
               tpcc_merge.loadOrderlineSecondaryToMerged(w_id);
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
      std::chrono::steady_clock::time_point merged_end = std::chrono::steady_clock::now();
      tpcc_merge.logSizes(t0, merged_start, merged_end, crm);
      // -------------------------------------------------------------------------------------
      if (FLAGS_tpcc_verify) {
         auto ret = helper.verifyCore(&tpcc_merge);
         if (ret != 0) {
            return ret;
         }
      }
   } else {
      auto ret = helper.verifyCore(&tpcc_merge);
      if (ret != 0) {
         return ret;
      }
      tpcc_merge.logSizes(crm);
   }

   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   db.startProfilingThread();
   std::vector<u64> tx_per_thread(FLAGS_worker_threads, 0);
   
   helper.scheduleTransations(&tpcc_merge, keep_running, running_threads_counter, tx_per_thread);

   RUN_UNTIL(db, keep_running, running_threads_counter, tx_per_thread, FLAGS_worker_threads, FLAGS_run_until_tx, FLAGS_run_for_seconds);
}