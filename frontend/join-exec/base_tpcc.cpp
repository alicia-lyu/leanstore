#include "LeanStoreExperimentHelper.hpp"
#include "../join-workload/TPCCBaseWorkload.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <chrono>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   INITIALIZE_CONTEXT();

   INITIALIZE_SECONDARY_INDEXES(crm);
   
   TPCCBaseWorkload<LeanStoreAdapter> tpcc_base(&tpcc, &orderline_secondary, stock_secondary_ptr);
   // -------------------------------------------------------------------------------------
   // Step 1: Load order_line and stock with specific scale factor
   if (!FLAGS_recover) {
      std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
      auto ret = helper.loadCore();
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

   RUN_UNTIL(db, keep_running, running_threads_counter, tx_per_thread, FLAGS_worker_threads, FLAGS_run_until_tx, FLAGS_run_for_seconds);
}