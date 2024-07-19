#include "../shared/LeanStoreAdapter.hpp"
#include "JoinedSchema.hpp"
#include "Schema.hpp"
#include "TPCCWorkload.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <iostream>
#include <set>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_int64(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_verify, false, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
DEFINE_uint64(tpcc_analytical_weight, 0, "");
DEFINE_uint64(ch_a_threads, 0, "CH analytical threads");
DEFINE_uint64(ch_a_rounds, 1, "");
DEFINE_uint64(ch_a_query, 2, "");
DEFINE_uint64(ch_a_start_delay_sec, 0, "");
DEFINE_uint64(ch_a_process_delay_sec, 0, "");
DEFINE_bool(ch_a_infinite, false, "");
DEFINE_bool(ch_a_once, false, "");
DEFINE_uint32(tpcc_threads, 0, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Join TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   assert(FLAGS_tpcc_warehouse_count > 0);
   LeanStore::addS64Flag("TPC_SCALE", &FLAGS_tpcc_warehouse_count);
   // -------------------------------------------------------------------------------------
   LeanStore db;
   LeanStoreAdapter<order_line_ols_t> order_line;
   LeanStoreAdapter<stock_ols_t> stock;
   LeanStoreAdapter<joined_ols_t> joined_orderline_stock;
   auto& crm = db.getCRManager();
   // -------------------------------------------------------------------------------------
   crm.scheduleJobSync(0, [&]() {
      order_line = LeanStoreAdapter<order_line_ols_t>(db, "order_line");
      stock = LeanStoreAdapter<stock_ols_t>(db, "stock");
      joined_orderline_stock = LeanStoreAdapter<joined_ols_t>(db, "joined_orderline_stock");
   });

   // -------------------------------------------------------------------------------------
   // Step 1: Load order_line and stock with specific scale factor
   std::atomic<u32> g_w_id = 1;
   for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&]() {
         while (true) {
            u32 w_id = g_w_id++;
            if (w_id > FLAGS_tpcc_warehouse_count) {
               return;
            }
            // Load order_line and stock for the warehouse
            cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
            // Load functions to be implemented
            loadOrderLine(w_id);
            loadStock(w_id);
            cr::Worker::my().commitTX();
         }
      });
   }
   crm.joinAll();

   // -------------------------------------------------------------------------------------
   // Step 2: Perform a merge join and load the result into joined_orderline_stock
   {
      auto orderline_scanner = order_line.getScanner();
      auto stock_scanner = stock.getScanner();
      MergeJoin<order_line_ols_t, stock_ols_t, joined_ols_t> merge_join(
          orderline_scanner,
          stock_scanner);

      while (true) {
         auto joined_record = merge_join.next();
         if (!joined_record) break;
         joined_orderline_stock.insert(joined_record.Key, joined_record);
      }
   }

   // -------------------------------------------------------------------------------------
   // Step 3: Start write TXs into order_line and stock, and read TXs into joined_orderline_stock
   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
   db.startProfilingThread();
   u64 tx_per_thread[FLAGS_worker_threads];

   // Writer threads
   for (u64 t_i = 0; t_i < FLAGS_worker_threads / 2; t_i++) {
      crm.scheduleJobAsync(t_i, [&, t_i]() {
         running_threads_counter++;
         while (keep_running) {
            cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
            // Perform insertions and deletions in order_line and stock
            // Implement logic for impacting joined_orderline_stock
            // ...
            cr::Worker::my().commitTX();
            WorkerCounters::myCounters().tx++;
         }
         running_threads_counter--;
      });
   }

   // Reader threads
   for (u64 t_i = FLAGS_worker_threads / 2; t_i < FLAGS_worker_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&, t_i]() {
         running_threads_counter++;
         while (keep_running) {
            cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
            // Perform reads in joined_orderline_stock
            // ...
            cr::Worker::my().commitTX();
            WorkerCounters::myCounters().tx++;
         }
         running_threads_counter--;
      });
   }

   if (FLAGS_run_until_tx) {
      while (db.getGlobalStats().accumulated_tx_counter < FLAGS_run_until_tx) {
         usleep(500);
      }
   } else {
      sleep(FLAGS_run_for_seconds);
   }

   keep_running = false;
   while (running_threads_counter) {
      usleep(1000);
   }
   crm.joinAll();

   // Output results
   cout << "Experiment completed." << endl;

   return 0;
}