#include "../shared/LeanStoreAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "Join.hpp"
#include "JoinedSchema.hpp"
#include "TPCCJoinWorkload.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <iostream>
#include <string>
// -------------------------------------------------------------------------------------
DEFINE_int64(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_verify, false, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
// DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
// DEFINE_uint64(tpcc_analytical_weight, 0, "");
DEFINE_uint64(ch_a_threads, 0, "CH analytical threads");
DEFINE_uint64(ch_a_rounds, 1, "");
DEFINE_uint64(ch_a_query, 2, "");
DEFINE_uint64(ch_a_start_delay_sec, 0, "");
DEFINE_uint64(ch_a_process_delay_sec, 0, "");
// DEFINE_bool(ch_a_infinite, false, "");
// DEFINE_bool(ch_a_once, false, "");
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
   // Check arguments
   ensure(FLAGS_ch_a_threads < FLAGS_worker_threads);
   // -------------------------------------------------------------------------------------
   LeanStore db;
   LeanStoreAdapter<warehouse_t> warehouse;
   LeanStoreAdapter<district_t> district;
   LeanStoreAdapter<customer_t> customer;
   LeanStoreAdapter<customer_wdl_t> customerwdl;
   LeanStoreAdapter<history_t> history;
   LeanStoreAdapter<neworder_t> neworder;
   LeanStoreAdapter<order_t> order;
   LeanStoreAdapter<order_wdc_t> order_wdc;
   LeanStoreAdapter<orderline_t> orderline;
   LeanStoreAdapter<item_t> item;
   LeanStoreAdapter<stock_t> stock;
   LeanStoreAdapter<ol_join_sec_t> orderline_secondary;
   // LeanStoreAdapter<stock_join_sec_t> stock_secondary;
   LeanStoreAdapter<joined_ols_t> joined_ols;

   auto& crm = db.getCRManager();
   // -------------------------------------------------------------------------------------
   crm.scheduleJobSync(0, [&]() {
      warehouse = LeanStoreAdapter<warehouse_t>(db, "warehouse");
      district = LeanStoreAdapter<district_t>(db, "district");
      customer = LeanStoreAdapter<customer_t>(db, "customer");
      customerwdl = LeanStoreAdapter<customer_wdl_t>(db, "customerwdl");
      history = LeanStoreAdapter<history_t>(db, "history");
      neworder = LeanStoreAdapter<neworder_t>(db, "neworder");
      order = LeanStoreAdapter<order_t>(db, "order");
      order_wdc = LeanStoreAdapter<order_wdc_t>(db, "order_wdc");
      orderline = LeanStoreAdapter<orderline_t>(db, "orderline");
      item = LeanStoreAdapter<item_t>(db, "item");
      stock = LeanStoreAdapter<stock_t>(db, "stock");
      orderline_secondary = LeanStoreAdapter<ol_join_sec_t>(db, "orderline_secondary");
      // stock_secondary = LeanStoreAdapter<stock_join_sec_t>(db, "stock_secondary"); not needed because join key is its primary key
      joined_ols = LeanStoreAdapter<joined_ols_t>(db, "joined_ols");
   });

   db.registerConfigEntry("tpcc_warehouse_count", FLAGS_tpcc_warehouse_count);
   db.registerConfigEntry("tpcc_warehouse_affinity", FLAGS_tpcc_warehouse_affinity);
   db.registerConfigEntry("tpcc_threads", FLAGS_tpcc_threads);
   db.registerConfigEntry("ch_a_threads", FLAGS_ch_a_threads);
   db.registerConfigEntry("ch_a_rounds", FLAGS_ch_a_rounds);
   db.registerConfigEntry("ch_a_query", FLAGS_ch_a_query);
   db.registerConfigEntry("ch_a_start_delay_sec", FLAGS_ch_a_start_delay_sec);
   db.registerConfigEntry("ch_a_process_delay_sec", FLAGS_ch_a_process_delay_sec);
   db.registerConfigEntry("run_until_tx", FLAGS_run_until_tx);

   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);

   const bool should_tpcc_driver_handle_isolation_anomalies = isolation_level < leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;

   TPCCWorkload<LeanStoreAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock,
                                       FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove,
                                       should_tpcc_driver_handle_isolation_anomalies, FLAGS_tpcc_warehouse_affinity);

   TPCCJoinWorkload<LeanStoreAdapter> tpcc_join(&tpcc, orderline_secondary, joined_ols);
   // -------------------------------------------------------------------------------------
   // Step 1: Load order_line and stock with specific scale factor
   if (!FLAGS_recover) {
      cout << "Loading TPC-C" << endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpcc.loadItem();
         tpcc.loadWarehouse();
         cr::Worker::my().commitTX();
      });
      std::atomic<u32> g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
               tpcc.loadStock(w_id);
               tpcc.loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  tpcc.loadCustomer(w_id, d_id);
                  tpcc.loadOrders(w_id, d_id);
               }
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
      // -------------------------------------------------------------------------------------
      if (FLAGS_tpcc_verify) {
         cout << "Verifying TPC-C" << endl;
         crm.scheduleJobSync(0, [&]() {
            cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
            tpcc.verifyItems();
            cr::Worker::my().commitTX();
         });
         g_w_id = 1;
         for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
            crm.scheduleJobAsync(t_i, [&]() {
               while (true) {
                  u32 w_id = g_w_id++;
                  if (w_id > FLAGS_tpcc_warehouse_count) {
                     return;
                  }
                  cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
                  tpcc.verifyWarehouse(w_id);
                  cr::Worker::my().commitTX();
               }
            });
            crm.joinAll();
         }
      }
   }

   // -------------------------------------------------------------------------------------
   // Step 2: Add secondary index to orderline and stock
   {
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         auto orderline_scanner = orderline.getScanner();
         while (true) {
            auto ret = orderline_scanner.next();
            if (!ret.has_value())
               break;
            auto [key, payload] = ret.value();
            ol_join_sec_t::Key sec_key = {key.ol_w_id, payload.ol_i_id, key.ol_d_id, key.ol_o_id, key.ol_number};
            orderline_secondary.insert(sec_key, {});
         }
         cr::Worker::my().commitTX();
      });
   }

   // -------------------------------------------------------------------------------------
   // Step 3: Perform a merge join and load the result into joined_orderline_stock
   {
      crm.scheduleJobSync(0, [&]() {
         std::cout << "Merging orderline and stock" << std::endl;
         tpcc.prepare();
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         auto orderline_scanner = orderline.getScanner<ol_join_sec_t>(&orderline_secondary);
         auto stock_scanner = stock.getScanner();
         MergeJoin<orderline_t, stock_t, joined_ols_t> merge_join(&orderline_scanner, &stock_scanner);

         while (true) {
            auto ret = merge_join.next();
            if (!ret.has_value())
               break;
            auto [key, payload] = ret.value();
            joined_ols.insert(key, payload);
         }
         // static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(joined_ols.btree))->printInfos(8 *
         // 1024 * 1024 * 1024);
         cr::Worker::my().commitTX();
      });
   }

   double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << "TPC-C loaded - consumed space in GiB = " << gib << endl;
   crm.scheduleJobSync(0, [&]() {
      cout << "Warehouse pages = " << warehouse.btree->countPages() << endl;
      cout << "District pages = " << district.btree->countPages() << endl;
      cout << "Customer pages = " << customer.btree->countPages() << endl;
      cout << "CustomerWDL pages = " << customerwdl.btree->countPages() << endl;
      cout << "History pages = " << history.btree->countPages() << endl;
      cout << "NewOrder pages = " << neworder.btree->countPages() << endl;
      cout << "Order pages = " << order.btree->countPages() << endl;
      cout << "OrderWDC pages = " << order_wdc.btree->countPages() << endl;
      cout << "OrderLine pages = " << orderline.btree->countPages() << endl;
      cout << "Item pages = " << item.btree->countPages() << endl;
      cout << "Stock pages = " << stock.btree->countPages() << endl;
      cout << "OrderLineSecondary pages = " << orderline_secondary.btree->countPages() << endl;
      cout << "JoinedOrderLineStock pages = " << joined_ols.btree->countPages() << endl;
   });

   crm.joinAll();

   // -------------------------------------------------------------------------------------
   // Step 4: Start write TXs into order_line and stock, and read TXs into joined_orderline_stock
   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
   db.startProfilingThread();
   u64 tx_per_thread[FLAGS_worker_threads];

   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&, t_i]() {
         running_threads_counter++;
         tpcc.prepare();
         volatile u64 tx_acc = 0;
         while (keep_running) {
            utils::Timer timer(CRCounters::myCounters().cc_ms_oltp_tx);
            jumpmuTry()
            {
               cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
               u32 w_id;
               if (FLAGS_tpcc_warehouse_affinity) {
                  w_id = t_i + 1;
               } else {
                  w_id = tpcc.urand(1, FLAGS_tpcc_warehouse_count);
               }
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               tpcc_join.tx(w_id);
               if (FLAGS_tpcc_abort_pct && tpcc.urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                  cr::Worker::my().abortTX();
               } else {
                  cr::Worker::my().commitTX();
               }
               // cout << "TXs = " << WorkerCounters::myCounters().tx << " TX aborts = " << WorkerCounters::myCounters().tx_abort << endl;
               WorkerCounters::myCounters().tx++;
               tx_acc = tx_acc + 1;
            }
            jumpmuCatch()
            {
               WorkerCounters::myCounters().tx_abort++;
            }
         }
         cr::Worker::my().shutdown();
         tx_per_thread[t_i] = tx_acc;
         running_threads_counter--;
      });
   }

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
   // -------------------------------------------------------------------------------------
   gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << endl << "consumed space in GiB = " << gib << endl;
   return 0;
}