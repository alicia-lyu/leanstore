#pragma once
#include "../shared/LeanStoreAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "../join-workload/TPCCBaseWorkload.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"

class LeanStoreExperimentHelper
{
   leanstore::profiling::DTTable* dt_table = nullptr;

  public:
   struct LeanStoreExperimentContext {
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
      TPCCWorkload<LeanStoreAdapter> tpcc;

      LeanStoreExperimentContext(int warehouse_count, int order_wdc_index, bool remove, bool handle_anomalies, int warehouse_affinity)
          : tpcc(warehouse,
                 district,
                 customer,
                 customerwdl,
                 history,
                 neworder,
                 order,
                 order_wdc,
                 orderline,
                 item,
                 stock,
                 order_wdc_index,
                 warehouse_count,
                 remove,
                 handle_anomalies,
                 warehouse_affinity)
      {
      }
   };

  private:
   LeanStoreExperimentContext* context_ptr = nullptr;

  public:
   std::unique_ptr<LeanStoreExperimentContext> prepareExperiment()
   {
      assert(FLAGS_tpcc_warehouse_count > 0);
      assert(FLAGS_read_percentage + FLAGS_scan_percentage + FLAGS_write_percentage == 100);

      LeanStore::addS64Flag("TPC_SCALE", &FLAGS_tpcc_warehouse_count);

      leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
      const bool should_tpcc_driver_handle_isolation_anomalies = isolation_level < leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;

      std::unique_ptr<LeanStoreExperimentContext> context =
          std::make_unique<LeanStoreExperimentContext>(FLAGS_tpcc_warehouse_count, FLAGS_order_wdc_index, FLAGS_tpcc_remove,
                                                       should_tpcc_driver_handle_isolation_anomalies, FLAGS_tpcc_warehouse_affinity);

      auto& crm = context->db.getCRManager();
      crm.scheduleJobSync(0, [&]() {
         context->warehouse = LeanStoreAdapter<warehouse_t>(context->db, "warehouse");
         context->district = LeanStoreAdapter<district_t>(context->db, "district");
         context->customer = LeanStoreAdapter<customer_t>(context->db, "customer");
         context->customerwdl = LeanStoreAdapter<customer_wdl_t>(context->db, "customerwdl");
         context->history = LeanStoreAdapter<history_t>(context->db, "history");
         context->neworder = LeanStoreAdapter<neworder_t>(context->db, "neworder");
         context->order = LeanStoreAdapter<order_t>(context->db, "order");
         context->order_wdc = LeanStoreAdapter<order_wdc_t>(context->db, "order_wdc");
         context->orderline = LeanStoreAdapter<orderline_t>(context->db, "orderline");
         context->item = LeanStoreAdapter<item_t>(context->db, "item");
         context->stock = LeanStoreAdapter<stock_t>(context->db, "stock");
      });

      context->db.registerConfigEntry("tpcc_warehouse_count", FLAGS_tpcc_warehouse_count);
      context->db.registerConfigEntry("tpcc_warehouse_affinity", FLAGS_tpcc_warehouse_affinity);
      context->db.registerConfigEntry("tpcc_threads", FLAGS_tpcc_threads);
      context->db.registerConfigEntry("run_until_tx", FLAGS_run_until_tx);

      context_ptr = context.get();
      dt_table = new leanstore::profiling::DTTable(context->db.getBufferManager());
      dt_table->open();

      return context;
   };

   int loadCore()
   {
      cout << "Loading TPC-C" << endl;

      auto& crm = context_ptr->db.getCRManager();
      auto& tpcc = context_ptr->tpcc;

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
               // if (load_stock)
                  // tpcc.loadStock(w_id);
               tpcc.loadDistrict(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  tpcc.loadCustomer(w_id, d_id);
                  tpcc.loadOrders(w_id, d_id);
               }
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
      return 0;
   };

   template <int id_count>
   int verifyCore(TPCCBaseWorkload<LeanStoreAdapter, id_count>* tpcc_base)
   {
      auto& crm = context_ptr->db.getCRManager();
      auto& tpcc = context_ptr->tpcc;

      std::cout << "Recovered TPC-C. Verifying..." << std::endl;
      cout << "Verifying TPC-C" << endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
         tpcc.verifyItems();
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
               cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
               tpcc_base->verifyWarehouse(w_id);
               cr::Worker::my().commitTX();
            }
         });
         crm.joinAll();
      }
      return 0;
   };

   template <int id_count>
   int scheduleTransations(TPCCBaseWorkload<LeanStoreAdapter, id_count>* tpcc_base,
                           atomic<u64>& keep_running,
                           atomic<u64>& running_threads_counter,
                           std::vector<u64>& tx_per_thread)
   {
      leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
      auto& crm = context_ptr->db.getCRManager();
      auto& tpcc = context_ptr->tpcc;

      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&, tpcc_base, t_i]() {
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
                  if (t_i != 0) {
                     tpcc.touch(w_id); // One thread to run standard TPCC transactions
                  } else {
                     tpcc_base->tx(w_id, FLAGS_read_percentage, FLAGS_scan_percentage, FLAGS_write_percentage, FLAGS_order_size);
                  }
                  if (FLAGS_tpcc_abort_pct && tpcc.urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX(); // counter: cc_ms_oltp_tx
                  } else {
                     cr::Worker::my().commitTX(); // counter: rfa_committed_tx
                  }
                  if (t_i == 0)
                     WorkerCounters::myCounters().tx++;
                  tx_acc = tx_acc + 1;
               }
               jumpmuCatch()
               {
                  if (t_i == 0)
                     WorkerCounters::myCounters().tx_abort++;
               }
            }
            cr::Worker::my().shutdown();
            tx_per_thread[t_i] = tx_acc;
            running_threads_counter--;
         });
      }
      return 0;
   }
};

#include <iostream>
#include <unistd.h>

#define RUN_UNTIL(db, keep_running, running_threads_counter, tx_per_thread, worker_threads, run_until_tx, run_for_seconds) \
   { \
      if (run_until_tx) { \
         while (true) { \
            if ((db).getGlobalStats().accumulated_tx_counter >= (run_until_tx)) { \
               std::cout << (run_until_tx) << " has been reached"; \
               break; \
            } \
            usleep(500); \
         } \
      } else { \
         /* Shutdown threads */ \
         sleep(run_for_seconds); \
      } \
      (keep_running) = false; \
      while ((running_threads_counter)) { \
      } \
      (db).getCRManager().joinAll(); \
   } \
   std::cout << std::endl; \
   { \
      u64 total = 0; \
      std::cout << "TXs per thread = "; \
      for (u64 t_i = 0; t_i < (worker_threads); t_i++) { \
         total += (tx_per_thread)[t_i]; \
         std::cout << (tx_per_thread)[t_i] << ", "; \
      } \
      std::cout << std::endl; \
      std::cout << "Total TPC-C TXs = " << total << std::endl; \
   }

#define INITIALIZE_CONTEXT() \
   gflags::SetUsageMessage("Leanstore Join TPC-C"); \
   gflags::ParseCommandLineFlags(&argc, &argv, true); \
   LeanStoreExperimentHelper helper; \
   auto context = helper.prepareExperiment(); \
   auto& crm = context->db.getCRManager(); \
   auto& db = context->db; \
   auto& tpcc = context->tpcc; \

#if INCLUDE_COLUMNS == 1
#define INITIALIZE_SECONDARY_INDEXES(crm) \
   LeanStoreAdapter<orderline_sec_t> orderline_secondary; \
   LeanStoreAdapter<stock_sec_t>* stock_secondary_ptr; \
   stock_secondary_ptr = &context->stock; \
   crm.scheduleJobSync(0, [&]() { \
      orderline_secondary = LeanStoreAdapter<orderline_sec_t>(db, "orderline_secondary"); \
   });
#else
#define INITIALIZE_SECONDARY_INDEXES(crm) \
   LeanStoreAdapter<orderline_sec_t> orderline_secondary; \
   LeanStoreAdapter<stock_sec_t>* stock_secondary_ptr; \
   LeanStoreAdapter<stock_sec_t> stock_secondary; \
   stock_secondary_ptr = &stock_secondary; \
   crm.scheduleJobSync(0, [&]() { \
      orderline_secondary = LeanStoreAdapter<orderline_sec_t>(db, "orderline_secondary"); \
      stock_secondary = LeanStoreAdapter<stock_sec_t>(db, "stock_secondary"); \
   });
#endif