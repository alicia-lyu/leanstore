#pragma once
#include "../shared/LeanStoreAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "ExperimentHelper.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"

class LeanStoreExperimentHelper : public ExperimentHelper
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

   int loadCore(bool load_stock = true)
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
               if (load_stock)
                  tpcc.loadStock(w_id);
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

   int verifyCore(TPCCBaseWorkload<LeanStoreAdapter>* tpcc_base)
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

   int scheduleTransations(TPCCBaseWorkload<LeanStoreAdapter>* tpcc_base,
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
                  tpcc_base->tx(w_id, FLAGS_read_percentage, FLAGS_scan_percentage, FLAGS_write_percentage, FLAGS_order_size);
                  if (FLAGS_tpcc_abort_pct && tpcc.urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                  } else {
                     cr::Worker::my().commitTX();
                  }
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
      return 0;
   }
};
