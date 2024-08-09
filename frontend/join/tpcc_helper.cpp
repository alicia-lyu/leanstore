#include <filesystem>
#include <sstream>
#include "../shared/LeanStoreAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"
#include "TPCCBaseWorkload.hpp"
#include "TPCCJoinWorkload.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"

using orderline_sec_t = typename std::conditional<INCLUDE_COLUMNS == 0, ol_sec_key_only_t, ol_join_sec_t>::type;
using joined_t = typename std::conditional<INCLUDE_COLUMNS == 0, joined_ols_key_only_t, joined_ols_t>::type;

std::string getConfigString()
{
   std::stringstream config;
   config << FLAGS_target_gib << "|" << FLAGS_semijoin_selectivity << "|" << INCLUDE_COLUMNS;
   return config.str();
}

struct ExperimentContext {
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

   ExperimentContext(int warehouse_count, int order_wdc_index, bool remove, bool handle_anomalies, int warehouse_affinity)
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

std::unique_ptr<ExperimentContext> prepareExperiment()
{
   assert(FLAGS_tpcc_warehouse_count > 0);
   assert(FLAGS_read_percentage + FLAGS_scan_percentage + FLAGS_write_percentage == 100);

   LeanStore::addS64Flag("TPC_SCALE", &FLAGS_tpcc_warehouse_count);

   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   const bool should_tpcc_driver_handle_isolation_anomalies = isolation_level < leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;

   std::unique_ptr<ExperimentContext> context =
       std::make_unique<ExperimentContext>(FLAGS_tpcc_warehouse_count, FLAGS_order_wdc_index, FLAGS_tpcc_remove,
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

   return context;
};

void logSize(std::string table, std::chrono::steady_clock::time_point prev)
{
   filesystem::path csv_path = std::filesystem::path(FLAGS_csv_path).parent_path().parent_path() / "join_size.csv";
   std::ofstream csv_file(csv_path, std::ios::app);
   if (filesystem::file_size(csv_path) == 0)
      csv_file << "table(s),config,size,time" << std::endl;
   std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
   auto config = getConfigString();
   auto size = 0; // TODO
   auto time = std::chrono::duration_cast<std::chrono::milliseconds>(now - prev).count();
   csv_file << table << "," << config << "," << size << "," << time << std::endl;
};

template <template <typename> class AdapterType>
int loadCore(cr::CRManager& crm, TPCCWorkload<AdapterType>& tpcc, bool load_stock = true)
{
   cout << "Loading TPC-C" << endl;

   std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();

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
               tpcc.loadStock(w_id, FLAGS_semijoin_selectivity);
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
   logSize("core", t1);
   return 0;
};

template <template <typename> class AdapterType>
int verifyCore(cr::CRManager& crm, TPCCWorkload<AdapterType>& tpcc, TPCCBaseWorkload<AdapterType>* tpcc_base)
{
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

template <template <typename> class AdapterType>
int scheduleTransations(cr::CRManager& crm, TPCCWorkload<AdapterType>& tpcc, TPCCBaseWorkload<AdapterType>* tpcc_base, atomic<u64>& keep_running, atomic<u64>& running_threads_counter, u64* tx_per_thread)
{
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);

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