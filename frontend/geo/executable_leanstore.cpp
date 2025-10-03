#include <gflags/gflags.h>
#include "../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../shared/logger/leanstore_logger.hpp"
#include "tpch_tables.hpp"
#include "executable_helper.hpp"
#include "per_structure_workload.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "per_structure_workload.hpp"
#include "views.hpp"
#include "workload.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 10, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 15, "Number of seconds to run each type of transactions");
DEFINE_int32(
    storage_structure,
    0,
    "Storage structure: 0 to force reload, 1 for traditional indexes, 2 for materialized views, 3 for merged indexes, 4 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");
DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");
DEFINE_int32(bgw_pct, 10, "Percentage of writes in background transactions (0-100)");

using namespace geo_join;

using GJ = GeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;
using EH = ExecutableHelper<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;

int main(int argc, char** argv)
{
   LoggerFlusher<HashLogger> final_flusher;
   gflags::SetUsageMessage("Leanstore GeoJoin TPC-H");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   LeanStore db;
   // Tables
   LeanStoreAdapter<part_t> part;
   LeanStoreAdapter<supplier_t> supplier;
   LeanStoreAdapter<partsupp_t> partsupp;
   LeanStoreAdapter<customerh_t> customer;
   LeanStoreAdapter<orders_t> orders;
   LeanStoreAdapter<lineitem_t> lineitem;
   LeanStoreAdapter<nation_t> nation;
   LeanStoreAdapter<region_t> region;
   // Additional indexes
   LeanStoreAdapter<nation2_t> nation2;
   LeanStoreAdapter<states_t> states;
   LeanStoreAdapter<county_t> county;
   LeanStoreAdapter<city_t> city;
   LeanStoreAdapter<customer2_t> customer2;
   // Views
   LeanStoreAdapter<mixed_view_t> mixed_view;
   LeanStoreAdapter<view_t> view;

   LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t> mergedGeoJoin;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part = LeanStoreAdapter<part_t>(db, "part");
      partsupp = LeanStoreAdapter<partsupp_t>(db, "partsupp");
      lineitem = LeanStoreAdapter<lineitem_t>(db, "lineitem");
      supplier = LeanStoreAdapter<supplier_t>(db, "supplier");
      customer = LeanStoreAdapter<customerh_t>(db, "customer");
      orders = LeanStoreAdapter<orders_t>(db, "orders");
      nation = LeanStoreAdapter<nation_t>(db, "nation");
      region = LeanStoreAdapter<region_t>(db, "region");

      nation2 = LeanStoreAdapter<nation2_t>(db, "nation2");
      states = LeanStoreAdapter<states_t>(db, "states");
      county = LeanStoreAdapter<county_t>(db, "county");
      city = LeanStoreAdapter<city_t>(db, "city");
      customer2 = LeanStoreAdapter<customer2_t>(db, "customer2");
      mergedGeoJoin = LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t>(db, "mergedGeoJoin");
      mixed_view = LeanStoreAdapter<mixed_view_t>(db, "mixed_view");
      view = LeanStoreAdapter<view_t>(db, "view");
   });
   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, mixed_view, view, nation2, states, county, city, customer2);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpchGeoJoin.load();
         cr::Worker::my().commitTX();
      });
      return 0;
   } else {
      tpch.recover_last_ids();
   }

   switch (FLAGS_storage_structure) {
      case 1: {
         auto base_workload =
             std::make_unique<BaseWorkload<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>(tpchGeoJoin);
         EH helper(crm, std::unique_ptr<PerStructureWorkload>(std::move(base_workload)), tpch);
         helper.run();
         break;
      }
      case 2: {
         auto view_workload =
             std::make_unique<ViewWorkload<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>(tpchGeoJoin);
         EH helper(crm, std::unique_ptr<PerStructureWorkload>(std::move(view_workload)), tpch);
         helper.run();
         break;
      }
      case 3: {
         auto merged_workload =
             std::make_unique<MergedWorkload<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>(tpchGeoJoin);
         EH helper(crm, std::unique_ptr<PerStructureWorkload>(std::move(merged_workload)), tpch);
         helper.run();
         break;
      }
      case 4: {
         auto geo_join_workload =
             std::make_unique<HashWorkload<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>(tpchGeoJoin);
         EH helper(crm, std::unique_ptr<PerStructureWorkload>(std::move(geo_join_workload)), tpch);
         helper.run();
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option: " << FLAGS_storage_structure << std::endl;
         return -1;
      }
   }
};