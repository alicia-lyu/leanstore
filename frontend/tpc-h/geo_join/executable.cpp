#include <gflags/gflags.h>
#include "../../shared/LeanStoreMergedAdapter.hpp"
#include "../leanstore_executable_helper.hpp"
#include "../leanstore_logger.hpp"
#include "../tables.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "views.hpp"
#include "workload.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");
DEFINE_int32(tx_count, 2000, "Number of transactions to run");
DEFINE_int32(storage_structure, 0, "Storage structure: 0 for traditional indexes, 1 for materialized views, 2 for merged indexes");

using namespace geo_join;

using GJ = GeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;

int main(int argc, char** argv)
{
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
   LeanStoreAdapter<states_t> states;
   LeanStoreAdapter<county_t> county;
   LeanStoreAdapter<city_t> city;
   // Views
   LeanStoreAdapter<view_t> view;
   LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t> mergedGeoJoin;

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
      states = LeanStoreAdapter<states_t>(db, "states");
      county = LeanStoreAdapter<county_t>(db, "county");
      city = LeanStoreAdapter<city_t>(db, "city");
      mergedGeoJoin = LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t>(db, "mergedGeoJoin");
      view = LeanStoreAdapter<view_t>(db, "view");
   });
   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, view, states, county, city);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         logger.reset();
         tpchGeoJoin.load();
         logger.logLoading();
         cr::Worker::my().commitTX();
      });
      return 0;
   } else {
      tpch.recover_last_ids();
      tpchGeoJoin.log_sizes();
   }
   std::vector<std::string> tput_prefixes = {"point-query", "maintain"};
   
   switch (FLAGS_storage_structure) {
      case 0: {
         std::cout << "TPC-H with traditional indexes" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_base = {std::bind(&GJ::query_by_base, &tpchGeoJoin),
                                                                std::bind(&GJ::range_query_by_base, &tpchGeoJoin)};
         std::vector<std::function<void()>> tput_cbs_base = {std::bind(&GJ::point_query_by_base, &tpchGeoJoin),
                                                             std::bind(&GJ::maintain_base, &tpchGeoJoin)};
         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchGeoJoin.point_lookups_of_rest(); }, elapsed_cbs_base, tput_cbs_base, tput_prefixes, "base");
         break;
      }
      case 1: {
         std::cout << "TPC-H with materialized views" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_view = {std::bind(&GJ::query_by_view, &tpchGeoJoin),
                                                                std::bind(&GJ::range_query_by_view, &tpchGeoJoin)};
         std::vector<std::function<void()>> tput_cbs_view = {[&]() { tpchGeoJoin.point_query_by_view(); },
                                                             std::bind(&GJ::maintain_view, &tpchGeoJoin)};
         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchGeoJoin.point_lookups_of_rest(); }, elapsed_cbs_view, tput_cbs_view, tput_prefixes, "view");
         break;
      }
      case 2: {
         std::cout << "TPC-H with merged indexes" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_merged = {std::bind(&GJ::query_by_merged, &tpchGeoJoin),
                                                                  std::bind(&GJ::range_query_by_merged, &tpchGeoJoin)};
         std::vector<std::function<void()>> tput_cbs_merged = {std::bind(&GJ::point_query_by_merged, &tpchGeoJoin),
                                                               std::bind(&GJ::maintain_merged, &tpchGeoJoin)};
         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchGeoJoin.point_lookups_of_rest(); }, elapsed_cbs_merged, tput_cbs_merged, tput_prefixes,
             "merged");
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option" << std::endl;
         return -1;
      }
   }
};