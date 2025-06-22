#include <gflags/gflags.h>
#include "../../shared/LeanStoreAdapter.hpp"
#include "../../shared/LeanStoreMergedAdapter.hpp"
#include "../executable_helper.hpp"
#include "../leanstore_logger.hpp"
#include "../tables.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "views.hpp"
#include "workload.hpp"
#include "executable_params.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 10, "Number of seconds to run each type of transactions");
DEFINE_int32(storage_structure, 0, "Storage structure: 0 for traditional indexes, 1 for materialized views, 2 for merged indexes, 3 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");

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
   LeanStoreAdapter<customer2_t> customer2;
   // Views
   LeanStoreAdapter<view_t> view;
   LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t> mergedGeoJoin;
   LeanStoreMergedAdapter<nation2_t, states_t> ns;
   LeanStoreMergedAdapter<county_t, city_t, customer2_t> ccc;

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
      customer2 = LeanStoreAdapter<customer2_t>(db, "customer2");
      mergedGeoJoin = LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t>(db, "mergedGeoJoin");
      view = LeanStoreAdapter<view_t>(db, "view");
      ns = LeanStoreMergedAdapter<nation2_t, states_t>(db, "ns");
      ccc = LeanStoreMergedAdapter<county_t, city_t, customer2_t>(db, "ccc");
   });
   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, view, ns, ccc, states, county, city, customer2);

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

   ExeParams<GJ> params(tpchGeoJoin);
   
   
   switch (FLAGS_storage_structure) {
      case 0: {
         std::cout << "TPC-H with traditional indexes" << std::endl;
         ExecutableHelper<LeanStoreAdapter> helper(crm, "base", tpch, std::bind(&GJ::get_indexes_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_base, params.tput_cbs_base, params.tput_prefixes);
         helper.run();
         break;
      }
      case 1: {
         std::cout << "TPC-H with materialized views" << std::endl;

         ExecutableHelper<LeanStoreAdapter> helper(crm, "view", tpch, std::bind(&GJ::get_view_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_view, params.tput_cbs_view, params.tput_prefixes);
         helper.run();
         break;
      }
      case 2: {
         std::cout << "TPC-H with merged indexes" << std::endl;
         ExecutableHelper<LeanStoreAdapter> helper(crm, "merged", tpch, std::bind(&GJ::get_merged_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_merged, params.tput_cbs_merged, params.tput_prefixes);
         helper.run();
         break;
      }
      case 3 : {
         std::cout << "TPC-H with 2 merged indexes" << std::endl;
         ExecutableHelper<LeanStoreAdapter> helper(crm, "2merged", tpch, std::bind(&GJ::get_2merged_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_2merged, params.tput_cbs_2merged, params.tput_prefixes);
         helper.run();
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option" << std::endl;
         return -1;
      }
   }
};