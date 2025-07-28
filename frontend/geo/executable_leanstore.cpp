#include <gflags/gflags.h>
#include "../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../tpc-h/leanstore_logger.hpp"
#include "../tpc-h/tables.hpp"
#include "executable_helper.hpp"
#include "executable_params.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "views.hpp"
#include "workload.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 10, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 30, "Number of seconds to run each type of transactions");
DEFINE_int32(
    storage_structure,
    0,
    "Storage structure: 0 to force reload, 1 for traditional indexes, 2 for materialized views, 3 for merged indexes, 4 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");
DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

using namespace geo_join;

using GJ = GeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;
using EH = ExecutableHelper<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;

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
   LeanStoreAdapter<nation2_t> nation2;
   LeanStoreAdapter<states_t> states;
   LeanStoreAdapter<county_t> county;
   LeanStoreAdapter<city_t> city;
   LeanStoreAdapter<customer2_t> customer2;
   // Views
   LeanStoreAdapter<ns_t> ns_view;
   LeanStoreAdapter<nsc_t> nsc_view;
   LeanStoreAdapter<nscci_t> nscci_view;
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

      nation2 = LeanStoreAdapter<nation2_t>(db, "nation2");
      states = LeanStoreAdapter<states_t>(db, "states");
      county = LeanStoreAdapter<county_t>(db, "county");
      city = LeanStoreAdapter<city_t>(db, "city");
      customer2 = LeanStoreAdapter<customer2_t>(db, "customer2");
      mergedGeoJoin = LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t>(db, "mergedGeoJoin");
      ns_view = LeanStoreAdapter<ns_t>(db, "ns_view");
      nsc_view = LeanStoreAdapter<nsc_t>(db, "nsc_view");
      nscci_view = LeanStoreAdapter<nscci_t>(db, "nscci_view");
      view = LeanStoreAdapter<view_t>(db, "view");
      ns = LeanStoreMergedAdapter<nation2_t, states_t>(db, "ns");
      ccc = LeanStoreMergedAdapter<county_t, city_t, customer2_t>(db, "ccc");
   });
   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, ns_view, nsc_view, nscci_view, view, ns, ccc, nation2, states, county, city, customer2);

   if (!FLAGS_recover || FLAGS_storage_structure == 0) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpchGeoJoin.load();
         cr::Worker::my().commitTX();
      });
      return 0;
   } else {
      tpch.recover_last_ids();
      tpchGeoJoin.select_to_insert();
   }

   ExeParams<GJ> params(tpchGeoJoin);

   switch (FLAGS_storage_structure) {
      case 1: {
         EH helper(crm, "base_idx", tpch, tpchGeoJoin, std::bind(&GJ::get_indexes_size, &tpchGeoJoin),
                   std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_base, params.tput_cbs_base, params.tput_prefixes);
         helper.run();
         break;
      }
      case 2: {
         EH helper(crm, "mat_view", tpch, tpchGeoJoin, std::bind(&GJ::get_view_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin),
                   params.elapsed_cbs_view, params.tput_cbs_view, params.tput_prefixes);
         helper.run();
         break;
      }
      case 3: {
         EH helper(crm, "merged_idx", tpch, tpchGeoJoin, std::bind(&GJ::get_merged_size, &tpchGeoJoin),
                   std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_merged, params.tput_cbs_merged, params.tput_prefixes);
         helper.run();
         break;
      }
      case 4: {
         EH helper(crm, "2merged", tpch, tpchGeoJoin, std::bind(&GJ::get_2merged_size, &tpchGeoJoin),
                   std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_2merged, params.tput_cbs_2merged, params.tput_prefixes);
         helper.run();
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option: " << FLAGS_storage_structure << std::endl;
         return -1;
      }
   }
};