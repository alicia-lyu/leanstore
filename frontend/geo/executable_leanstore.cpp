#include <gflags/gflags.h>
#include "../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../shared/logger/leanstore_logger.hpp"
#include "executable_helper.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "per_structure_workload.hpp"
#include "views.hpp"
#include "workload.hpp"

using namespace leanstore;

DEFINE_int32(geo_scale_factor, 15, "Geo benchmark scale factor (customer count = 30000 * geo_scale_factor)");
DEFINE_int32(tx_seconds, 15, "Number of seconds to run each type of transactions");
DEFINE_int32(
    storage_structure,
    0,
    "Storage structure: 0 to force reload, 1 for traditional indexes, 2 for materialized views, 3 for merged indexes, 4 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");
DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");
DEFINE_bool(geo_bg_thread, false, "Run a background thread issuing geo-only maintain/erase TXs on customer2");
// Compat shims for the shared logger CSV writer, which hard-references
// these gflags by name. Geo no longer uses these for any benchmark logic
// — the value of tpch_scale_factor is mirrored from geo_scale_factor after
// flag parsing so the `scale` column in TPut.csv stays meaningful, and
// bgw_pct stays 0 because geo's microbenchmark contract is read-only
// foreground + geo-only writes via --geo_bg_thread.
DEFINE_int32(tpch_scale_factor, 15, "[geo compat] mirrored from --geo_scale_factor for the shared logger");
DEFINE_int32(bgw_pct, 0, "[geo compat] always 0 in the geo microbenchmark");

using namespace geo_join;

using GJ = GeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;
using BaseWorkload = PerStructureWorkload<BaseGeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>;
using ViewWorkload = PerStructureWorkload<ViewGeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>;
using MergedWorkload = PerStructureWorkload<MergedGeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>;
using HashWorkload = PerStructureWorkload<HashGeoJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>>;

int main(int argc, char** argv)
{
   LoggerFlusher<HashLogger> final_flusher;
   gflags::SetUsageMessage("Leanstore GeoJoin (microbenchmark)");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // Mirror the geo scale factor into the shared logger's CSV-column flag so
   // TPut.csv's `scale` column reports the effective scale of the run.
   FLAGS_tpch_scale_factor = FLAGS_geo_scale_factor;
   LeanStore db;
   // Geo hierarchy indexes
   LeanStoreAdapter<nation2_t> nation2;
   LeanStoreAdapter<states_t> states;
   LeanStoreAdapter<county_t> county;
   LeanStoreAdapter<city_t> city;
   LeanStoreAdapter<customer2_t> customer2;
   // Views
   LeanStoreAdapter<nscci_t> geo_view;
   LeanStoreAdapter<customer_count_t> cust_count_view;
   LeanStoreAdapter<view_t> view;

   LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t> mergedGeoJoin;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      nation2 = LeanStoreAdapter<nation2_t>(db, "nation2");
      states = LeanStoreAdapter<states_t>(db, "states");
      county = LeanStoreAdapter<county_t>(db, "county");
      city = LeanStoreAdapter<city_t>(db, "city");
      customer2 = LeanStoreAdapter<customer2_t>(db, "customer2");
      mergedGeoJoin = LeanStoreMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t>(db, "mergedGeoJoin");
      geo_view = LeanStoreAdapter<nscci_t>(db, "geo_view");
      cust_count_view = LeanStoreAdapter<customer_count_t>(db, "cust_count_view");
      view = LeanStoreAdapter<view_t>(db, "view");
   });
   db.registerConfigEntry("geo_scale_factor", FLAGS_geo_scale_factor);
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   GJ geoJoin(logger, mergedGeoJoin, geo_view, cust_count_view, view, nation2, states, county, city, customer2);

   if (!FLAGS_recover) {
      std::cout << "Loading geo hierarchy" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         geoJoin.load();
         cr::Worker::my().commitTX();
      });
      return 0;
   } else {
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX();
         geoJoin.recover_last_customer_id();
         cr::Worker::my().commitTX();
      });
   }

   switch (FLAGS_storage_structure) {
      case 1: {
         auto base_workload = std::make_unique<BaseWorkload>(geoJoin, "base_idx");
         using EH = ExecutableHelper<BaseWorkload, LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;
         EH helper(crm, std::unique_ptr(std::move(base_workload)), logger);
         helper.run();
         break;
      }
      case 2: {
         auto view_workload = std::make_unique<ViewWorkload>(geoJoin, "mat_view");
         using EH = ExecutableHelper<ViewWorkload, LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;
         EH helper(crm, std::unique_ptr(std::move(view_workload)), logger);
         helper.run();
         break;
      }
      case 3: {
         auto merged_workload = std::make_unique<MergedWorkload>(geoJoin, "merged_idx");
         using EH = ExecutableHelper<MergedWorkload, LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;
         EH helper(crm, std::unique_ptr(std::move(merged_workload)), logger);
         helper.run();
         break;
      }
      case 4: {
         auto hash_workload = std::make_unique<HashWorkload>(geoJoin, "hash");
         using EH = ExecutableHelper<HashWorkload, LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner, LeanStoreMergedScanner>;
         EH helper(crm, std::unique_ptr(std::move(hash_workload)), logger);
         helper.run();
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option: " << FLAGS_storage_structure << std::endl;
         return -1;
      }
   }
};
