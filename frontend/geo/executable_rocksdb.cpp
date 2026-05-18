#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include "../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedScanner.hpp"
#include "../shared/logger/rocksdb_logger.hpp"
#include "executable_helper.hpp"
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
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");                                     // flush out loading data from the buffer pool
DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");  // empirical optimal value
DEFINE_bool(geo_bg_thread, false, "Run a background thread issuing geo-only maintain/erase TXs on customer2");
// Compat shims — see executable_leanstore.cpp for rationale.
DEFINE_int32(tpch_scale_factor, 15, "[geo compat] mirrored from --geo_scale_factor for the shared logger");
DEFINE_int32(bgw_pct, 0, "[geo compat] always 0 in the geo microbenchmark");

using namespace geo_join;

using GJ = GeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;
using BaseWorkload = PerStructureWorkload<BaseGeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>>;
using ViewWorkload = PerStructureWorkload<ViewGeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>>;
using MergedWorkload = PerStructureWorkload<MergedGeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>>;
using HashWorkload = PerStructureWorkload<HashGeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>>;

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   LoggerFlusher<HashLogger> final_flusher;
   gflags::SetUsageMessage("RocksDB GeoJoin (microbenchmark)");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   FLAGS_tpch_scale_factor = FLAGS_geo_scale_factor;

   auto type = RocksDB::DB_TYPE::TransactionDB;
   RocksDB rocks_db(type);

   // Geo hierarchy indexes
   RocksDBAdapter<nation2_t> nation2(rocks_db);
   RocksDBAdapter<states_t> states(rocks_db);
   RocksDBAdapter<county_t> county(rocks_db);
   RocksDBAdapter<city_t> city(rocks_db);
   RocksDBAdapter<customer2_t> customer2(rocks_db);
   // Views
   RocksDBAdapter<nscci_t> geo_view(rocks_db);
   RocksDBAdapter<customer_count_t> cust_count_view(rocks_db);
   RocksDBAdapter<view_t> view(rocks_db);
   RocksDBMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t> mergedGeoJoin(rocks_db);
   // -------------------------------------------------------------------------------------
   rocks_db.open();  // only after all adapters are created (along with their column families)

   RocksDBLogger logger(rocks_db);
   GJ geoJoin(logger, mergedGeoJoin, geo_view, cust_count_view, view, nation2, states, county, city, customer2);
   if (!FLAGS_recover) {
      geoJoin.load();
      return 0;
   } else {
      geoJoin.recover_last_customer_id();
   }

   switch (FLAGS_storage_structure) {
      case 1: {
         auto base_workload =
             std::make_unique<BaseWorkload>(geoJoin, "base_idx");
         using EH = ExecutableHelper<BaseWorkload, RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;
         EH helper(rocks_db, std::unique_ptr(std::move(base_workload)), logger);
         helper.run();
         break;
      }
      case 2: {
         auto view_workload =
             std::make_unique<ViewWorkload>(geoJoin, "mat_view");
         using EH = ExecutableHelper<ViewWorkload, RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;
         EH helper(rocks_db, std::unique_ptr(std::move(view_workload)), logger);
         helper.run();
         break;
      }
      case 3: {
         auto merged_workload =
             std::make_unique<MergedWorkload>(geoJoin, "merged_idx");
         using EH = ExecutableHelper<MergedWorkload, RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;
         EH helper(rocks_db, std::unique_ptr(std::move(merged_workload)), logger);
         helper.run();
         break;
      }
      case 4: {
         auto hash_workload =
             std::make_unique<HashWorkload>(geoJoin, "hash");
         using EH = ExecutableHelper<HashWorkload, RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;
         EH helper(rocks_db, std::unique_ptr(std::move(hash_workload)), logger);
         helper.run();
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option: " << FLAGS_storage_structure << std::endl;
         return -1;
      }
   }

   return 0;
};
