#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include "../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedScanner.hpp"
#include "../tpc-h/rocksdb_logger.hpp"
#include "../tpc-h/tables.hpp"
#include "executable_helper.hpp"
#include "per_structure_workload.hpp"
#include "views.hpp"
#include "workload.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 50, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 15, "Number of seconds to run each type of transactions");
DEFINE_int32(
    storage_structure,
    0,
    "Storage structure: 0 to force reload, 1 for traditional indexes, 2 for materialized views, 3 for merged indexes, 4 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");                                     // flush out loading data from the buffer pool
DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");  // empirical optimal value
DEFINE_int32(bgw_pct, 10, "Percentage of writes in background transactions (0-100)");

using namespace geo_join;

using GJ = GeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;
using EH = ExecutableHelper<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore GeoJoin TPC-H");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   auto type = RocksDB::DB_TYPE::TransactionDB;
   RocksDB rocks_db(type);

   // Tables
   RocksDBAdapter<part_t> part(rocks_db);
   RocksDBAdapter<supplier_t> supplier(rocks_db);
   RocksDBAdapter<partsupp_t> partsupp(rocks_db);
   RocksDBAdapter<customerh_t> customer(rocks_db);
   RocksDBAdapter<orders_t> orders(rocks_db);
   RocksDBAdapter<lineitem_t> lineitem(rocks_db);
   RocksDBAdapter<nation_t> nation(rocks_db);
   RocksDBAdapter<region_t> region(rocks_db);
   // Additional indexes
   RocksDBAdapter<nation2_t> nation2(rocks_db);
   RocksDBAdapter<states_t> states(rocks_db);
   RocksDBAdapter<county_t> county(rocks_db);
   RocksDBAdapter<city_t> city(rocks_db);
   RocksDBAdapter<customer2_t> customer2(rocks_db);
   // Views
   RocksDBAdapter<mixed_view_t> mixed_view(rocks_db);
   RocksDBAdapter<view_t> view(rocks_db);
   RocksDBMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t> mergedGeoJoin(rocks_db);
   // -------------------------------------------------------------------------------------
   rocks_db.open();  // only after all adapters are created (along with their column families)

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<RocksDBAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, mixed_view, view, nation2, states, county, city, customer2);
   if (!FLAGS_recover) {
      tpchGeoJoin.load();
      return 0;
   } else {
      tpch.recover_last_ids();
   }

   switch (FLAGS_storage_structure) {
      case 1: {
         auto base_workload =
             std::make_unique<BaseWorkload<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>>(tpchGeoJoin);
         EH helper(rocks_db, std::unique_ptr<PerStructureWorkload>(std::move(base_workload)), tpch);
         helper.run();
         break;
      }
      case 2: {
         auto view_workload =
             std::make_unique<ViewWorkload<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>>(tpchGeoJoin);
         EH helper(rocks_db, std::unique_ptr<PerStructureWorkload>(std::move(view_workload)), tpch);
         helper.run();
         break;
      }
      case 3: {
         auto merged_workload =
             std::make_unique<MergedWorkload<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>>(tpchGeoJoin);
         EH helper(rocks_db, std::unique_ptr<PerStructureWorkload>(std::move(merged_workload)), tpch);
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