#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include "../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedScanner.hpp"
#include "../tpc-h/rocksdb_logger.hpp"
#include "../tpc-h/tables.hpp"
#include "executable_helper.hpp"
#include "executable_params.hpp"
#include "views.hpp"
#include "workload.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 50, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 30, "Number of seconds to run each type of transactions");
DEFINE_int32(
    storage_structure,
    0,
    "Storage structure: 0 to force reload, 1 for traditional indexes, 2 for materialized views, 3 for merged indexes, 4 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");  // flush out loading data from the buffer pool

using namespace geo_join;

using GJ = GeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;
using EH = ExecutableHelper<RocksDBAdapter>;

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
   RocksDBAdapter<view_t> view(rocks_db);

   RocksDBMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t> mergedGeoJoin(rocks_db);
   RocksDBMergedAdapter<nation2_t, states_t> ns(rocks_db);
   RocksDBMergedAdapter<county_t, city_t, customer2_t> ccc(rocks_db);
   // -------------------------------------------------------------------------------------
   rocks_db.open();  // only after all adapters are created (along with their column families)

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<RocksDBAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, view, ns, ccc, nation2, states, county, city, customer2);
   if (!FLAGS_recover || FLAGS_storage_structure == 0) {
      tpchGeoJoin.load();
      return 0;
   } else {
      tpch.recover_last_ids();
   }

   ExeParams<GJ> params(tpchGeoJoin);

   switch (FLAGS_storage_structure) {
      case 1: {
         EH helper_base(rocks_db, "base", tpch, std::bind(&GJ::get_indexes_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin),
                        params.elapsed_cbs_base, params.tput_cbs_base, params.tput_prefixes, std::bind(&GJ::cleanup_base, &tpchGeoJoin));
         helper_base.run();
         break;
      }
      case 2: {
         EH helper_view(rocks_db, "view", tpch, std::bind(&GJ::get_view_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin),
                        params.elapsed_cbs_view, params.tput_cbs_view, params.tput_prefixes, std::bind(&GJ::cleanup_view, &tpchGeoJoin));
         helper_view.run();
         break;
      }
      case 3: {
         EH helper_merged(rocks_db, "merged", tpch, std::bind(&GJ::get_merged_size, &tpchGeoJoin),
                          std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_merged, params.tput_cbs_merged,
                          params.tput_prefixes, std::bind(&GJ::cleanup_merged, &tpchGeoJoin));
         helper_merged.run();
         break;
      }
      case 4: {
         EH helper_2merged(rocks_db, "2merged", tpch, std::bind(&GJ::get_2merged_size, &tpchGeoJoin),
                           std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_2merged, params.tput_cbs_2merged,
                           params.tput_prefixes, std::bind(&GJ::cleanup_2merged, &tpchGeoJoin));
         helper_2merged.run();
         break;
      }
      default:
         return -1;
   }

   return 0;
};