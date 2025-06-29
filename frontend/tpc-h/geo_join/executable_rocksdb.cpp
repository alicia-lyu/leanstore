#include <gflags/gflags.h>
#include "../../shared/RocksDBMergedAdapter.hpp"
#include "../../shared/RocksDBMergedScanner.hpp"
#include "../executable_helper.hpp"
#include "../rocksdb_logger.hpp"
#include "../tables.hpp"
#include "views.hpp"
#include <rocksdb/db.h>
#include "../../shared/RocksDBAdapter.hpp"
#include "workload.hpp"
#include "executable_params.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 10, "Number of seconds to run each type of transactions");
DEFINE_int32(storage_structure, 0, "Storage structure: 0 for traditional indexes, 1 for materialized views, 2 for merged indexes, 3 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 30, "Warmup seconds"); // flush out loading data from the buffer pool

using namespace geo_join;

using GJ = GeoJoin<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>;

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
   rocks_db.open(); // only after all adapters are created (along with their column families)

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<RocksDBAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, view, ns, ccc, states, county, city, customer2);

   tpchGeoJoin.load();

   ExeParams<GJ> params(tpchGeoJoin);
   
   switch (FLAGS_storage_structure) {
      case 0: {
         std::cout << "TPC-H with traditional indexes" << std::endl;
         
         ExecutableHelper<RocksDBAdapter> helper(rocks_db, "base", tpch, std::bind(&GJ::get_indexes_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_base, params.tput_cbs_base, params.tput_prefixes);
         helper.run();
         break;
      }
      case 1: {
         std::cout << "TPC-H with materialized views" << std::endl;

         ExecutableHelper<RocksDBAdapter> helper(rocks_db, "view", tpch, std::bind(&GJ::get_view_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_view, params.tput_cbs_view, params.tput_prefixes);
         helper.run();
         break;
      }
      case 2: {
         std::cout << "TPC-H with merged indexes" << std::endl;

         ExecutableHelper<RocksDBAdapter> helper(rocks_db, "merged", tpch, std::bind(&GJ::get_merged_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_merged, params.tput_cbs_merged, params.tput_prefixes);
         helper.run();
         break;
      }
      case 3 : {
         std::cout << "TPC-H with 2 merged indexes" << std::endl;

         ExecutableHelper<RocksDBAdapter> helper(rocks_db, "2merged", tpch, std::bind(&GJ::get_2merged_size, &tpchGeoJoin), std::bind(&GJ::point_lookups_of_rest, &tpchGeoJoin), params.elapsed_cbs_2merged, params.tput_cbs_2merged, params.tput_prefixes);
         helper.run();
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option" << std::endl;
         return -1;
      }
   }
};