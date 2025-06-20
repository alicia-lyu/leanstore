#include <gflags/gflags.h>
#include "../../shared/RocksDBMergedAdapter.hpp"
#include "../../shared/RocksDBMergedScanner.hpp"
#include "../executable_helper.hpp"
#include "../rocksdb_logger.hpp"
#include "../tables.hpp"
#include "views.hpp"
#include <rocksdb/db.h>
#include "../shared/RocksDBAdapter.hpp"
#include "workload.hpp"

using namespace leanstore;

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 10, "Number of seconds to run each type of transactions");
DEFINE_int32(storage_structure, 0, "Storage structure: 0 for traditional indexes, 1 for materialized views, 2 for merged indexes, 3 for 2 merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");

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
   RocksDBAdapter<part_t> part;
   RocksDBAdapter<supplier_t> supplier;
   RocksDBAdapter<partsupp_t> partsupp;
   RocksDBAdapter<customerh_t> customer;
   RocksDBAdapter<orders_t> orders;
   RocksDBAdapter<lineitem_t> lineitem;
   RocksDBAdapter<nation_t> nation;
   RocksDBAdapter<region_t> region;
   // Additional indexes
   RocksDBAdapter<states_t> states;
   RocksDBAdapter<county_t> county;
   RocksDBAdapter<city_t> city;
   RocksDBAdapter<customer2_t> customer2;
   // Views
   RocksDBAdapter<view_t> view;

   RocksDBMergedAdapter<nation2_t, states_t, county_t, city_t, customer2_t> mergedGeoJoin;
   RocksDBMergedAdapter<nation2_t, states_t> ns;
   RocksDBMergedAdapter<county_t, city_t, customer2_t> ccc;
   // -------------------------------------------------------------------------------------
   rocks_db.open(); // only after all adapters are created (along with their column families)
   RocksDBLogger logger(db);
   TPCHWorkload<RocksDBAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   GJ tpchGeoJoin(tpch, mergedGeoJoin, view, ns, ccc, states, county, city, customer2);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      rocks_db.startTX();
         tpchGeoJoin.load();
         rocks_db.commitTX();
      return 0;
   } else {
      tpch.recover_last_ids();
   }
   std::vector<std::string> tput_prefixes = {
      // "join-ns", 
      // "join-nsc",
      // "join-nscci",  
      // "maintain", 
      // "group-point", 
      "mixed-point"
   };
   
   switch (FLAGS_storage_structure) {
      case 0: {
         std::cout << "TPC-H with traditional indexes" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_base = {
            // std::bind(&GJ::query_by_base, &tpchGeoJoin),                              
            // std::bind(&GJ::agg_by_base, &tpchGeoJoin),
            std::bind(&GJ::mixed_query_by_base, &tpchGeoJoin)
         };
         std::vector<std::function<void()>> tput_cbs_base = {
            // std::bind(&GJ::ns_base, &tpchGeoJoin),
            // std::bind(&GJ::nsc_base, &tpchGeoJoin),
            // std::bind(&GJ::nscci_by_base, &tpchGeoJoin),
            // std::bind(&GJ::maintain_base, &tpchGeoJoin),
            // std::bind(&GJ::point_agg_by_base, &tpchGeoJoin),
            std::bind(&GJ::point_mixed_query_by_base, &tpchGeoJoin)
         };
         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchGeoJoin.point_lookups_of_rest(); }, elapsed_cbs_base, tput_cbs_base, tput_prefixes, "base",
             [&]() { return tpchGeoJoin.get_indexes_size(); });
         break;
      }
      case 1: {
         std::cout << "TPC-H with materialized views" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_view = {
            // std::bind(&GJ::query_by_view, &tpchGeoJoin),     
            // std::bind(&GJ::agg_in_view, &tpchGeoJoin),
            std::bind(&GJ::mixed_query_by_view, &tpchGeoJoin)
         };
         std::vector<std::function<void()>> tput_cbs_view = {
            // std::bind(&GJ::ns_view, &tpchGeoJoin),
            // std::bind(&GJ::nsc_view, &tpchGeoJoin),
            // std::bind(&GJ::nscci_by_view, &tpchGeoJoin),
            // std::bind(&GJ::maintain_view, &tpchGeoJoin),
            // std::bind(&GJ::point_agg_by_view, &tpchGeoJoin),
            std::bind(&GJ::point_mixed_query_by_view, &tpchGeoJoin)
         };
         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchGeoJoin.point_lookups_of_rest(); }, elapsed_cbs_view, tput_cbs_view, tput_prefixes, "view",
             [&]() { return tpchGeoJoin.get_view_size(); });
         break;
      }
      case 2: {
         std::cout << "TPC-H with merged indexes" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_merged = {
            // std::bind(&GJ::query_by_merged, &tpchGeoJoin),
            // std::bind(&GJ::agg_by_merged, &tpchGeoJoin),
            std::bind(&GJ::mixed_query_by_merged, &tpchGeoJoin)
         };
         std::vector<std::function<void()>> tput_cbs_merged = {
            // std::bind(&GJ::ns_merged, &tpchGeoJoin),
            // std::bind(&GJ::nsc_merged, &tpchGeoJoin),
            // std::bind(&GJ::nscci_by_merged, &tpchGeoJoin),
            // std::bind(&GJ::maintain_merged, &tpchGeoJoin),
            // std::bind(&GJ::point_agg_by_merged, &tpchGeoJoin),
            std::bind(&GJ::point_mixed_query_by_merged, &tpchGeoJoin)
         };
         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchGeoJoin.point_lookups_of_rest(); }, elapsed_cbs_merged, tput_cbs_merged, tput_prefixes,
             "merged", [&]() { return tpchGeoJoin.get_merged_size(); });
         break;
      }
      case 3 : {
         std::cout << "TPC-H with 2 merged indexes" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_2merged = {
            // std::bind(&GJ::query_by_2merged, &tpchGeoJoin),
            // std::bind(&GJ::agg_by_2merged, &tpchGeoJoin),
            std::bind(&GJ::mixed_query_by_2merged, &tpchGeoJoin)
         };
         std::vector<std::function<void()>> tput_cbs_2merged = {
            // std::bind(&GJ::ns_by_2merged, &tpchGeoJoin),
            // std::bind(&GJ::nsc_by_2merged, &tpchGeoJoin),
            // std::bind(&GJ::nscci_by_2merged, &tpchGeoJoin),
            // std::bind(&GJ::maintain_2merged, &tpchGeoJoin),
            // std::bind(&GJ::point_agg_by_2merged, &tpchGeoJoin),
            std::bind(&GJ::point_mixed_query_by_2merged, &tpchGeoJoin)
         };
         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchGeoJoin.point_lookups_of_rest(); }, elapsed_cbs_2merged, tput_cbs_2merged, tput_prefixes,
             "2merged", [&]() { return tpchGeoJoin.get_2merged_size(); });
         break;
      }
      default: {
         std::cerr << "Invalid storage structure option" << std::endl;
         return -1;
      }
   }
};