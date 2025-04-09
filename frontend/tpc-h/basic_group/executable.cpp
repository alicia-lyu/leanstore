#include <gflags/gflags.h>
#include "workload.hpp"
#include "views.hpp"
#include "../../shared/LeanStoreMergedAdapter.hpp"
#include "../LeanStoreLogger.hpp"
#include "../Tables.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "../leanstore_executable_helper.hpp"

using namespace leanstore;
using namespace basic_group;

DEFINE_double(tpch_scale_factor, 1, "TPC-H scale factor");

// using merged_count_option_t = merged_count_partsupp_t;
// using merged_sum_option_t = merged_sum_supplycost_t;
// using merged_partsupp_option_t = merged_partsupp_t;

using merged_count_option_t = merged_count_variant_t;
using merged_sum_option_t = merged_sum_variant_t;
using merged_partsupp_option_t = merged_partsupp_variant_t;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Join TPC-H");
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
   // Views
   LeanStoreAdapter<view_t> view;
   LeanStoreAdapter<count_partsupp_t> count_partsupp;
   LeanStoreAdapter<sum_supplycost_t> sum_supplycost;
   LeanStoreMergedAdapter<merged_count_option_t, merged_sum_option_t, merged_partsupp_option_t> mergedBasicGroup;

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
      mergedBasicGroup = LeanStoreMergedAdapter<merged_count_option_t, merged_sum_option_t, merged_partsupp_option_t>(db, "mergedBasicGroup");
      view = LeanStoreAdapter<view_t>(db, "view");
      count_partsupp = LeanStoreAdapter<count_partsupp_t>(db, "count_partsupp");
      sum_supplycost = LeanStoreAdapter<sum_supplycost_t>(db, "sum_supplycost");
   });

   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);

   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;

   LeanStoreLogger logger(db);

   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   BasicGroup<LeanStoreAdapter, LeanStoreMergedAdapter, merged_count_option_t, merged_sum_option_t, merged_partsupp_option_t> tpchBasicGroup(tpch, mergedBasicGroup, view, count_partsupp, sum_supplycost);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpchBasicGroup.loadBaseTables();
         tpchBasicGroup.loadAllOptions();
         tpchBasicGroup.logSize();
         cr::Worker::my().commitTX();
      });
   }

   warmupAndTX(tpchBasicGroup, tpch, crm, isolation_level, pointLookupsForMerged, queryByMerged, maintainMerged);

   warmupAndTX(tpchBasicGroup, tpch, crm, isolation_level, pointLookupsForIndex, queryByIndex, maintainIndex);

   warmupAndTX(tpchBasicGroup, tpch, crm, isolation_level, pointLookupsForView, queryByView, maintainView);
}
