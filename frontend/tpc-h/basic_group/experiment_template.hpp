#pragma once

#include <gflags/gflags.h>
#include "../../shared/LeanStoreMergedAdapter.hpp"
#include "../leanstore_executable_helper.hpp"
#include "../leanstore_logger.hpp"
#include "../tables.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "views.hpp"
#include "workload.hpp"

template <typename merged_count_option_t, typename merged_sum_option_t, typename merged_partsupp_option_t>
int run()
{
   using namespace basic_group;
   using namespace leanstore;
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
   });

   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);

   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;

   LeanStoreLogger logger(db);

   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   BasicGroup<LeanStoreAdapter, LeanStoreMergedAdapter, merged_count_option_t, merged_sum_option_t, merged_partsupp_option_t> tpchBasicGroup(
       tpch, mergedBasicGroup, view);

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

   WARMUP_THEN_TXS(tpchBasicGroup, tpch, crm, isolation_level, pointLookupsForMerged, queryByMerged, maintainMerged);

   WARMUP_THEN_TXS(tpchBasicGroup, tpch, crm, isolation_level, pointLookupsForView, queryByView, maintainView);

   return 0;
}