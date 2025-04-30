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

template <typename merged_view_option_t, typename merged_partsupp_option_t>
int run()
{
   using namespace basic_group;
   using namespace leanstore;
   using BG = BasicGroup<LeanStoreAdapter, LeanStoreMergedAdapter, merged_view_option_t, merged_partsupp_option_t>;
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
   LeanStoreMergedAdapter<merged_view_option_t, merged_partsupp_option_t> mergedBasicGroup;

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
      mergedBasicGroup = LeanStoreMergedAdapter<merged_view_option_t, merged_partsupp_option_t>(db, "mergedBasicGroup");
      view = LeanStoreAdapter<view_t>(db, "view");
   });

   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);

   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;

   LeanStoreLogger logger(db);

   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   BG tpchBasicGroup(tpch, mergedBasicGroup, view);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpchBasicGroup.load();
         cr::Worker::my().commitTX();
      });
   } else {
      tpch.recover_last_ids();
      tpchBasicGroup.log_sizes();

      std::vector<std::string> tput_prefixes = {"point-query", "maintain"};

      std::vector<std::function<void()>> elapsed_cbs_view = {std::bind(&BG::queryByView, &tpchBasicGroup)};

      std::vector<std::function<void()>> tput_cbs_view = {std::bind(&BG::pointQueryByView, &tpchBasicGroup),
                                                          std::bind(&BG::maintainView, &tpchBasicGroup)};

      WARMUP_THEN_TXS(
          tpch, crm, isolation_level, [&tpchBasicGroup]() { tpchBasicGroup.pointLookupsForView(); }, elapsed_cbs_view, tput_cbs_view, tput_prefixes,
          "view");

      std::vector<std::function<void()>> elapsed_cbs_merged = {std::bind(&BG::queryByMerged, &tpchBasicGroup)};
      std::vector<std::function<void()>> tput_cbs_merged = {std::bind(&BG::pointQueryByMerged, &tpchBasicGroup),
                                                            std::bind(&BG::maintainMerged, &tpchBasicGroup)};

      WARMUP_THEN_TXS(
          tpch, crm, isolation_level, [&tpchBasicGroup]() { tpchBasicGroup.pointLookupsForMerged(); }, elapsed_cbs_merged, tput_cbs_merged,
          tput_prefixes, "merged");
   }
   return 0;
}