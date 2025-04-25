#include <gflags/gflags.h>
#include "../../shared/LeanStoreMergedAdapter.hpp"
#include "../leanstore_executable_helper.hpp"
#include "../leanstore_logger.hpp"
#include "../tables.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "views.hpp"
#include "workload.hpp"

using namespace leanstore;
using namespace basic_join_group;

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");
DEFINE_int32(tx_count, 1000, "Number of transactions to run");

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
   LeanStoreMergedAdapter<merged_view_t, merged_orders_t, merged_lineitem_t> merged;

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
      merged = LeanStoreMergedAdapter<merged_view_t, merged_orders_t, merged_lineitem_t>(db, "merged");
      view = LeanStoreAdapter<view_t>(db, "view");
   });

   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   BasicJoinGroup<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner> tpchBasicJoinGroup(tpch, merged, view);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         logger.reset();
         tpchBasicJoinGroup.load();
         logger.logLoading();
         cr::Worker::my().commitTX();
      });
   } else {
      tpch.recover_last_ids();
      
      WARMUP_THEN_TXS(tpchBasicJoinGroup, tpch, crm, isolation_level, point_lookups_for_merged, query_by_merged, point_query_by_merged,
                      maintain_view, "merged");
      WARMUP_THEN_TXS(tpchBasicJoinGroup, tpch, crm, isolation_level, point_lookups_for_view, query_by_view, point_query_by_view, maintain_view, "view");
   }
}