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

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");

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
   LeanStoreAdapter<joinedPPsL_t> joinedPPsL;
   LeanStoreAdapter<sorted_lineitem_t> sortedLineitem;
   LeanStoreMergedAdapter<merged_part_t, merged_partsupp_t, merged_lineitem_t> mergedBasicJoin;

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
      mergedBasicJoin = LeanStoreMergedAdapter<merged_part_t, merged_partsupp_t, merged_lineitem_t>(db, "mergedBasicJoin");
      joinedPPsL = LeanStoreAdapter<joinedPPsL_t>(db, "joinedPPsL");
      sortedLineitem = LeanStoreAdapter<sorted_lineitem_t>(db, "sortedLineitem");
   });

   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   TPCHWorkload<LeanStoreAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   basic_join::BasicJoin<LeanStoreAdapter, LeanStoreMergedAdapter> tpchBasicJoin(tpch, mergedBasicJoin, joinedPPsL, sortedLineitem);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         logger.reset();
         tpchBasicJoin.loadBaseTables();
         tpchBasicJoin.loadSortedLineitem();
         tpchBasicJoin.loadBasicJoin();
         tpchBasicJoin.loadMergedBasicJoin();
         tpchBasicJoin.logSize();
         logger.logLoading();
         cr::Worker::my().commitTX();
      });
   }

   WARMUP_THEN_TXS(tpchBasicJoin, tpch, crm, isolation_level, pointLookupsForMerged, queryByMerged, pointQueryByMerged, maintainMerged);

   WARMUP_THEN_TXS(tpchBasicJoin, tpch, crm, isolation_level, pointLookupsForBase, queryByBase, pointQueryByBase, maintainBase);

   WARMUP_THEN_TXS(tpchBasicJoin, tpch, crm, isolation_level, pointLookupsForView, queryByView, pointQueryByView, maintainView);
}