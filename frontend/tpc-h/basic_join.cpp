#include <gflags/gflags.h>
#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/LeanStoreMergedAdapter.hpp"
#include "TPCHWorkload.hpp"
#include "Tables.hpp"
#include "Views.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"

using namespace leanstore;

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
   LeanStoreAdapter<joinedPPs_t> joinedPPs;
   LeanStoreAdapter<merged_lineitem_t> sortedLineitem;
   LeanStoreMergedAdapter mergedBasicJoin;

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
      mergedBasicJoin = LeanStoreMergedAdapter(db, "mergedBasicJoin");
      joinedPPsL = LeanStoreAdapter<joinedPPsL_t>(db, "joinedPPsL");
      joinedPPs = LeanStoreAdapter<joinedPPs_t>(db, "joinedPPs");
      sortedLineitem = LeanStoreAdapter<merged_lineitem_t>(db, "sortedLineitem");
   });

   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;
   // -------------------------------------------------------------------------------------
   TPCHWorkload<LeanStoreAdapter, LeanStoreMergedAdapter> tpch(db, part, supplier, partsupp, customer, orders, lineitem, nation, region, mergedBasicJoin, joinedPPsL, joinedPPs, sortedLineitem);

   if (!FLAGS_recover) {
        std::cout << "Loading TPC-H" << std::endl;
        crm.scheduleJobSync(0, [&]() {
            cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
            tpch.loadPart();
            tpch.loadSupplier();
            tpch.loadCustomer();
            tpch.loadOrders();
            tpch.loadPartsuppLineitem();
            tpch.loadNation();
            tpch.loadRegion();
            tpch.loadSortedLineitem();
            tpch.loadMergedBasicJoin();
            tpch.loadBasicJoin();
            tpch.logSize();
            cr::Worker::my().commitTX();
        });
   }

   crm.scheduleJobSync(0, [&]() {
      tpch.prepare();
      cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
      tpch.basicJoin();
      cr::Worker::my().commitTX();
      cr::Worker::my().shutdown();
   });
}