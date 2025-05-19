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
DEFINE_int32(pq_count, 2000, "Number of point queries to run");
DEFINE_int32(mt_count, 2000, "Number of maintenance TXs to run");DEFINE_int32(storage_structure, 0, "Storage structure: 0 for traditional indexes, 1 for materialized views, 2 for merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");

using namespace basic_join;

using BJ = BasicJoin<LeanStoreAdapter, LeanStoreMergedAdapter, LeanStoreScanner>;

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
   BJ tpchBasicJoin(tpch, mergedBasicJoin, joinedPPsL, sortedLineitem);

   if (!FLAGS_recover) {
      std::cout << "Loading TPC-H" << std::endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         logger.reset();
         tpchBasicJoin.load();
         logger.logLoading();
         cr::Worker::my().commitTX();
      });
   } else {
      tpch.recover_last_ids();
      tpchBasicJoin.log_sizes();

      std::vector<std::string> tput_prefixes = {"point-query", "range-query", "maintain"};

      if (FLAGS_storage_structure == 0) {
         std::cout << "TPC-H with traditional indexes" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_base = {std::bind(&BJ::queryByBase, &tpchBasicJoin)};
         std::vector<std::function<void()>> tput_cbs_base = {std::bind(&BJ::pointQueryByBase, &tpchBasicJoin),
                                                             std::bind(&BJ::range_query_by_base, &tpchBasicJoin),
                                                             std::bind(&BJ::maintainBase, &tpchBasicJoin)};

         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchBasicJoin.pointLookupsForBase(); }, elapsed_cbs_base, tput_cbs_base, tput_prefixes, "base");
      } else if (FLAGS_storage_structure == 1) {
         std::cout << "TPC-H with materialized views" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_view = {std::bind(&BJ::queryByView, &tpchBasicJoin)};
         std::vector<std::function<void()>> tput_cbs_view = {std::bind(&BJ::pointQueryByView, &tpchBasicJoin),
                                                             std::bind(&BJ::range_query_by_view, &tpchBasicJoin),
                                                             std::bind(&BJ::maintainView, &tpchBasicJoin)};

         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchBasicJoin.pointLookupsForView(); }, elapsed_cbs_view, tput_cbs_view, tput_prefixes, "view");
      } else if (FLAGS_storage_structure == 2) {
         std::cout << "TPC-H with merged indexes" << std::endl;
         std::vector<std::function<void()>> elapsed_cbs_merged = {std::bind(&BJ::queryByMerged, &tpchBasicJoin)};
         std::vector<std::function<void()>> tput_cbs_merged = {std::bind(&BJ::pointQueryByMerged, &tpchBasicJoin),
                                                               std::bind(&BJ::range_query_by_merged, &tpchBasicJoin),
                                                               std::bind(&BJ::maintainMerged, &tpchBasicJoin)};

         WARMUP_THEN_TXS(
             tpch, crm, isolation_level, [&]() { tpchBasicJoin.pointLookupsForMerged(); }, elapsed_cbs_merged, tput_cbs_merged, tput_prefixes,
             "merged");
      } else {
         std::cerr << "Invalid storage structure: " << FLAGS_storage_structure << std::endl;
         return -1;
      }
   }
}