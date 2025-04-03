#include <gflags/gflags.h>
#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/LeanStoreMergedAdapter.hpp"
#include "BasicJoin.hpp"
#include "LeanStoreLogger.hpp"
#include "TPCHWorkload.hpp"
#include "Tables.hpp"
#include "Views.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"

using namespace leanstore;

DEFINE_double(tpch_scale_factor, 1, "TPC-H scale factor");

#define warmupAndTX(tpchBasicJoin, tpch, crm, isolation_level, lookupFunc, queryFunc, maintainFunc)                                         \
   {                                                                                                                                        \
      atomic<u64> keep_running = true;                                                                                                      \
      atomic<u64> lookup_count = 0;                                                                                                         \
      atomic<u64> running_threads_counter = 0;                                                                                              \
      crm.scheduleJobAsync(0, [&]() {                                                                                                       \
         runLookupPhase([&]() { tpchBasicJoin.lookupFunc(); }, lookup_count, running_threads_counter, keep_running, tpch, isolation_level); \
      });                                                                                                                                   \
      sleep(10);                                                                                                                            \
      crm.scheduleJobSync(1, [&]() {                                                                                                        \
         runTXPhase(                                                                                                                        \
             [&]() {                                                                                                                        \
                tpchBasicJoin.queryFunc();                                                                                                  \
                tpchBasicJoin.maintainFunc();                                                                                               \
             },                                                                                                                             \
             running_threads_counter, isolation_level);                                                                                     \
      });                                                                                                                                   \
      keep_running = false;                                                                                                                 \
      while (running_threads_counter) {                                                                                                     \
      }                                                                                                                                     \
      crm.joinAll();                                                                                                                        \
   }

void runLookupPhase(std::function<void()> lookupCallback,
                    atomic<u64>& lookup_count,
                    atomic<u64>& running_threads_counter,
                    atomic<u64>& keep_running,
                    TPCHWorkload<LeanStoreAdapter, LeanStoreMergedAdapter>& tpch,
                    leanstore::TX_ISOLATION_LEVEL isolation_level)
{
   running_threads_counter++;
   tpch.prepare();
   while (keep_running) {
      jumpmuTry()
      {
         cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
         // tpchBasicJoin.pointLookupsForBase();
         lookupCallback();
         lookup_count++;
         cr::Worker::my().commitTX();
      }
      jumpmuCatch()
      {
         std::cerr << "#" << lookup_count.load() << " pointLookupsForBase failed." << std::endl;
      }
      if (lookup_count.load() % 100 == 1 && running_threads_counter == 1)
         std::cout << "\r#" << lookup_count.load() << " warm-up pointLookupsForBase performed.";
   }
   cr::Worker::my().shutdown();
   running_threads_counter--;
}

void runTXPhase(std::function<void()> TXCallback, atomic<u64>& running_threads_counter, leanstore::TX_ISOLATION_LEVEL isolation_level)
{
   running_threads_counter++;
   cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
   std::cout << std::endl;
   for (int i = 0; i < 2; ++i) {
      // tpchBasicJoin.queryByBase();
      // tpchBasicJoin.maintainBase();
      TXCallback();
   }
   cr::Worker::my().commitTX();
   cr::Worker::my().shutdown();
   running_threads_counter--;
}

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
   LeanStoreAdapter<sorted_lineitem_t> sortedLineitem;
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
      sortedLineitem = LeanStoreAdapter<sorted_lineitem_t>(db, "sortedLineitem");
   });

   db.registerConfigEntry("tpch_scale_factor", FLAGS_tpch_scale_factor);
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;
   // -------------------------------------------------------------------------------------
   LeanStoreLogger logger(db);
   TPCHWorkload<LeanStoreAdapter, LeanStoreMergedAdapter> tpch(part, supplier, partsupp, customer, orders, lineitem, nation, region, logger);
   BasicJoin<LeanStoreAdapter, LeanStoreMergedAdapter> tpchBasicJoin(tpch, mergedBasicJoin, joinedPPsL, joinedPPs, sortedLineitem);

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

   warmupAndTX(tpchBasicJoin, tpch, crm, isolation_level, pointLookupsForBase, queryByBase, maintainBase);

   warmupAndTX(tpchBasicJoin, tpch, crm, isolation_level, pointLookupsForMerged, queryByMerged, maintainMerged);

   warmupAndTX(tpchBasicJoin, tpch, crm, isolation_level, pointLookupsForView, queryByView, maintainView);
}