#include "../shared/LeanStoreAdapter.hpp"
#include "TPCHWorkload.hpp"

#define warmupAndTX(tpchQuery, tpch, crm, isolation_level, lookupFunc, queryFunc, maintainFunc)                                         \
   {                                                                                                                                    \
      atomic<u64> keep_running = true;                                                                                                  \
      atomic<u64> lookup_count = 0;                                                                                                     \
      atomic<u64> running_threads_counter = 0;                                                                                          \
      crm.scheduleJobAsync(0, [&]() {                                                                                                   \
         runLookupPhase([&]() { tpchQuery.lookupFunc(); }, lookup_count, running_threads_counter, keep_running, tpch, isolation_level); \
      });                                                                                                                               \
      sleep(10);                                                                                                                        \
      crm.scheduleJobSync(1, [&]() {                                                                                                    \
         runTXPhase(                                                                                                                    \
             [&]() {                                                                                                                    \
                tpchQuery.queryFunc();                                                                                                  \
                tpchQuery.maintainFunc();                                                                                               \
             },                                                                                                                         \
             running_threads_counter, isolation_level);                                                                                 \
      });                                                                                                                               \
      keep_running = false;                                                                                                             \
      while (running_threads_counter) {                                                                                                 \
      }                                                                                                                                 \
      crm.joinAll();                                                                                                                    \
   }

inline void runLookupPhase(std::function<void()> lookupCallback,
                           atomic<u64>& lookup_count,
                           atomic<u64>& running_threads_counter,
                           atomic<u64>& keep_running,
                           TPCHWorkload<LeanStoreAdapter>& tpch,
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

inline void runTXPhase(std::function<void()> TXCallback, atomic<u64>& running_threads_counter, leanstore::TX_ISOLATION_LEVEL isolation_level)
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