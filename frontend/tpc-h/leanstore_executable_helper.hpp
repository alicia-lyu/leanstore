#pragma once

#include <gflags/gflags.h>
#include <cmath>
#include "../shared/LeanStoreAdapter.hpp"
#include "tpch_workload.hpp"

#define WARMUP_THEN_TXS(tpchQuery, tpch, crm, isolation_level, lookupFunc, queryFunc, maintainFunc)                                     \
   {                                                                                                                                    \
      atomic<u64> keep_running = true;                                                                                                  \
      atomic<u64> lookup_count = 0;                                                                                                     \
      atomic<u64> running_threads_counter = 0;                                                                                          \
      crm.scheduleJobAsync(0, [&]() {                                                                                                   \
         runLookupPhase([&]() { tpchQuery.lookupFunc(); }, lookup_count, running_threads_counter, keep_running, tpch, isolation_level); \
      });                                                                                                                               \
      sleep(pow(FLAGS_tpch_scale_factor * 100 / FLAGS_dram_gib,2));                                                                            \
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
         std::cerr << "#" << lookup_count.load() << " pointLookups failed." << std::endl;
      }
      if (lookup_count.load() % 100 == 1 && running_threads_counter == 1)
         std::cout << "\r#" << lookup_count.load() << " warm-up pointLookups performed.";
   }
   cr::Worker::my().shutdown();
   running_threads_counter--;
}

inline void runTXPhase(std::function<void()> TXCallback, atomic<u64>& running_threads_counter, leanstore::TX_ISOLATION_LEVEL isolation_level)
{
   running_threads_counter++;
   cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
   std::cout << std::endl;
   TXCallback();
   cr::Worker::my().commitTX();
   cr::Worker::my().shutdown();
   running_threads_counter--;
}