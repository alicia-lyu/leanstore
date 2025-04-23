#pragma once

#include <gflags/gflags.h>
#include <chrono>
#include <cmath>
#include "../shared/LeanStoreAdapter.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "tpch_workload.hpp"

#define WARMUP_THEN_TXS(tpchQuery, tpch, crm, isolation_level, lookupFunc, queryFunc, pointQueryFunc, maintainFunc)                     \
   {                                                                                                                                    \
      atomic<u64> keep_running = true;                                                                                                  \
      atomic<u64> lookup_count = 0;                                                                                                     \
      atomic<u64> running_threads_counter = 0;                                                                                          \
      crm.scheduleJobAsync(0, [&]() {                                                                                                   \
         runLookupPhase([&]() { tpchQuery.lookupFunc(); }, lookup_count, running_threads_counter, keep_running, tpch, isolation_level); \
      });                                                                                                                               \
      sleep(10);                                                                                                                        \
      crm.scheduleJobSync(1, [&]() {                                                                                                    \
         runTXPhase([&]() { tpchQuery.queryFunc(); }, [&]() { tpchQuery.pointQueryFunc(); }, [&]() { tpchQuery.maintainFunc(); },       \
                    [&]() { tpchQuery.refresh_rand_keys(); }, running_threads_counter, isolation_level, tpch);                          \
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
      jumpmuCatchNoPrint()
      {
         std::cerr << "#" << lookup_count.load() << " point lookups failed." << std::endl;
      }
      if (lookup_count.load() % 100 == 1 && running_threads_counter == 1)
         std::cout << "\r#" << lookup_count.load() << " warm-up point lookups performed.";
   }
   std::cout << std::endl;
   cr::Worker::my().shutdown();
   running_threads_counter--;
}

inline void runTXPhase(std::function<void()> query_cb,
                       std::function<void()> point_query_cb,
                       std::function<void()> maintain_cb,
                       std::function<void()> refresh_rand_keys_cb,
                       atomic<u64>& running_threads_counter,
                       leanstore::TX_ISOLATION_LEVEL isolation_level,
                       TPCHWorkload<LeanStoreAdapter>& tpch)
{
   running_threads_counter++;
   cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
   query_cb();
   cr::Worker::my().commitTX();

   auto pq_start = std::chrono::high_resolution_clock::now();
   atomic<u64> point_query_count = 0;
   while (point_query_count < 100) {
      jumpmuTry()
      {
         cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
         // tpchBasicJoin.pointLookupsForBase();
         refresh_rand_keys_cb();
         point_query_cb();
         point_query_count++;
         cr::Worker::my().commitTX();
      }
      jumpmuCatchNoPrint()
      {
         std::cerr << "#" << point_query_count.load() << " point query failed." << std::endl;
      }
      std::cout << "\r#" << point_query_count.load() << " point queries performed.";
   }
   std::cout << std::endl;
   auto pq_end = std::chrono::high_resolution_clock::now();
   auto pq_duration = std::chrono::duration_cast<std::chrono::microseconds>(pq_end - pq_start).count();
   double point_query_tput = (double)point_query_count.load() / pq_duration * 1e6;
   tpch.logger.log(static_cast<long>(point_query_tput), ColumnName::TPUT, "point_query");

   auto maintain_start = std::chrono::high_resolution_clock::now();
   atomic<u64> maintain_count = 0;
   while (maintain_count < 100) {
      jumpmuTry()
      {
         cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
         // tpchBasicJoin.pointLookupsForBase();
         refresh_rand_keys_cb();
         maintain_cb();
         maintain_count++;
         cr::Worker::my().commitTX();
      }
      jumpmuCatchNoPrint()
      {
         std::cerr << "#" << maintain_count.load() << " maintenance TX failed." << std::endl;
      }
      std::cout << "\r#" << maintain_count.load() << " maintenance TXs performed.";
   }
   std::cout << std::endl;
   auto maintain_end = std::chrono::high_resolution_clock::now();
   auto maintain_duration = std::chrono::duration_cast<std::chrono::microseconds>(maintain_end - maintain_start).count();
   double maint_tput = (double)maintain_count.load() / maintain_duration * 1e6;
   tpch.logger.log(static_cast<long>(maint_tput), ColumnName::TPUT, "maintain");

   cr::Worker::my().shutdown();
   running_threads_counter--;
}