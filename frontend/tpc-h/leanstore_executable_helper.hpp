#pragma once

#include <gflags/gflags.h>
#include <chrono>
#include <cmath>
#include "../shared/LeanStoreAdapter.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "tpch_workload.hpp"

DECLARE_int32(tx_count);

#define WARMUP_THEN_TXS(tpch, crm, isolation_level, lookup_cb, elapsed_cbs, tput_cbs, tput_prefixes, suffix)                                       \
   {                                                                                                                                               \
      atomic<u64> keep_running = true;                                                                                                             \
      atomic<u64> lookup_count = 0;                                                                                                                \
      atomic<u64> running_threads_counter = 0;                                                                                                     \
      crm.scheduleJobAsync(0, [&]() { runLookupPhase(lookup_cb, lookup_count, running_threads_counter, keep_running, tpch, isolation_level); });   \
      sleep(10);                                                                                                                                   \
      crm.scheduleJobSync(1, [&]() { runTXPhase(elapsed_cbs, tput_cbs, tput_prefixes, running_threads_counter, isolation_level, tpch, suffix); }); \
      keep_running = false;                                                                                                                        \
      while (running_threads_counter) {                                                                                                            \
      }                                                                                                                                            \
      crm.joinAll();                                                                                                                               \
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

inline void run_tput(std::function<void()> cb, leanstore::TX_ISOLATION_LEVEL isolation_level, TPCHWorkload<LeanStoreAdapter>& tpch, std::string msg)
{
   auto pq_start = std::chrono::high_resolution_clock::now();
   atomic<int> point_query_count = 0;
   while (point_query_count < FLAGS_tx_count) {
      jumpmuTry()
      {
         cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
         // tpchBasicJoin.pointLookupsForBase();
         cb();
         point_query_count++;
         cr::Worker::my().commitTX();
      }
      jumpmuCatchNoPrint()
      {
         std::cerr << "#" << point_query_count.load() << " " << msg << "  failed." << std::endl;
      }
      std::cout << "\r#" << point_query_count.load() << " " << msg << " performed.";
   }
   std::cout << std::endl;
   auto pq_end = std::chrono::high_resolution_clock::now();
   auto pq_duration = std::chrono::duration_cast<std::chrono::microseconds>(pq_end - pq_start).count();
   double point_query_tput = (double)point_query_count.load() / pq_duration * 1e6;
   tpch.logger.log(static_cast<long>(point_query_tput), ColumnName::TPUT, msg);
}

inline void run_elapsed(std::function<void()> cb, leanstore::TX_ISOLATION_LEVEL isolation_level)
{
   cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
   cb();
   cr::Worker::my().commitTX();
}

inline void runTXPhase(std::vector<std::function<void()>> elapsed_cbs,
                       std::vector<std::function<void()>> tput_cbs,
                       std::vector<std::string> tput_prefixes,
                       atomic<u64>& running_threads_counter,
                       leanstore::TX_ISOLATION_LEVEL isolation_level,
                       TPCHWorkload<LeanStoreAdapter>& tpch,
                       std::string suffix)
{
   running_threads_counter++;

   for (auto& cb : elapsed_cbs) {
      run_elapsed(cb, isolation_level);
   }

   assert(tput_cbs.size() == tput_prefixes.size());

   for (size_t i = 0; i < tput_cbs.size(); i++) {
      run_tput(tput_cbs[i], isolation_level, tpch, tput_prefixes[i] + "-" + suffix);
   }

   cr::Worker::my().shutdown();
   running_threads_counter--;
}