#pragma once

#include <gflags/gflags.h>
#include <chrono>
#include <cmath>
#include "../shared/LeanStoreAdapter.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "tpch_workload.hpp"

DECLARE_int32(storage_structure);
DECLARE_int32(warmup_seconds);
DECLARE_int32(tx_seconds);

#define WARMUP_THEN_TXS(tpch, crm, isolation_level, lookup_cb, elapsed_cbs, tput_cbs, tput_prefixes, suffix, size_cb)                            \
   {                                                                                                                                             \
      atomic<u64> keep_running = true;                                                                                                           \
      atomic<u64> lookup_count = 0;                                                                                                              \
      atomic<u64> running_threads_counter = 0;                                                                                                   \
      double size = size_cb();                                                                                                                   \
      std::cout << std::endl << std::string(20, '=') << suffix << "," << size << std::string(20, '=') << std::endl;                                                      \
      crm.scheduleJobAsync(0, [&]() { runLookupPhase(lookup_cb, lookup_count, running_threads_counter, keep_running, tpch, isolation_level); }); \
      sleep(FLAGS_warmup_seconds);                                                                                                               \
      crm.scheduleJobSync(                                                                                                                       \
          1, [&]() { runTXPhase(elapsed_cbs, tput_cbs, tput_prefixes, running_threads_counter, isolation_level, tpch, suffix, size); });         \
      keep_running = false;                                                                                                                      \
      while (running_threads_counter) {                                                                                                          \
      }                                                                                                                                          \
      crm.joinAll();                                                                                                                             \
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

inline void run_tput(std::function<void()> cb,
                     leanstore::TX_ISOLATION_LEVEL isolation_level,
                     TPCHWorkload<LeanStoreAdapter>& tpch,
                     std::string tx,
                     std::string method,
                     double size)
{
   auto start = std::chrono::high_resolution_clock::now();
   atomic<int> count = 0;
   std::cout << "Running " << tx << " on " << method << " for " << FLAGS_tx_seconds << " seconds..." << std::endl;
   atomic<u64> keep_running = true;
   std::thread([&] {
      std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tx_seconds));
      keep_running = false;
   }).detach();
   while (keep_running || count.load() < 10) {
      jumpmuTry()
      {
         cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
         // tpchBasicJoin.pointLookupsForBase();
         cb();
         count++;
         cr::Worker::my().commitTX();
      }
      jumpmuCatchNoPrint()
      {
         std::cerr << "#" << count.load() << " " << tx << "for " << method << " failed." << std::endl;
      }
      if (count.load() % 100 == 1)
         std::cout << "\r#" << count.load() << " " << tx << " for " << method << " performed.";
   }
   std::cout << std::endl;
   auto end = std::chrono::high_resolution_clock::now();
   auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
   double tput = (double)count.load() / duration * 1e6;
   tpch.logger.log(static_cast<long>(std::round(tput)), tx, method, size, count.load());
}

inline void run_elapsed(std::function<void()> cb, leanstore::TX_ISOLATION_LEVEL isolation_level)
{
   cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
   cb();
   cr::Worker::my().commitTX();
}

inline void runTXPhase(std::vector<std::function<void()>> elapsed_cbs,
                       std::vector<std::function<void()>> tput_cbs,
                       std::vector<std::string> txs,
                       atomic<u64>& running_threads_counter,
                       leanstore::TX_ISOLATION_LEVEL isolation_level,
                       TPCHWorkload<LeanStoreAdapter>& tpch,
                       std::string method,
                       double size)
{
   running_threads_counter++;

   std::cout << method << "," << size << std::endl;

   for (auto& cb : elapsed_cbs) {
      run_elapsed(cb, isolation_level);
   }

   assert(tput_cbs.size() == txs.size());

   for (size_t i = 0; i < tput_cbs.size(); i++) {
      run_tput(tput_cbs[i], isolation_level, tpch, txs[i], method, size);
   }

   cr::Worker::my().shutdown();
   running_threads_counter--;
}