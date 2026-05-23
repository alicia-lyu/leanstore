#pragma once
#include <gflags/gflags.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <random>
#include <thread>
#include "../shared/db_traits.hpp"
#include "../shared/logger/logger.hpp"

DECLARE_int32(storage_structure);
DECLARE_bool(geo_bg_thread);
DEFINE_bool(log_progress, true, "Log progress of the workload execution");
// Skip join-n / mixed-n / distinct-n in tput_tx. Those phases override the
// tx_seconds budget via keep_running_condition() — they run until a full
// nation-scan completes (~42 min/phase at SF=19 on mat_view, c2). Default
// true to keep the sweep wall-time bounded; flip off only when we explicitly
// want full nation-scan numbers.
DEFINE_bool(geo_skip_n_queries, true, "Skip join-n / mixed-n / distinct-n phases (they ignore tx_seconds)");

template <typename PerStructureWorkloadFull,
          template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct ExecutableHelper {
   std::unique_ptr<DBTraits> db_traits;
   std::unique_ptr<PerStructureWorkloadFull> workload;

   Logger& logger;

   std::atomic<bool> keep_running_bg_tx = true;
   std::atomic<bool> run_main_thread = false;
   std::atomic<u64> bg_tx_count = 0;
   std::atomic<u64> running_threads_counter = 0;

#ifndef ROCKSDB_ONLY
   ExecutableHelper(leanstore::cr::CRManager& crm, std::unique_ptr<PerStructureWorkloadFull> workload, Logger& logger)
       : db_traits(std::make_unique<LeanStoreTraits>(crm)), workload(std::move(workload)), logger(logger)
   {
   }
#endif

   ExecutableHelper(RocksDB& rocks_db, std::unique_ptr<PerStructureWorkloadFull> workload, Logger& logger)
       : db_traits(std::make_unique<RocksDBTraits>(rocks_db)), workload(std::move(workload)), logger(logger)
   {
   }

   void elapsed_tx(std::function<void()> cb, std::string tx)
   {
      while (!run_main_thread) {
      }
      running_threads_counter++;
      db_traits->run_tx_w_rollback(cb, tx, MAIN_WORKER);
      running_threads_counter--;
      run_main_thread = false;
   }

   void run()
   {
      std::cout << std::string(20, '=') << workload->get_name() << "," << workload->get_size() << std::string(20, '=') << std::endl;
      logger.prepare();

      schedule_bg_txs();

      if (!FLAGS_geo_skip_n_queries) {
         tput_tx(std::bind(&PerStructureWorkloadFull::join_n, workload.get()), "join-n");
      }
      tput_tx(std::bind(&PerStructureWorkloadFull::join_ns, workload.get()), "join-ns");
      tput_tx(std::bind(&PerStructureWorkloadFull::join_nsc, workload.get()), "join-nsc");

      if (!FLAGS_geo_skip_n_queries) {
         tput_tx(std::bind(&PerStructureWorkloadFull::mixed_n, workload.get()), "mixed-n");
      }
      tput_tx(std::bind(&PerStructureWorkloadFull::mixed_ns, workload.get()), "mixed-ns");
      tput_tx(std::bind(&PerStructureWorkloadFull::mixed_nsc, workload.get()), "mixed-nsc");

      if (!FLAGS_geo_skip_n_queries) {
         tput_tx(std::bind(&PerStructureWorkloadFull::distinct_n, workload.get()), "distinct-n");
      }
      tput_tx(std::bind(&PerStructureWorkloadFull::distinct_ns, workload.get()), "distinct-ns");
      tput_tx(std::bind(&PerStructureWorkloadFull::distinct_nsc, workload.get()), "distinct-nsc");

      keep_running_bg_tx = false;
      // wait for background thread to finish
      while (running_threads_counter > 0) {
         std::this_thread::sleep_for(std::chrono::milliseconds(100));  // sleep 0.1 sec
      }

      tput_tx(std::bind(&PerStructureWorkloadFull::insert1, workload.get()), "maintain");

      std::cout << "All threads finished. Cleaning up inserted data..." << std::endl;

      db_traits->run_tx_w_rollback(std::bind(&PerStructureWorkloadFull::cleanup_updates, workload.get()), "cleanup");
      db_traits->cleanup_thread(MAIN_WORKER);
   }

   void schedule_bg_txs()
   {
      // Microbenchmark contract: --geo_bg_thread=false runs no background
      // thread at all (true isolated microbenchmark). When true, a single
      // background thread issues read-only hierarchical point-lookup TXs:
      // for a randomly-picked already-loaded customer, look up that customer
      // plus its city, county, state, and nation — 5 lookups per TX, all
      // inside one TX (mirrors maintain_view's lookup pattern). No writes
      // touch any geo table.
      //
      // Prime select_to_insert() once unconditionally so the post-sweep
      // "maintain" foreground TX has cities to draw from regardless of bg
      // mode (the bg thread no longer maintains city_reservoir state).
      workload->select_to_insert();
      if (!FLAGS_geo_bg_thread) {
         run_main_thread = true;
         return;
      }
      std::thread([this]() {
         running_threads_counter++;
         // One-time reservoir sample of valid customer keys. Cost: one full
         // scan of customer2 (base/view/hash) or the merged tree (merged).
         // At SF=206 LSM that's ~6M rows, ~seconds on cached pages.
         auto sample = workload->sample_customer_keys(10000);
         if (sample.empty()) {
            std::cerr << "[bg] sample_customer_keys returned 0 keys — bg thread will idle" << std::endl;
            run_main_thread = true;
            db_traits->cleanup_thread(BG_WORKER);
            running_threads_counter--;
            return;
         }
         std::mt19937 rng(std::random_device{}());
         std::uniform_int_distribution<size_t> pick(0, sample.size() - 1);

         // Open the gate now — foreground phases can run while bg loops.
         // No per-phase reset is needed since bg performs no writes.
         run_main_thread = true;

         while (keep_running_bg_tx) {
            const auto& ck = sample[pick(rng)];
            jumpmuTry()
            {
               db_traits->run_tx([&]() { workload->point_lookup_hierarchy(ck); }, BG_WORKER);
               bg_tx_count++;
            }
            jumpmuCatchNoPrint()
            {
               db_traits->rollback_tx(BG_WORKER);
               std::cerr << "#" << bg_tx_count.load() << " bg lookup tx failed." << std::endl;
            }
            if (bg_tx_count.load() % 1000 == 1 && running_threads_counter == 1 && FLAGS_log_progress)
               std::cout << "\r#" << bg_tx_count.load() << " bg lookup tx performed.";
         }

         std::cout << "#" << bg_tx_count.load() << " bg lookup tx in total performed." << std::endl;
         db_traits->cleanup_thread(BG_WORKER);
         running_threads_counter--;
      }).detach();
      sleep(FLAGS_warmup_seconds);  // warmup phase; then background phase
   }

   bool keep_running_condition(bool keep_running, std::string tx)
   {
      bool ret = keep_running;
      if (tx == "maintain") {
         ret = ret && !workload->insertion_complete();
      } else if (tx == "join-n" || tx == "mixed-n") {
         return !workload->n_scan_finished();  // overriding
      }
      return ret;
   }

   void tput_tx(std::function<void()> cb, std::string tx)
   {
      while (!run_main_thread) {
      }
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      atomic<int> count = 0;

      std::cout << "Running " << tx << " on " << workload->get_name() << " for " << FLAGS_tx_seconds << " seconds..." << std::endl;

      atomic<bool> keep_running_tx = true;
      std::thread([&] {
         std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tx_seconds));
         std::cout << "ExecutableHelper::tput_tx keeping_running = false";
         keep_running_tx = false;
      }).detach();

      running_threads_counter++;

      while (keep_running_condition(keep_running_tx.load(), tx)) {
         jumpmuTry()
         {
            db_traits->run_tx(cb);
            count++;
         }
         jumpmuCatchNoPrint()
         {
            db_traits->rollback_tx(MAIN_WORKER);
            std::cerr << "#" << count.load() << " " << tx << "for " << workload->get_name() << " failed." << std::endl;
         }
         if (count.load() % 1000 == 1 && (FLAGS_log_progress || !keep_running_tx.load())) {
            std::cout << "\r#" << count.load() << " " << tx << " for " << workload->get_name() << " performed.";
         }
      }

      std::cout << "#" << count.load() << " " << tx << " for " << workload->get_name() << " performed." << std::endl;

      auto end = std::chrono::high_resolution_clock::now();
      long duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      double tput = (double)count.load() / duration * 1e6;
      logger.log(tput, count.load(), tx, workload->get_name(), workload->get_size());
      running_threads_counter--;
      // Read-only bg means no per-phase reset is needed. The gate is opened
      // once at startup (in schedule_bg_txs after the sample completes) and
      // stays open across all foreground phases. The bg thread runs its
      // lookup loop in parallel with each phase; no flip-back required.
   }
};
