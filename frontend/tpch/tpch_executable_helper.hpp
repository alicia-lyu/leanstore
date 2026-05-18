#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "../shared/db_traits.hpp"
#include "tpch_flags.hpp"
#include "tpch_workload.hpp"
#include "leanstore/utils/JumpMU.hpp"

namespace tpch
{

// A type-erased "one TX of a registered background query" callback. The
// closure must internally route the TX through DBTraits on BG_WORKER,
// manage its own out-vector, and swallow per-TX jumpmu rollbacks (see
// register_bg_query_step() helpers in family-loader headers for the
// canonical wrapping). Family loaders push one step per (query × foreground
// structure) so the bg thread can round-robin family queries at the same
// storage structure as the foreground.
using BgStepFn = std::function<void()>;

template <typename PerStructureWrapper, typename AggRow,
          template <typename> class AdapterType,
          class LineitemRecord = lineitem_t>
struct TpchExecutableHelper {
   std::unique_ptr<DBTraits> db_traits;
   PerStructureWrapper wrapper;
   TPCHWorkload<AdapterType, LineitemRecord>& tpch;
   std::string structure_name;
   long last_count = 0;  // TX count from the most recent tput_tx() call

   // Optional registry of background-query steps. When --bg_query_thread=true,
   // the bg thread cycles through these in round-robin instead of re-running
   // the foreground query. Set via set_bg_query_steps() before run().
   std::vector<BgStepFn> bg_query_steps;
   void set_bg_query_steps(std::vector<BgStepFn> steps) { bg_query_steps = std::move(steps); }

   TpchExecutableHelper(RocksDB& rocks_db, PerStructureWrapper wrapper,
                        TPCHWorkload<AdapterType, LineitemRecord>& tpch, std::string name)
       : db_traits(std::make_unique<RocksDBTraits>(rocks_db)),
         wrapper(std::move(wrapper)),
         tpch(tpch),
         structure_name(std::move(name))
   {
   }

#ifndef ROCKSDB_ONLY
   TpchExecutableHelper(leanstore::cr::CRManager& crm, PerStructureWrapper wrapper,
                        TPCHWorkload<AdapterType, LineitemRecord>& tpch, std::string name)
       : db_traits(std::make_unique<LeanStoreTraits>(crm)),
         wrapper(std::move(wrapper)),
         tpch(tpch),
         structure_name(std::move(name))
   {
   }
#endif

   void run()
   {
      std::cout << "\n-- " << structure_name << " "
                << std::string(std::max(1, 40 - (int)structure_name.size()), '-')
                << std::endl;
      tpch.prepare();
      // Snap a baseline before the query loop so post-load compactions
      // are excluded from per-TX SSTWrite figures.
      tpch.logger.capture_baseline();
      tput_tx("query");
   }

   // Returns the number of queries executed in the most recent run() call.
   // Used by per-query executables to compute per-query stage averages.
   long tx_count() const { return last_count; }

   void tput_tx(const std::string& tx_name)
   {
      tpch.logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      std::atomic<int> count = 0;
      std::atomic<long> bg_count = 0;

      std::cout << "Running " << tx_name << " on " << structure_name
                << " for " << FLAGS_tx_seconds << " seconds..." << std::endl;
      if (FLAGS_bg_query_thread) {
         std::cout << "  (--bg_query_thread=true: a background worker runs the same "
                   << "query concurrently for the full TX window)" << std::endl;
      }

      std::atomic<bool> keep_running = true;
      std::atomic<bool> bg_running = false;
      std::thread([&] {
         std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tx_seconds));
         keep_running = false;
      }).detach();

      // --bg_query_thread: spawn a single read-only background worker for
      // the duration of the foreground TX window. The thread reads whatever
      // Params each query has most recently set; bg never mutates params
      // (avoids the params-race; the foreground is the source of truth).
      // This is a contention test, not a parity check — bg results are
      // discarded.
      //
      // Routing:
      //   - If bg_query_steps is non-empty (family-loader case), the thread
      //     round-robins through the registered steps. Each step internally
      //     runs one TX on BG_WORKER and increments its own counter; we just
      //     bump the aggregate bg_count once per step call.
      //   - Otherwise (single-binary fallback), the thread re-runs the
      //     foreground query back-to-back on BG_WORKER.
      std::thread bg_thread;
      if (FLAGS_bg_query_thread) {
         bg_thread = std::thread([&]() {
            bg_running = true;
            std::vector<AggRow> bg_out;
            bg_out.reserve(8);
            size_t bg_iter = 0;
            const bool use_registry = !bg_query_steps.empty();
            while (keep_running.load()) {
               jumpmuTry()
               {
                  if (use_registry) {
                     bg_query_steps[bg_iter % bg_query_steps.size()]();
                     ++bg_iter;
                  } else {
                     bg_out.clear();
                     db_traits->run_tx([&]() { wrapper.query(bg_out); }, BG_WORKER);
                  }
                  bg_count++;
               }
               jumpmuCatchNoPrint()
               {
                  db_traits->rollback_tx(BG_WORKER);
                  // Quiet failure — the bg thread is just contention; we
                  // log the total at the end and move on.
               }
            }
            db_traits->cleanup_thread(BG_WORKER);
            bg_running = false;
         });
      }

      std::vector<AggRow> out;
      out.reserve(8);

      while (keep_running.load()) {
         out.clear();
         jumpmuTry()
         {
            wrapper.set_params_for_iter(static_cast<long>(count.load()));
            db_traits->run_tx([&]() { wrapper.query(out); });
            count++;
         }
         jumpmuCatchNoPrint()
         {
            db_traits->rollback_tx(MAIN_WORKER);
            std::cerr << "#" << count.load() << " " << tx_name
                      << " for " << structure_name << " failed." << std::endl;
         }
      }

      if (bg_thread.joinable()) {
         bg_thread.join();
      }

      std::cout << "#" << count.load() << " " << tx_name << " for "
                << structure_name << " performed." << std::endl;
      if (FLAGS_bg_query_thread) {
         std::cout << "  bg_query_thread: " << bg_count.load()
                   << " background TXs completed during the same window." << std::endl;
      }

      auto end = std::chrono::high_resolution_clock::now();
      long duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      double tput = static_cast<double>(count.load()) / duration * 1e6;
      last_count = count.load();
      tpch.logger.log(tput, count.load(), tx_name, structure_name, wrapper.get_size());

      double avg_latency_ms = (tput > 0) ? 1000.0 / tput : 0.0;
      double elapsed_s = duration / 1e6;
      std::cout << "\n  Summary: " << std::fixed << std::setprecision(2)
                << tput << " TX/s  |  " << avg_latency_ms << " ms/query  |  "
                << count.load() << " queries in " << std::setprecision(1)
                << elapsed_s << "s  |  " << std::setprecision(2)
                << wrapper.get_size() << " MiB\n" << std::endl;
   }
};

}  // namespace tpch
