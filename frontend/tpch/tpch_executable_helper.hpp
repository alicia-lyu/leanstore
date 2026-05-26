#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
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

   // One heterogeneous point-lookup over the base TPC-H tables, on BG_WORKER.
   // Used by the bg=2 fallback path (binaries that register no family cohort,
   // e.g. q10 / q10i) so their bg=2 includes the same uniform-random
   // point-lookup stream as the family cohort, not just same-query
   // contention. Mirrors register_vanilla_bg_steps' point-lookup step;
   // tolerates not-found via tryLookup (PK ranges are sparse).
   void bg_point_lookup()
   {
      const Integer pick = urand(0, 7);
      db_traits->run_tx([&]() {
         switch (pick) {
            case 0: { part_t::Key k{tpch.getPartID()}; tpch.part.tryLookup(k, [](const part_t&) {}); break; }
            case 1: { supplier_t::Key k{tpch.getSupplierID()}; tpch.supplier.tryLookup(k, [](const supplier_t&) {}); break; }
            case 2: { partsupp_t::Key k{tpch.getPartID(), tpch.getSupplierID()}; tpch.partsupp.tryLookup(k, [](const partsupp_t&) {}); break; }
            case 3: { customerh_t::Key k{tpch.getCustomerID()}; tpch.customer.tryLookup(k, [](const customerh_t&) {}); break; }
            case 4: { orders_t::Key k{tpch.getOrderID()}; tpch.orders.tryLookup(k, [](const orders_t&) {}); break; }
            case 5: {
               if constexpr (std::is_same_v<LineitemRecord, lineitem_t>) {
                  lineitem_t::Key k{tpch.getOrderID(), urand(1, 7)};
                  tpch.lineitem.tryLookup(k, [](const LineitemRecord&) {});
               } else {  // non-base lineitem variant: lookup orders instead
                  orders_t::Key k{tpch.getOrderID()};
                  tpch.orders.tryLookup(k, [](const orders_t&) {});
               }
               break;
            }
            case 6: { nation_t::Key k{tpch.getNationID()}; tpch.nation.tryLookup(k, [](const nation_t&) {}); break; }
            case 7: default: { region_t::Key k{tpch.getRegionID()}; tpch.region.tryLookup(k, [](const region_t&) {}); break; }
         }
      }, BG_WORKER);
   }

   void tput_tx(const std::string& tx_name)
   {
      tpch.logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      std::atomic<int> count = 0;
      std::atomic<long> bg_count = 0;

      std::cout << "Running " << tx_name << " on " << structure_name
                << " for " << FLAGS_tx_seconds << " seconds..." << std::endl;
      if (FLAGS_bg_query_thread) {
         std::cout << "  (--bg_query_thread=true: bg cohort: "
                   << bg_query_steps.size() << " steps";
         if (bg_query_steps.empty()) {
            std::cout << " -> fallback: re-run foreground query"
                      << (FLAGS_bg_point_lookups ? " + point-lookup stream" : "");
         }
         std::cout << ")" << std::endl;
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
      //     dispatches steps by time-balance fairness: always pick the step
      //     with the smallest cumulative elapsed seconds. When there is only
      //     one step, this collapses to "always pick index 0". When there
      //     are multiple steps with similar per-call cost (e.g. Q3 vs Q5 at
      //     SF=380) the visit ratio is ~1:1; when step costs differ widely
      //     (e.g. heavy family-query step vs cheap point-lookup step), the
      //     cheaper step runs more often to keep wall-clock allocation
      //     balanced 1:1:...:1.
      //   - Otherwise (single-binary fallback), the thread re-runs the
      //     foreground query back-to-back on BG_WORKER.
      std::thread bg_thread;
      if (FLAGS_bg_query_thread) {
         bg_thread = std::thread([&]() {
            bg_running = true;
            std::vector<AggRow> bg_out;
            bg_out.reserve(8);
            const bool use_registry = !bg_query_steps.empty();
            // Cumulative elapsed seconds per registered step. Pick the step
            // with the smallest cumulative time each iteration.
            std::vector<double> bg_step_elapsed(bg_query_steps.size(), 0.0);
            // Fallback time-balance accumulators (foreground-query vs the
            // point-lookup stream) for binaries with no registered cohort.
            double fb_query_elapsed = 0.0, fb_pl_elapsed = 0.0;
            while (keep_running.load()) {
               jumpmuTry()
               {
                  if (use_registry) {
                     // Smallest-cumulative-elapsed pick. With one step this
                     // is just index 0; with N steps it time-balances 1:...:1.
                     size_t pick = 0;
                     for (size_t i = 1; i < bg_step_elapsed.size(); ++i) {
                        if (bg_step_elapsed[i] < bg_step_elapsed[pick]) pick = i;
                     }
                     auto step_start = std::chrono::steady_clock::now();
                     bg_query_steps[pick]();
                     auto step_end = std::chrono::steady_clock::now();
                     bg_step_elapsed[pick] +=
                         std::chrono::duration<double>(step_end - step_start).count();
                  } else if (FLAGS_bg_point_lookups && fb_pl_elapsed <= fb_query_elapsed) {
                     // Fallback (no registered cohort, e.g. q10/q10i) with
                     // bg=2: time-balance a point-lookup stream 1:1 (wall
                     // clock) against the re-run foreground query, mirroring
                     // the registry's smallest-cumulative-elapsed pick. Makes
                     // bg=2 mean "second query worker + point-lookup noise"
                     // even without a family cohort.
                     auto t0 = std::chrono::steady_clock::now();
                     bg_point_lookup();
                     fb_pl_elapsed += std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - t0).count();
                  } else {
                     bg_out.clear();
                     auto t0 = std::chrono::steady_clock::now();
                     db_traits->run_tx([&]() { wrapper.query(bg_out); }, BG_WORKER);
                     fb_query_elapsed += std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - t0).count();
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
            // param_seed is an additive offset into the per-query
            // PARAM_TABLE rotation: the first iteration uses
            // PARAM_TABLE[(param_seed + 0) % N] instead of always [0].
            // The sweep passes a per-rep seed identical across all four
            // structures, so structures stay comparable while reps sample
            // distinct parameters. Default param_seed=0 ⇒ historical
            // behaviour (validation default).
            wrapper.set_params_for_iter(
                static_cast<long>(FLAGS_param_seed) + static_cast<long>(count.load()));
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
