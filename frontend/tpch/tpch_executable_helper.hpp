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
   // the cohort thread (BG_WORKER) cycles through these in round-robin
   // instead of re-running the foreground query. Set via
   // set_bg_query_steps() before run().
   std::vector<BgStepFn> bg_query_steps;
   void set_bg_query_steps(std::vector<BgStepFn> steps) { bg_query_steps = std::move(steps); }

   // Optional override for the point-lookup step run on BG_LOOKUP_WORKER.
   // If unset, the helper's built-in bg_point_lookup() runs (8 vanilla TPC-H
   // tables). TPCHi binaries set a 9-table variant including invoice via
   // make_tpchi_point_lookup_step() to keep the contention surface honest.
   BgStepFn bg_lookup_step;
   void set_bg_lookup_step(BgStepFn step) { bg_lookup_step = std::move(step); }

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

   // Default point-lookup step run on BG_LOOKUP_WORKER when no override is
   // set via set_bg_lookup_step(). Heterogeneous over 8 vanilla TPC-H base
   // tables (TPCHi binaries override to add invoice via
   // make_tpchi_point_lookup_step). Tolerates not-found via tryLookup
   // (PK ranges are sparse).
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
      }, BG_LOOKUP_WORKER);
   }

   void tput_tx(const std::string& tx_name)
   {
      tpch.logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      std::atomic<int> count = 0;
      std::atomic<long> bg_cohort_count = 0;
      std::atomic<long> bg_lookup_count = 0;

      std::cout << "Running " << tx_name << " on " << structure_name
                << " for " << FLAGS_tx_seconds << " seconds..." << std::endl;
      if (FLAGS_bg_query_thread) {
         std::cout << "  (--bg_query_thread=true: cohort thread = "
                   << bg_query_steps.size() << " steps";
         if (bg_query_steps.empty()) {
            std::cout << " -> fallback: re-run foreground on BG_WORKER";
         }
         std::cout << "; lookup thread = "
                   << (FLAGS_bg_point_lookups ? "on (BG_LOOKUP_WORKER)" : "off")
                   << ")" << std::endl;
      }

      std::atomic<bool> keep_running = true;
      std::thread([&] {
         std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tx_seconds));
         keep_running = false;
      }).detach();

      // --bg_query_thread: spawn two dedicated read-only background workers
      // for the duration of the foreground TX window:
      //
      //   Thread A (cohort, BG_WORKER):
      //     - If bg_query_steps is non-empty (family-loader case): rotate
      //       cohort steps by smallest-cumulative-elapsed fairness. When
      //       step costs differ widely (e.g. Q3 vs Q5 at SF=380) the
      //       cheaper step still runs more often to keep wall-clock
      //       allocation balanced 1:1:...:1. No point-lookup interleaving
      //       — those run on Thread B.
      //     - Otherwise: re-run the foreground query back-to-back on
      //       BG_WORKER (single-binary fallback for q10/q10i/q12).
      //
      //   Thread B (lookup, BG_LOOKUP_WORKER):
      //     - Active when --bg_point_lookups=true.
      //     - Runs bg_lookup_step in a tight loop if set, else falls back to
      //       the built-in bg_point_lookup() (8 vanilla tables). TPCHi
      //       binaries override via set_bg_lookup_step() to include invoice.
      //     - Decoupled from Thread A so a heavy structure (S2/S4, btree S1)
      //       no longer un-rotates bg_txs to ~2: the lookup stream keeps
      //       firing while the cohort/foreground re-run takes its time.
      //
      // Both threads read whatever Params each query has most recently set;
      // bg never mutates params (avoids the params-race; foreground is the
      // source of truth). Results are discarded — this is a contention
      // test, not a parity check.
      std::thread bg_cohort_thread;
      std::thread bg_lookup_thread;
      if (FLAGS_bg_query_thread) {
         bg_cohort_thread = std::thread([&]() {
            std::vector<AggRow> bg_out;
            bg_out.reserve(8);
            const bool use_registry = !bg_query_steps.empty();
            std::vector<double> bg_step_elapsed(bg_query_steps.size(), 0.0);
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
                  } else {
                     bg_out.clear();
                     db_traits->run_tx([&]() { wrapper.query(bg_out); }, BG_WORKER);
                  }
                  bg_cohort_count++;
               }
               jumpmuCatchNoPrint()
               {
                  db_traits->rollback_tx(BG_WORKER);
               }
            }
            db_traits->cleanup_thread(BG_WORKER);
         });

         if (FLAGS_bg_point_lookups) {
            bg_lookup_thread = std::thread([&]() {
               while (keep_running.load()) {
                  jumpmuTry()
                  {
                     if (bg_lookup_step) {
                        bg_lookup_step();
                     } else {
                        bg_point_lookup();
                     }
                     bg_lookup_count++;
                  }
                  jumpmuCatchNoPrint()
                  {
                     db_traits->rollback_tx(BG_LOOKUP_WORKER);
                  }
               }
               db_traits->cleanup_thread(BG_LOOKUP_WORKER);
            });
         }
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

      if (bg_cohort_thread.joinable()) {
         bg_cohort_thread.join();
      }
      if (bg_lookup_thread.joinable()) {
         bg_lookup_thread.join();
      }

      std::cout << "#" << count.load() << " " << tx_name << " for "
                << structure_name << " performed." << std::endl;
      if (FLAGS_bg_query_thread) {
         std::cout << "  bg_cohort_thread (BG_WORKER): " << bg_cohort_count.load()
                   << " TXs"
                   << "  |  bg_lookup_thread (BG_LOOKUP_WORKER): "
                   << bg_lookup_count.load() << " TXs"
                   << "  |  total: "
                   << (bg_cohort_count.load() + bg_lookup_count.load())
                   << std::endl;
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
