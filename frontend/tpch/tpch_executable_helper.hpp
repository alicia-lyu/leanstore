#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>
#include <gflags/gflags_declare.h>
#include "../shared/db_traits.hpp"
#include "tpch_flags.hpp"
#include "tpch_workload.hpp"
#include "leanstore/utils/JumpMU.hpp"

DECLARE_uint32(worker_threads);

namespace tpch
{

// A type-erased "one TX of a registered background query" callback. The
// closure is parametric on worker_id: the helper allocates a dedicated
// CRM worker per cohort step and passes it in at call time, so each
// cohort query runs on its own thread without contending for a shared
// BG_WORKER slot. The closure must route the TX through DBTraits on the
// passed worker_id, manage its own out-vector, and swallow per-TX
// jumpmu rollbacks (see register_*_bg_steps in family-loader headers).
using BgCatalogFn = std::function<void(u64 worker_id)>;

// Worker-id allocation for the N-step parallel cohort dispatch.
// Layout (preserving existing constants — see ../shared/db_traits.hpp):
//   BG_WORKER (=0)        : cohort thread 0
//   MAIN_WORKER (=1)      : foreground
//   BG_LOOKUP_WORKER (=2) : point-lookup thread
//   3, 4, 5, ...          : additional cohort threads 1, 2, 3, ...
// So cohort thread i (0-indexed) uses worker_id BG_WORKER for i==0 and
// i+2 for i>=1. Max worker_id with N-step cohort is (N==1 ? BG_LOOKUP_WORKER : N+1).
inline u64 cohort_worker_id(size_t i)
{
   return (i == 0) ? BG_WORKER : static_cast<u64>(i + 2);
}
inline u64 max_cohort_worker_id(size_t cohort_size)
{
   if (cohort_size <= 1) return BG_LOOKUP_WORKER;  // 2
   return static_cast<u64>(cohort_size + 1);
}

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
   // set_bg_query_catalog() before run().
   std::vector<BgCatalogFn> bg_query_catalog;
   void set_bg_query_catalog(std::vector<BgCatalogFn> steps) { bg_query_catalog = std::move(steps); }

   // Optional override for the point-lookup step run on BG_LOOKUP_WORKER.
   // If unset, the helper's built-in bg_point_lookup() runs (8 vanilla TPC-H
   // tables). TPCHi binaries set a 9-table variant including invoice via
   // make_tpchi_point_lookup_step() to keep the contention surface honest.
   BgCatalogFn bg_lookup_step;
   void set_bg_lookup_step(BgCatalogFn step) { bg_lookup_step = std::move(step); }

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
         const size_t cohort_size = bg_query_catalog.size();
         // CRM worker budget: foreground (MAIN_WORKER) + lookup
         // (BG_LOOKUP_WORKER, optional) + N cohort threads.
         const u64 max_id = std::max<u64>(
             MAIN_WORKER,
             FLAGS_bg_point_lookups ? BG_LOOKUP_WORKER : 0);
         const u64 needed_max_id = cohort_size == 0
             ? max_id
             : std::max<u64>(max_id, max_cohort_worker_id(cohort_size));
         const u32 needed_workers = static_cast<u32>(needed_max_id + 1);
         if (FLAGS_worker_threads < needed_workers) {
            throw std::runtime_error(
                "bg=2 needs --worker_threads >= " + std::to_string(needed_workers)
                + " (cohort_size=" + std::to_string(cohort_size)
                + ", lookup=" + (FLAGS_bg_point_lookups ? "on" : "off")
                + "); got " + std::to_string(FLAGS_worker_threads));
         }
         std::cout << "  (--bg_query_thread=true: cohort = " << cohort_size
                   << " parallel threads";
         if (cohort_size == 0) {
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

      // --bg_query_thread: spawn N+1 dedicated read-only background workers
      // for the duration of the foreground TX window — N parallel cohort
      // threads (one per query in bg_query_catalog) plus one point-lookup
      // thread. No time-balance fairness needed: each cohort query has its
      // own CRM worker, so a heavy structure (S2/S4, btree S1) only slows
      // its own thread without un-rotating the others to ~2.
      //
      //   Cohort threads (i=0..N-1): worker_id = cohort_worker_id(i)
      //     - Each runs bg_query_catalog[i] in a tight loop.
      //     - i=0 uses BG_WORKER (=0); i>=1 uses i+2 to avoid collision
      //       with MAIN_WORKER (=1) and BG_LOOKUP_WORKER (=2).
      //     - If bg_query_catalog is empty: fall back to single re-run-fg
      //       thread on BG_WORKER (q10/q10i/q12 with no family cohort).
      //
      //   Lookup thread (BG_LOOKUP_WORKER, optional):
      //     - Active when --bg_point_lookups=true.
      //     - Runs bg_lookup_step closure if set (TPCHi binaries override
      //       via set_bg_lookup_step() to include invoice), else falls
      //       back to the helper's 8-table vanilla bg_point_lookup().
      //
      // All threads read whatever Params each query has most recently set;
      // bg never mutates params (avoids the params-race; foreground is the
      // source of truth). Results are discarded — this is a contention
      // test, not a parity check.
      std::vector<std::thread> bg_cohort_threads;
      std::thread bg_lookup_thread;
      if (FLAGS_bg_query_thread) {
         const size_t cohort_size = bg_query_catalog.size();
         if (cohort_size == 0) {
            // No registered cohort: single thread re-runs foreground on BG_WORKER.
            bg_cohort_threads.emplace_back([&]() {
               std::vector<AggRow> bg_out;
               bg_out.reserve(8);
               while (keep_running.load()) {
                  jumpmuTry()
                  {
                     bg_out.clear();
                     db_traits->run_tx([&]() { wrapper.query(bg_out); }, BG_WORKER);
                     bg_cohort_count++;
                  }
                  jumpmuCatchNoPrint()
                  {
                     db_traits->rollback_tx(BG_WORKER);
                  }
               }
               db_traits->cleanup_thread(BG_WORKER);
            });
         } else {
            // N parallel cohort threads.
            bg_cohort_threads.reserve(cohort_size);
            for (size_t i = 0; i < cohort_size; ++i) {
               const u64 wid = cohort_worker_id(i);
               bg_cohort_threads.emplace_back([&, i, wid]() {
                  while (keep_running.load()) {
                     jumpmuTry()
                     {
                        bg_query_catalog[i](wid);
                        bg_cohort_count++;
                     }
                     jumpmuCatchNoPrint()
                     {
                        db_traits->rollback_tx(wid);
                     }
                  }
                  db_traits->cleanup_thread(wid);
               });
            }
         }

         if (FLAGS_bg_point_lookups) {
            bg_lookup_thread = std::thread([&]() {
               while (keep_running.load()) {
                  jumpmuTry()
                  {
                     if (bg_lookup_step) {
                        bg_lookup_step(BG_LOOKUP_WORKER);
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

      for (auto& t : bg_cohort_threads) {
         if (t.joinable()) t.join();
      }
      if (bg_lookup_thread.joinable()) {
         bg_lookup_thread.join();
      }

      std::cout << "#" << count.load() << " " << tx_name << " for "
                << structure_name << " performed." << std::endl;
      if (FLAGS_bg_query_thread) {
         std::cout << "  bg_cohort (" << bg_cohort_threads.size()
                   << " threads): " << bg_cohort_count.load() << " TXs"
                   << "  |  bg_lookup (BG_LOOKUP_WORKER): "
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
