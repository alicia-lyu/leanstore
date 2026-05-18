#pragma once

#include <atomic>
#include <chrono>
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

template <typename PerStructureWrapper, typename AggRow,
          template <typename> class AdapterType,
          class LineitemRecord = lineitem_t>
struct TpchExecutableHelper {
   std::unique_ptr<DBTraits> db_traits;
   PerStructureWrapper wrapper;
   TPCHWorkload<AdapterType, LineitemRecord>& tpch;
   std::string structure_name;
   long last_count = 0;  // TX count from the most recent tput_tx() call

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

      // --bg_query_thread: spawn a single read-only background worker that
      // re-runs the foreground query back-to-back on BG_WORKER for the
      // duration of the foreground TX window. The thread reads whatever
      // Params the foreground has most recently set; it does not call
      // set_params_for_iter() itself, both to avoid the params-race and
      // because the foreground is the source of truth for the param
      // rotation. This is a contention test, not a parity check — the bg
      // thread's results are discarded.
      std::thread bg_thread;
      if (FLAGS_bg_query_thread) {
         bg_thread = std::thread([&]() {
            bg_running = true;
            std::vector<AggRow> bg_out;
            bg_out.reserve(8);
            while (keep_running.load()) {
               bg_out.clear();
               jumpmuTry()
               {
                  db_traits->run_tx([&]() { wrapper.query(bg_out); }, BG_WORKER);
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
