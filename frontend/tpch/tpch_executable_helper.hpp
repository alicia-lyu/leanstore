#pragma once

#include <atomic>
#include <chrono>
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
          template <typename> class AdapterType>
struct TpchExecutableHelper {
   std::unique_ptr<DBTraits> db_traits;
   PerStructureWrapper wrapper;
   TPCHWorkload<AdapterType>& tpch;
   std::string structure_name;

   TpchExecutableHelper(RocksDB& rocks_db, PerStructureWrapper wrapper,
                        TPCHWorkload<AdapterType>& tpch, std::string name)
       : db_traits(std::make_unique<RocksDBTraits>(rocks_db)),
         wrapper(std::move(wrapper)),
         tpch(tpch),
         structure_name(std::move(name))
   {
   }

#ifndef ROCKSDB_ONLY
   TpchExecutableHelper(leanstore::cr::CRManager& crm, PerStructureWrapper wrapper,
                        TPCHWorkload<AdapterType>& tpch, std::string name)
       : db_traits(std::make_unique<LeanStoreTraits>(crm)),
         wrapper(std::move(wrapper)),
         tpch(tpch),
         structure_name(std::move(name))
   {
   }
#endif

   void run()
   {
      std::cout << std::string(20, '=') << structure_name << ","
                << wrapper.get_size() << std::string(20, '=') << std::endl;
      tpch.prepare();
      tput_tx("query");
   }

   void tput_tx(const std::string& tx_name)
   {
      tpch.logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      std::atomic<int> count = 0;

      std::cout << "Running " << tx_name << " on " << structure_name
                << " for " << FLAGS_tx_seconds << " seconds..." << std::endl;

      std::atomic<bool> keep_running = true;
      std::thread([&] {
         std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tx_seconds));
         keep_running = false;
      }).detach();

      std::vector<AggRow> out;
      out.reserve(8);

      while (keep_running.load()) {
         out.clear();
         jumpmuTry()
         {
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

      std::cout << "#" << count.load() << " " << tx_name << " for "
                << structure_name << " performed." << std::endl;

      auto end = std::chrono::high_resolution_clock::now();
      long duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      double tput = static_cast<double>(count.load()) / duration * 1e6;
      tpch.logger.log(tput, count.load(), tx_name, structure_name, wrapper.get_size());
   }
};

}  // namespace tpch
