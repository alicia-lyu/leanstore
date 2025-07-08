#pragma once
#include <gflags/gflags.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <thread>
#include "../shared/RocksDB.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "../tpc-h/workload.hpp"

DECLARE_int32(storage_structure);
DECLARE_int32(warmup_seconds);
DECLARE_int32(tx_seconds);

struct DBTraits {
   virtual void run_tx(std::function<void()> cb) = 0;
   virtual void schedule_warmup(std::function<void()> cb) = 0;
   virtual std::string name() = 0;
   virtual void cleanup_thread() = 0;
   virtual ~DBTraits() = default;
};

struct LeanStoreTraits : public DBTraits {
   leanstore::cr::CRManager& crm;
   explicit LeanStoreTraits(leanstore::cr::CRManager& crm) : crm(crm) { std::cout << "Running experiment with " << name() << std::endl; }
   ~LeanStoreTraits() = default;
   void run_tx(std::function<void()> cb)
   {
      crm.scheduleJobSync(1, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE);
         cb();
         leanstore::cr::Worker::my().commitTX();
      });
   }

   void schedule_warmup(std::function<void()> cb)
   {
      crm.scheduleJobAsync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE);
         cb();
         leanstore::cr::Worker::my().commitTX();
      });
   }

   void cleanup_thread() { leanstore::cr::Worker::my().shutdown(); }

   std::string name() { return "LeanStore"; }
};

struct RocksDBTraits : public DBTraits {
   RocksDB& rocks_db;
   explicit RocksDBTraits(RocksDB& rocks_db) : rocks_db(rocks_db) { std::cout << "Running experiment with " << name() << std::endl; }
   ~RocksDBTraits() = default;
   void run_tx(std::function<void()> cb)
   {
      rocks_db.startTX();
      cb();
      rocks_db.commitTX();
   }
   void schedule_warmup(std::function<void()> cb)
   {
      std::thread([&]() { cb(); }).detach();
   }
   void cleanup_thread()
   {  // No cleanup needed for RocksDB threads
   }
   std::string name() { return "RocksDB"; }
};

template <template <typename> class AdapterType>
struct ExecutableHelper {
   std::unique_ptr<DBTraits> db_traits;
   std::string method;

   std::function<void()> lookup_cb;
   std::vector<std::function<void()>> elapsed_cbs;
   std::vector<std::function<void()>> tput_cbs;
   std::vector<std::string> tput_prefixes;
   TPCHWorkload<AdapterType> tpch;
   double size;
   std::function<void()> cleanup_cb = []() {};

   std::atomic<bool> keep_running_warmup = true;
   std::atomic<u64> lookup_count = 0;
   std::atomic<u64> running_threads_counter = 0;

   ExecutableHelper(leanstore::cr::CRManager& crm,
                    std::string method,
                    TPCHWorkload<AdapterType> tpch,
                    std::function<double()> size_cb,
                    std::function<void()> lookup_cb,
                    std::vector<std::function<void()>> elapsed_cbs,
                    std::vector<std::function<void()>> tput_cbs,
                    std::vector<std::string> tput_prefixes)
       : db_traits(std::make_unique<LeanStoreTraits>(crm)),
         method(method),
         lookup_cb(lookup_cb),
         elapsed_cbs(elapsed_cbs),
         tput_cbs(tput_cbs),
         tput_prefixes(tput_prefixes),
         tpch(tpch),
         size(size_cb())
   {
   }

   ExecutableHelper(RocksDB& rocks_db,
                    std::string method,
                    TPCHWorkload<AdapterType> tpch,
                    std::function<double()> size_cb,
                    std::function<void()> lookup,
                    std::vector<std::function<void()>> elapsed_cbs,
                    std::vector<std::function<void()>> tput_cbs,
                    std::vector<std::string> tput_prefixes,
                    std::function<void()> cleanup_cb)
       : db_traits(std::make_unique<RocksDBTraits>(rocks_db)),
         method(method),
         lookup_cb(lookup),
         elapsed_cbs(elapsed_cbs),
         tput_cbs(tput_cbs),
         tput_prefixes(tput_prefixes),
         tpch(tpch),
         size(size_cb()),
         cleanup_cb(cleanup_cb)
   {
   }

   void run()
   {
      std::cout << std::string(20, '=') << method << "," << size << std::string(20, '=') << std::endl;
      tpch.prepare();

      warmup();

      for (auto& cb : elapsed_cbs) {
         running_threads_counter++;
         db_traits->run_tx(cb);
         running_threads_counter--;
      }

      for (size_t i = 0; i < tput_cbs.size(); i++) {
         tput_tx(tput_cbs[i], tput_prefixes[i]);
      }

      keep_running_warmup = false;

      while (running_threads_counter > 0) {
         std::this_thread::sleep_for(std::chrono::milliseconds(100));  // sleep 0.1 sec
      }

      std::cout << "All threads finished. Cleaning up inserted data from maintenance phase..." << std::endl;
      db_traits->run_tx(cleanup_cb);
   }
   void warmup()
   {
      db_traits->schedule_warmup([this]() { warmup_phase(); });
      sleep(FLAGS_warmup_seconds);
   }

   void warmup_phase()
   {
      running_threads_counter++;
      while (keep_running_warmup) {
         jumpmuTry()
         {
            lookup_cb();
            lookup_count++;
         }
         jumpmuCatchNoPrint()
         {
            std::cerr << "#" << lookup_count.load() << " point lookups failed." << std::endl;
         }
         if (lookup_count.load() % 100 == 1 && running_threads_counter == 1 && FLAGS_log_progress)
            std::cout << "\r#" << lookup_count.load() << " warm-up point lookups performed.";
      }
      std::cout << std::endl;
      std::cout << "#" << lookup_count.load() << " warm-up point lookups in total performed." << std::endl;
      db_traits->cleanup_thread();
      running_threads_counter--;
   }

   void tput_tx(std::function<void()> cb, std::string tx)
   {
      tpch.logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      atomic<int> count = 0;

      std::cout << "Running " << tx << " on " << method << " for " << FLAGS_tx_seconds << " seconds..." << std::endl;

      atomic<bool> keep_running_tx = true;
      std::thread([&] {
         std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tx_seconds));
         keep_running_tx = false;
      }).detach();

      running_threads_counter++;

      while (keep_running_tx || count.load() < 10) {
         jumpmuTry()
         {
            db_traits->run_tx(cb);
            count++;
         }
         jumpmuCatchNoPrint()
         {
            std::cerr << "#" << count.load() << " " << tx << "for " << method << " failed." << std::endl;
         }
         if (count.load() % 1000 == 1 && FLAGS_log_progress)
            std::cout << "\r#" << count.load() << " " << tx << " for " << method << " performed.";
      }

      std::cout << "#" << count.load() << " " << tx << " for " << method << " performed." << std::endl;

      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      double tput = (double)count.load() / duration * 1e6;
      tpch.logger.log(static_cast<long>(std::round(tput)), count.load(), tx, method, size);
      running_threads_counter--;
   }
};
