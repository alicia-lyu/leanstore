#pragma once
#include <gflags/gflags.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <thread>
#include "../shared/RocksDB.hpp"
#include "../tpc-h/logger.hpp"
#include "../tpc-h/workload.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "workload.hpp"

DECLARE_int32(storage_structure);
DECLARE_int32(warmup_seconds);
DECLARE_int32(tx_seconds);

static constexpr u64 BG_WORKER = 0;
static constexpr u64 MAIN_WORKER = 1;

struct DBTraits {
   virtual void run_tx(std::function<void()> cb, u64 worker_id = MAIN_WORKER) = 0;
   virtual void run_tx_w_rollback(std::function<void()> cb, std::string tx, u64 worker_id = MAIN_WORKER)
   {
      jumpmuTry()
      {
         run_tx(cb, worker_id);
      }
      jumpmuCatch()
      {
         rollback_tx(worker_id);
         std::cerr << "Transaction " << tx << " failed." << std::endl;
      }
   }
   virtual std::string name() = 0;
   virtual void cleanup_thread(u64 worker_id) = 0;
   virtual void rollback_tx(u64 worker_id) = 0;
   virtual ~DBTraits() = default;
};

struct LeanStoreTraits : public DBTraits {
   leanstore::cr::CRManager& crm;
   explicit LeanStoreTraits(leanstore::cr::CRManager& crm) : crm(crm) { std::cout << "Running experiment with " << name() << std::endl; }
   ~LeanStoreTraits() = default;
   void run_tx(std::function<void()> cb, u64 worker_id)
   {
      crm.scheduleJobSync(worker_id, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE);
         cb();
         leanstore::cr::Worker::my().commitTX();
      });
   }

   void cleanup_thread(u64 worker_id)
   {
      crm.scheduleJobSync(worker_id, [&]() { leanstore::cr::Worker::my().shutdown(); });
   }

   void rollback_tx(u64) {}

   std::string name() { return "LeanStore"; }
};

struct RocksDBTraits : public DBTraits {
   RocksDB& rocks_db;
   explicit RocksDBTraits(RocksDB& rocks_db) : rocks_db(rocks_db) { std::cout << "Running experiment with " << name() << std::endl; }
   ~RocksDBTraits() = default;
   void run_tx(std::function<void()> cb, u64) // worker id determined by caller thread
   {
      rocks_db.startTX();
      cb();
      rocks_db.commitTX();
   }
   void cleanup_thread(u64)
   {  // No cleanup needed for RocksDB threads
   }

   void rollback_tx(u64) { rocks_db.rollbackTX(); }
   std::string name() { return "RocksDB"; }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct ExecutableHelper {
   std::unique_ptr<DBTraits> db_traits;
   std::string method;

   std::function<void()> lookup_cb;
   std::function<void()> update_cb;
   std::function<bool()> erase_cb;

   std::vector<std::function<void()>> elapsed_cbs;
   std::vector<std::function<void()>> tput_cbs;
   std::vector<std::string> tput_prefixes;
   TPCHWorkload<AdapterType>& tpch;
   geo_join::GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& geo_join;

   std::function<double()> size_cb;
   std::function<void()> cleanup_cb = []() {};

   std::atomic<bool> keep_running_bg_tx = true;
   std::atomic<u64> bg_tx_count = 0;
   std::atomic<u64> running_threads_counter = 0;

   ExecutableHelper(leanstore::cr::CRManager& crm,
                    std::string method,
                    TPCHWorkload<AdapterType>& tpch,
                    geo_join::GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& geo_join,
                    std::function<double()> size_cb,
                    std::function<void()> lookup_cb,
                    std::function<void()> update_cb,
                    std::function<bool()> erase_cb,
                    std::vector<std::function<void()>> elapsed_cbs,
                    std::vector<std::function<void()>> tput_cbs,
                    std::vector<std::string> tput_prefixes)
       : db_traits(std::make_unique<LeanStoreTraits>(crm)),
         method(method),
         lookup_cb(lookup_cb),
         update_cb(update_cb),
         erase_cb(erase_cb),
         elapsed_cbs(elapsed_cbs),
         tput_cbs(tput_cbs),
         tput_prefixes(tput_prefixes),
         tpch(tpch),
         geo_join(geo_join),
         size_cb(size_cb)
   {
   }

   ExecutableHelper(RocksDB& rocks_db,
                    std::string method,
                    TPCHWorkload<AdapterType>& tpch,
                    geo_join::GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& geo_join,
                    std::function<double()> size_cb,
                    std::function<void()> lookup_cb,
                    std::function<void()> update_cb,
                    std::function<bool()> erase_cb,
                    std::vector<std::function<void()>> elapsed_cbs,
                    std::vector<std::function<void()>> tput_cbs,
                    std::vector<std::string> tput_prefixes,
                    std::function<void()> cleanup_cb)
       : db_traits(std::make_unique<RocksDBTraits>(rocks_db)),
         method(method),
         lookup_cb(lookup_cb),
         update_cb(update_cb),
         erase_cb(erase_cb),
         elapsed_cbs(elapsed_cbs),
         tput_cbs(tput_cbs),
         tput_prefixes(tput_prefixes),
         tpch(tpch),
         geo_join(geo_join),
         size_cb(size_cb),
         cleanup_cb(cleanup_cb)
   {
   }

   void run()
   {
      std::cout << std::string(20, '=') << method << "," << size_cb() << std::string(20, '=') << std::endl;
      tpch.prepare();

      schedule_bg_txs();

      for (auto& cb : elapsed_cbs) {
         running_threads_counter++;
         db_traits->run_tx_w_rollback(cb, "elapsed");
         running_threads_counter--;
      }

      for (size_t i = 0; i < tput_cbs.size(); i++) {
         tput_tx(tput_cbs[i], tput_prefixes[i]);
      }

      keep_running_bg_tx = false;
      // wait for background thread to finish
      while (running_threads_counter > 0) {
         std::this_thread::sleep_for(std::chrono::milliseconds(100));  // sleep 0.1 sec
      }

      tput_tx(update_cb, "maintain");  // isolate update perf

      while (running_threads_counter > 0) {
         std::this_thread::sleep_for(std::chrono::milliseconds(100));  // sleep 0.1 sec
      }

      std::cout << "All threads finished. Cleaning up inserted data from maintenance phase..." << std::endl;
      db_traits->run_tx_w_rollback(cleanup_cb, "cleanup");
      db_traits->cleanup_thread(MAIN_WORKER);
   }
   void schedule_bg_txs()
   {
      std::thread([this]() {
         running_threads_counter++;
         bool customer_to_erase = false;
         while (keep_running_bg_tx) {
            int lottery = rand() % 100;
            std::string tx_type;
            jumpmuTry()
            {
               if (lottery < FLAGS_bgw_pct) {
                  if ((lottery < FLAGS_bgw_pct / 2 && customer_to_erase) ||  // half of the time erase if we have customers to erase
                      geo_join.insertion_complete()) {                       // or when insertion needs to be reset
                     db_traits->run_tx([&]() { customer_to_erase = erase_cb(); }, BG_WORKER);
                     tx_type = "erase";
                  } else {
                     db_traits->run_tx(update_cb, BG_WORKER);
                     tx_type = "update";
                     customer_to_erase = true;
                  }
               } else {
                  db_traits->run_tx(lookup_cb, BG_WORKER);
                  tx_type = "lookup";
               }
               bg_tx_count++;
            }
            jumpmuCatchNoPrint()
            {
               db_traits->rollback_tx(BG_WORKER);
               std::cerr << "#" << bg_tx_count.load() << " bg " << tx_type << " tx failed." << std::endl;
            }
            if (bg_tx_count.load() % 100 == 1 && running_threads_counter == 1 && FLAGS_log_progress)
               std::cout << "\r#" << bg_tx_count.load() << " bg tx performed.";
         }
         std::cout << "Remaining customers to erase = " << geo_join.remaining_customers_to_erase() << std::endl;
         while (customer_to_erase) {
            db_traits->run_tx_w_rollback([&]() { customer_to_erase = erase_cb(); }, "erase", BG_WORKER);
         }
         std::cout << std::endl;
         std::cout << "#" << bg_tx_count.load() << " bg tx in total performed." << std::endl;
         db_traits->cleanup_thread(BG_WORKER);
         running_threads_counter--;
      }).detach();
      sleep(FLAGS_warmup_seconds);  // warmup phase; then background phase
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
         std::cout << "keeping_running = false";
         keep_running_tx = false;
      }).detach();

      running_threads_counter++;

      while (keep_running_tx) {
         if (tx == "maintain" && geo_join.insertion_complete()) {
            std::cout << "Maintenance phase completed. No more insertions." << std::endl;
            break;
         }
         jumpmuTry()
         {
            db_traits->run_tx(cb);
            count++;
         }
         jumpmuCatchNoPrint()
         {
            db_traits->rollback_tx(MAIN_WORKER);
            std::cerr << "#" << count.load() << " " << tx << "for " << method << " failed." << std::endl;
         }
         if ((count.load() % 1000 == 1 && FLAGS_log_progress) || keep_running_tx == false) {
            std::cout << "\r#" << count.load() << " " << tx << " for " << method << " performed.";
         }
      }

      std::cout << "#" << count.load() << " " << tx << " for " << method << " performed." << std::endl;

      auto end = std::chrono::high_resolution_clock::now();
      long duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      double tput = (double)count.load() / duration * 1e6;
      tpch.logger.log(tput, count.load(), tx, method, size_cb());
      running_threads_counter--;
   }
};
