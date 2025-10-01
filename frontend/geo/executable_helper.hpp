#pragma once
#include <gflags/gflags.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <thread>
#include "../shared/RocksDB.hpp"
#include "../shared/logger/logger.hpp"
#include "tpch_workload.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "per_structure_workload.hpp"

DECLARE_int32(storage_structure);
DECLARE_int32(warmup_seconds);
DECLARE_int32(tx_seconds);
DEFINE_bool(log_progress, true, "Log progress of the workload execution");

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
   void run_tx(std::function<void()> cb, u64)  // worker id determined by caller thread
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
   std::unique_ptr<PerStructureWorkload> workload;

   TPCHWorkload<AdapterType>& tpch;

   std::atomic<bool> keep_running_bg_tx = true;
   std::atomic<bool> run_main_thread = false;
   std::atomic<u64> bg_tx_count = 0;
   std::atomic<u64> running_threads_counter = 0;

   ExecutableHelper(leanstore::cr::CRManager& crm, std::unique_ptr<PerStructureWorkload> workload, TPCHWorkload<AdapterType>& tpch)
       : db_traits(std::make_unique<LeanStoreTraits>(crm)), workload(std::move(workload)), tpch(tpch)
   {
   }

   ExecutableHelper(RocksDB& rocks_db, std::unique_ptr<PerStructureWorkload> workload, TPCHWorkload<AdapterType>& tpch)
       : db_traits(std::make_unique<RocksDBTraits>(rocks_db)), workload(std::move(workload)), tpch(tpch)
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
      tpch.prepare();

      schedule_bg_txs();

      elapsed_tx(std::bind(&PerStructureWorkload::join, workload.get()), "join");

      tput_tx(std::bind(&PerStructureWorkload::join_ns, workload.get()), "join-ns");
      tput_tx(std::bind(&PerStructureWorkload::join_nsc, workload.get()), "join-nsc");
      // tput_tx(std::bind(&PerStructureWorkload::join_nscci, workload.get()), "join-nscci");
      tput_tx(std::bind(&PerStructureWorkload::mixed_ns, workload.get()), "mixed-ns");
      tput_tx(std::bind(&PerStructureWorkload::mixed_nsc, workload.get()), "mixed-nsc");
      // tput_tx(std::bind(&PerStructureWorkload::mixed_nscci, workload.get()), "mixed-nscci");

      keep_running_bg_tx = false;
      // wait for background thread to finish
      while (running_threads_counter > 0) {
         std::this_thread::sleep_for(std::chrono::milliseconds(100));  // sleep 0.1 sec
      }

      tput_tx(std::bind(&PerStructureWorkload::insert1, workload.get()), "maintain");

      std::cout << "All threads finished. Cleaning up inserted data..." << std::endl;

      db_traits->run_tx_w_rollback(std::bind(&PerStructureWorkload::cleanup_updates, workload.get()), "cleanup");

      db_traits->cleanup_thread(MAIN_WORKER);
   }

   void erase_remaining_customers(bool& customer_to_erase, long long& bg_erase_count)
   {
      std::cout << "Remaining customers to erase = " << workload->remaining_customers_to_erase() << std::endl;
      while (customer_to_erase) {
         db_traits->run_tx_w_rollback([&]() { customer_to_erase = workload->erase1(); }, "erase", BG_WORKER);
         bg_erase_count++;
      }
   }

   void schedule_bg_txs()
   {
      std::thread([this]() {
         running_threads_counter++;
         bool customer_to_erase = false;
         long long bg_insert_count = 0;
         long long bg_erase_count = 0;
         long long bg_lookup_count = 0;
         std::function<void()> periodic_reset = [&]() {
            // start time
            auto start = std::chrono::system_clock::now();
            erase_remaining_customers(customer_to_erase, bg_erase_count);
            workload->select_to_insert();
            auto end = std::chrono::system_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
            std::cout << "Periodic reset took " << elapsed << " seconds." << std::endl;
            run_main_thread = true;
         };
         while (keep_running_bg_tx) {
            int lottery = rand() % 100;
            std::string tx_type;
            jumpmuTry()
            {
               if (run_main_thread == false) {
                  periodic_reset();
               }
               if (lottery < FLAGS_bgw_pct) {
                  if ((lottery < FLAGS_bgw_pct / 2 && customer_to_erase) ||  // half of the time erase if we have customers to erase
                      workload->insertion_complete()) {                      // or when insertion needs to be reset
                     db_traits->run_tx([&]() { customer_to_erase = workload->erase1(); }, BG_WORKER);
                     tx_type = "erase";
                     bg_erase_count++;
                  } else {
                     db_traits->run_tx(std::bind(&PerStructureWorkload::insert1, workload.get()), BG_WORKER);
                     tx_type = "update";
                     customer_to_erase = true;
                     bg_insert_count++;
                  }
               } else {
                  db_traits->run_tx(std::bind(&PerStructureWorkload::bg_lookup, workload.get()), BG_WORKER);
                  tx_type = "lookup";
                  bg_lookup_count++;
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
         periodic_reset();

         std::cout << "#" << bg_tx_count.load() << " bg tx in total performed. " << bg_insert_count << " inserts, " << bg_erase_count << " erases, "
                   << bg_lookup_count << " lookups." << std::endl;
         db_traits->cleanup_thread(BG_WORKER);
         running_threads_counter--;
      }).detach();
      sleep(FLAGS_warmup_seconds);  // warmup phase; then background phase
   }

   void tput_tx(std::function<void()> cb, std::string tx)
   {
      while (!run_main_thread) {
      }
      tpch.logger.reset();
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

      while (keep_running_tx) {
         if (tx == "maintain" && workload->insertion_complete()) {
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
            std::cerr << "#" << count.load() << " " << tx << "for " << workload->get_name() << " failed." << std::endl;
         }
         if ((count.load() % 1000 == 1 && FLAGS_log_progress) || keep_running_tx == false) {
            std::cout << "\r#" << count.load() << " " << tx << " for " << workload->get_name() << " performed.";
         }
      }

      std::cout << "#" << count.load() << " " << tx << " for " << workload->get_name() << " performed." << std::endl;

      auto end = std::chrono::high_resolution_clock::now();
      long duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      double tput = (double)count.load() / duration * 1e6;
      tpch.logger.log(tput, count.load(), tx, workload->get_name(), workload->get_size());
      running_threads_counter--;
      run_main_thread = false;
   }
};
