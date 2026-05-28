#pragma once

#include <gflags/gflags_declare.h>
#include <functional>
#include <iostream>
#include <string>
#include "RocksDB.hpp"
#ifndef ROCKSDB_ONLY
#include "leanstore/concurrency-recovery/CRMG.hpp"
#endif
#include "leanstore/utils/JumpMU.hpp"

DECLARE_int32(warmup_seconds);
DECLARE_int32(tx_seconds);

static constexpr u64 BG_WORKER        = 0;  // cohort rotation (or foreground re-run fallback)
static constexpr u64 MAIN_WORKER      = 1;  // foreground query
static constexpr u64 BG_LOOKUP_WORKER = 2;  // dedicated point-lookup stream (bg=2)

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

#ifndef ROCKSDB_ONLY
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
#endif

struct RocksDBTraits : public DBTraits {
   RocksDB& rocks_db;
   explicit RocksDBTraits(RocksDB& rocks_db) : rocks_db(rocks_db) { std::cout << "Running experiment with " << name() << std::endl; }
   ~RocksDBTraits() = default;
   void run_tx(std::function<void()> cb, u64)
   {
      rocks_db.startTX();
      cb();
      rocks_db.commitTX();
   }
   void cleanup_thread(u64) {}

   void rollback_tx(u64) { rocks_db.rollbackTX(); }
   std::string name() { return "RocksDB"; }
};
