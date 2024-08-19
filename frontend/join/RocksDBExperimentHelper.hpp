#pragma once
#include <memory.h>
#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/RocksDB.hpp"
#include "../shared/RocksDBAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "ExperimentHelper.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"

DEFINE_string(rocks_db, "pessimistic", "none/pessimistic/optimistic");
DEFINE_bool(print_header, true, "");
class RocksDBExperimentHelper : public ExperimentHelper
{
  public:
   using orderline_sec_t = typename ExperimentHelper::orderline_sec_t;
   using joined_t = typename ExperimentHelper::joined_t;
   struct RocksDBExperimentContext {
      RocksDB::DB_TYPE type;
      RocksDB rocks_db;
      RocksDBAdapter<warehouse_t> warehouse;
      RocksDBAdapter<district_t> district;
      RocksDBAdapter<customer_t> customer;
      RocksDBAdapter<customer_wdl_t> customerwdl;
      RocksDBAdapter<history_t> history;
      RocksDBAdapter<neworder_t> neworder;
      RocksDBAdapter<order_t> order;
      RocksDBAdapter<order_wdc_t> order_wdc;
      RocksDBAdapter<orderline_t> orderline;
      RocksDBAdapter<item_t> item;
      RocksDBAdapter<stock_t> stock;

      TPCCWorkload<RocksDBAdapter> tpcc;

      RocksDBExperimentContext()
          : type([&]() -> RocksDB::DB_TYPE {
               if (FLAGS_rocks_db == "none") {
                  return RocksDB::DB_TYPE::DB;
               } else if (FLAGS_rocks_db == "pessimistic") {
                  return RocksDB::DB_TYPE::TransactionDB;
               } else if (FLAGS_rocks_db == "optimistic") {
                  // TODO: still WIP
                  UNREACHABLE();
                  return RocksDB::DB_TYPE::OptimisticDB;
               } else {
                  UNREACHABLE();
               }
            }()),
            rocks_db(type),
            warehouse(rocks_db),
            district(rocks_db),
            customer(rocks_db),
            customerwdl(rocks_db),
            history(rocks_db),
            neworder(rocks_db),
            order(rocks_db),
            order_wdc(rocks_db),
            orderline(rocks_db),
            item(rocks_db),
            stock(rocks_db),
            tpcc(warehouse,
                 district,
                 customer,
                 customerwdl,
                 history,
                 neworder,
                 order,
                 order_wdc,
                 orderline,
                 item,
                 stock,
                 FLAGS_order_wdc_index,
                 FLAGS_tpcc_warehouse_count,
                 FLAGS_tpcc_remove,
                 true,
                 true)
      {
      }
   };

  private:
   RocksDBExperimentContext* context_ptr;

  public:
   std::unique_ptr<RocksDBExperimentContext> prepareExperiment()
   {
      auto context = std::make_unique<RocksDBExperimentContext>();
      context_ptr = context.get();
      return context;
   };

   int loadCore(bool load_stock = true)
   {
      std::vector<thread> threads;
      context_ptr->rocks_db.startTX();
      context_ptr->tpcc.loadItem();
      context_ptr->tpcc.loadWarehouse();
      context_ptr->rocks_db.commitTX();
      std::atomic<u32> g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         threads.emplace_back([&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               jumpmuTry()
               {
                  context_ptr->rocks_db.startTX();
                  if (load_stock) {
                     context_ptr->tpcc.loadStock(w_id);
                  }
                  context_ptr->tpcc.loadDistrict(w_id);
                  for (Integer d_id = 1; d_id <= 10; d_id++) {
                     context_ptr->tpcc.loadCustomer(w_id, d_id);
                     context_ptr->tpcc.loadOrders(w_id, d_id);
                  }
                  context_ptr->rocks_db.commitTX();
               }
               jumpmuCatch()
               {
                  UNREACHABLE();
               }
            }
         });
      }
      for (auto& thread : threads) {
         thread.join();
      }
      return 0;
   }

   int scheduleTransations(TPCCBaseWorkload<RocksDBAdapter>* tpcc_base,
                           std::vector<thread>& threads,
                           atomic<u64>& keep_running,
                           atomic<u64>& running_threads_counter,
                           atomic<u64>* thread_committed,
                           atomic<u64>* thread_aborted)
   {
      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         thread_committed[t_i] = 0;
         thread_aborted[t_i] = 0;
         // -------------------------------------------------------------------------------------
         threads.emplace_back([&, t_i]() {
            running_threads_counter++;
            leanstore::CPUCounters::registerThread("worker_" + std::to_string(t_i), false);
            if (FLAGS_pin_threads) {
               leanstore::utils::pinThisThread(t_i);
            }
            context_ptr->tpcc.prepare();
            while (keep_running) {
               jumpmuTry()
               {
                  Integer w_id = context_ptr->tpcc.urand(1, FLAGS_tpcc_warehouse_count);
                  context_ptr->rocks_db.startTX();
                  tpcc_base->tx(w_id, FLAGS_read_percentage, FLAGS_scan_percentage, FLAGS_write_percentage, FLAGS_order_size);
                  context_ptr->rocks_db.commitTX();
                  thread_committed[t_i]++;
               }
               jumpmuCatch()
               {
                  thread_aborted[t_i]++;
               }
            }
            running_threads_counter--;
         });
      }
      return 0;
   }
};