#pragma once
#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/RocksDB.hpp"
#include "../shared/RocksDBAdapter.hpp"
#include "../tpc-c/Schema.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "ExperimentHelper.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"

class RocksDBExperimentHelper : public ExperimentHelper
{

  public:
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
      RocksDBAdapter<orderline_sec_t> orderline_secondary;
      RocksDBAdapter<joined_t> joined_ols;

      RocksDBExperimentContext(std::string FLAGS_rocks_db)
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
            orderline_secondary(rocks_db),
            joined_ols(rocks_db)
      {
      }
   };

   std::unique_ptr<RocksDBExperimentContext> prepareExperiment() {

   };

   int loadCore(bool load_stock = true)
   {
   }

   int verifyCore(TPCCBaseWorkload<RocksDBAdapter>* tpcc_base)
   {
   }

   int scheduleTransations(TPCCBaseWorkload<RocksDBAdapter>* tpcc_base,
                           atomic<u64>& keep_running,
                           atomic<u64>& running_threads_counter,
                           u64* tx_per_thread)
   {
   }
};