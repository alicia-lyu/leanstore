#pragma once
#include <gflags/gflags_declare.h>
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"

DEFINE_int64(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_verify, false, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
// DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
// DEFINE_uint64(tpcc_analytical_weight, 0, "");
DEFINE_uint64(ch_a_threads, 0, "CH analytical threads");
DEFINE_uint64(ch_a_rounds, 1, "");
DEFINE_uint64(ch_a_query, 2, "");
DEFINE_uint64(ch_a_start_delay_sec, 0, "");
DEFINE_uint64(ch_a_process_delay_sec, 0, "");
// DEFINE_bool(ch_a_infinite, false, "");
// DEFINE_bool(ch_a_once, false, "");
DEFINE_uint32(tpcc_threads, 0, "");
DEFINE_uint32(read_percentage, 0, "");
DEFINE_uint32(scan_percentage, 0, "");
DEFINE_uint32(write_percentage, 100, "");
DEFINE_uint32(order_size, 5, "Number of lines in a new order");
// TODO: included columns
DEFINE_int32(semijoin_selectivity, 100, "\% of orderline to be joined with stock");
// Accomplished by only loading a subset of items. Semi-join selectivity of stock may be
// lower. Empirically 90+% items are present in some orderline, picking out those in stock.

#ifndef INCLUDE_COLUMNS
#define INCLUDE_COLUMNS \
   1  // All columns, unless defined. Now included columns in orderline secondary and joined results are the same. Maybe should be different.
#endif

template <template <typename> class AdapterType>
class TPCCBaseWorkload
{
  protected:
   TPCCWorkload<AdapterType>* tpcc;

  public:
   using orderline_sec_t = typename std::conditional<INCLUDE_COLUMNS == 0, ol_sec_key_only_t, ol_join_sec_t>::type;
   using joined_t = typename std::conditional<INCLUDE_COLUMNS == 0, joined_ols_key_only_t, joined_ols_t>::type;
   TPCCBaseWorkload(TPCCWorkload<AdapterType>* tpcc) : tpcc(tpcc)
   {
      if constexpr (INCLUDE_COLUMNS == 0) {
         std::cout << "Columns included: Key only" << std::endl;
      } else if constexpr (INCLUDE_COLUMNS == 1) {
         std::cout << "Columns included: All" << std::endl;
      } else {
         throw std::runtime_error("Invalid INCLUDE_COLUMNS value");
      }
   }
   virtual ~TPCCBaseWorkload() = default;

   virtual void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since) = 0;

   virtual void ordersByItemId(Integer w_id, Integer d_id, Integer i_id) = 0;

   virtual void newOrderRnd(Integer w_id, Integer order_size = 5) = 0;

   int tx(Integer w_id, int read_percentage, int scan_percentage, int write_percentage, Integer order_size = 5)
   {
      int rnd = (int)leanstore::utils::RandomGenerator::getRand(0, read_percentage + scan_percentage + write_percentage);
      if (rnd < read_percentage) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Integer i_id = leanstore::utils::RandomGenerator::getRand(1, (int)(tpcc->ITEMS_NO * tpcc->scale_factor) + 1);
         ordersByItemId(w_id, d_id, i_id);
         return 0;
      } else if (rnd < read_percentage + scan_percentage) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Timestamp since = 1;
         recentOrdersStockInfo(w_id, d_id, since);
         return 0;
      } else {
         newOrderRnd(w_id, order_size);
         return 0;
      }
   }

   virtual void verifyWarehouse(Integer w_id) = 0;
};