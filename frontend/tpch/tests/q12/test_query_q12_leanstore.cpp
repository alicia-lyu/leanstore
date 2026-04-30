#ifndef ROCKSDB_ONLY
// Standalone query test: loads MI[0] + Q12 pipeline view, then runs all four
// query_by_* paths and asserts cross-structure parity and shape correctness.
//
// Mirrors test_load_q12_leanstore.cpp for the load phase; adds query execution
// and correctness checks via test_query_q12_checks.hpp.

#include <gflags/gflags.h>
#include <chrono>
#include <iomanip>
#include <iostream>

#include "../../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../../backend.hpp"
#include "../../ol_pipeline.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../q12/workload.hpp"
#include "test_query_q12_checks.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q12 query test — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   B::Adapter<part_t>      part;
   B::Adapter<supplier_t>  supplier;
   B::Adapter<partsupp_t>  partsupp;
   B::Adapter<customerh_t> customer;
   B::Adapter<orders_t>    orders;
   B::Adapter<lineitem_t>  lineitem;
   B::Adapter<nation_t>    nation;
   B::Adapter<region_t>    region;

   B::MergedAdapter<orders_t, lineitem_t> merged_ol;
   B::Adapter<tpch::q12::q12_pipeline_view_t> pipeline_view;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part          = B::Adapter<part_t>(db, "part");
      supplier      = B::Adapter<supplier_t>(db, "supplier");
      partsupp      = B::Adapter<partsupp_t>(db, "partsupp");
      customer      = B::Adapter<customerh_t>(db, "customer");
      orders        = B::Adapter<orders_t>(db, "orders");
      lineitem      = B::Adapter<lineitem_t>(db, "lineitem");
      nation        = B::Adapter<nation_t>(db, "nation");
      region        = B::Adapter<region_t>(db, "region");
      merged_ol     = B::MergedAdapter<orders_t, lineitem_t>(db, "test_merged_ol");
      pipeline_view = B::Adapter<tpch::q12::q12_pipeline_view_t>(db, "q12_pipeline_view");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      leanstore::cr::Worker::my().commitTX();
   });

   tpch::OrdersLineitemPipeline<B> ol(orders, lineitem, merged_ol);
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      ol.populate_merged();
      leanstore::cr::Worker::my().commitTX();
   });

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch::q12::populate_q12_view<B>(orders, lineitem, pipeline_view);
      leanstore::cr::Worker::my().commitTX();
   });

   // Run all four query_by_* paths inside a read transaction.
   tpch::q12::Q12Workload<B> q12(tpch, orders, lineitem, pipeline_view, merged_ol);

   std::vector<tpch::q12::q12_agg_row_t> r_base, r_view, r_merged, r_hash;
   long us_base = 0, us_view = 0, us_merged = 0, us_hash = 0;
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      // Per-query wall-clock timing — see test_query_q12_rocksdb.cpp for
      // rationale. LeanStore variant times inside the worker job so the
      // measurements include only the query phase, not transaction setup.
      auto time_us = [](auto&& fn) {
         auto t0 = std::chrono::high_resolution_clock::now();
         fn();
         auto t1 = std::chrono::high_resolution_clock::now();
         return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
      };
      us_base   = time_us([&] { q12.query_by_base  (r_base);   });
      us_view   = time_us([&] { q12.query_by_view  (r_view);   });
      us_merged = time_us([&] { q12.query_by_merged(r_merged); });
      us_hash   = time_us([&] { q12.query_by_hash  (r_hash);   });
      leanstore::cr::Worker::my().commitTX();
   });

   auto print_timing = [](const char* name, long us) {
      std::cout << "[time] " << std::left << std::setw(16) << name
                << std::right << std::setw(10) << us << " us  ("
                << std::fixed << std::setprecision(3) << (us / 1000.0) << " ms)\n";
   };
   print_timing("query_by_base",   us_base);
   print_timing("query_by_view",   us_view);
   print_timing("query_by_merged", us_merged);
   print_timing("query_by_hash",   us_hash);

   // Print result tables for human inspection.
   tpch::q12::print_rows("query_by_base",   r_base,   std::cout);
   tpch::q12::print_rows("query_by_view",   r_view,   std::cout);
   tpch::q12::print_rows("query_by_merged", r_merged, std::cout);
   tpch::q12::print_rows("query_by_hash",   r_hash,   std::cout);

   const tpch::q12::Params params = tpch::q12::Params::defaults();
   bool all_ok = true;

   // Shape checks (2 rows, expected shipmodes, non-zero counts).
   all_ok = tpch::q12::check_shape_q12("base",   r_base,   params, std::cout) && all_ok;
   all_ok = tpch::q12::check_shape_q12("view",   r_view,   params, std::cout) && all_ok;
   all_ok = tpch::q12::check_shape_q12("merged", r_merged, params, std::cout) && all_ok;
   all_ok = tpch::q12::check_shape_q12("hash",   r_hash,   params, std::cout) && all_ok;

   // Cross-structure parity: all four must produce identical results.
   all_ok = tpch::q12::check_parity_q12(r_base, r_view, r_merged, r_hash, std::cout) && all_ok;

   return all_ok ? 0 : 1;
}

#endif  // ROCKSDB_ONLY
