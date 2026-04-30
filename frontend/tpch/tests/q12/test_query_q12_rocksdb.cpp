// Standalone query test: loads MI[0] + Q12 pipeline view, then runs all four
// query_by_* paths and asserts cross-structure parity and shape correctness.
//
// Mirrors test_load_q12_rocksdb.cpp for the load phase; adds query execution
// and correctness checks via test_query_q12_checks.hpp.

#include <gflags/gflags.h>
#include <iostream>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../ol_pipeline.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../q12/workload.hpp"
#include "test_query_q12_checks.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q12 query test — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // All 8 TPC-H base tables.
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // MI[0]: merged ORDERS x LINEITEM index.
   B::MergedAdapter<orders_t, lineitem_t> merged_ol(rocks_db);

   // Structure 2 pipeline view (joined_ol_t rows, unfiltered).
   B::Adapter<tpch::q12::q12_pipeline_view_t> pipeline_view(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch.load();

   // MI[0]: dual-write replay of base tables.
   tpch::OrdersLineitemPipeline<B> ol(orders, lineitem, merged_ol);
   ol.populate_merged();

   // Pipeline view: manual two-pointer merge, one row per (order, lineitem).
   tpch::q12::populate_q12_view<B>(orders, lineitem, pipeline_view);

   // Run all four query_by_* paths.
   tpch::q12::Q12Workload<B> q12(tpch, orders, lineitem, pipeline_view, merged_ol);

   std::vector<tpch::q12::q12_agg_row_t> r_base, r_view, r_merged, r_hash;
   q12.query_by_base  (r_base);
   q12.query_by_view  (r_view);
   q12.query_by_merged(r_merged);
   q12.query_by_hash  (r_hash);

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
