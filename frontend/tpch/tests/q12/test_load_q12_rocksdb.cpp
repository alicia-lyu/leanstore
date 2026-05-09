// Standalone load-test: builds MI[0] + Q12 pipeline view, then cross-checks
// that both structures agree on row count and orderkey range.
//
// Analogue of test_load_merged_rocksdb.cpp but adds the per-query view step.
// Does not depend on query_by_* bodies — useful as a stepping-stone before
// the full q12_lsm executable exists.

#include <gflags/gflags.h>
#include <iostream>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../tpch_family/ol_pipeline.hpp"
#include "../test_load_merged_stats.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "test_load_view_stats.hpp"
#include "../../q12/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q12 view load-test — RocksDB backend");
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

   // Structure 2 pipeline view (joined_ol_t rows).
   B::Adapter<tpch::q12::q12_pipeline_view_t> pipeline_view(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch.load();  // base tables only

   // MI[0]
   tpch::OrdersLineitemPipeline<B> ol(orders, lineitem, merged_ol);
   ol.populate_merged();

   // Pipeline view: BinaryMergeJoin over base scanners.
   tpch::q12::populate_q12_view<B>(orders, lineitem, pipeline_view);

   // Stats and cross-check.
   const Integer expected_orders =
       TPCHWorkload<B::Adapter>::ORDERS_SCALE * FLAGS_tpch_scale_factor;
   const tpch::MergedOlStats mi_stats =
       tpch::dump_merged_ol_stats<B>(merged_ol, expected_orders, tpch.last_order_id);
   const tpch::q12::ViewStats view_stats =
       tpch::q12::dump_view_stats<B>(pipeline_view);
   tpch::q12::compare_mi_and_view(view_stats, mi_stats);

   return 0;
}
