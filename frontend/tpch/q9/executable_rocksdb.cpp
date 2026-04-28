// RocksDB entry point for Q9 workload.
//
// Wires gflags, opens a RocksDB TransactionDB, declares the eight adapters
// Q9 needs (orders, lineitem, nation, supplier, part, partsupp, pipeline_view,
// merged_ol), constructs TPCHWorkload and Q9Workload, then dispatches to the
// chosen per-structure wrapper based on FLAGS_storage_structure.
//
// NATION, SUPPLIER, PART, PARTSUPP are base-table adapters used at query time
// (loaded into hashmaps or merge-joined); they are not part of any merged index.
//
// CMake target (to be added in a later pass):
//   add_executable(q9_lsm tpch/q9/executable_rocksdb.cpp)
//   target_link_libraries(q9_lsm RocksDB RocksDBLogger ...)
//   target_compile_definitions(q9_lsm PRIVATE ROCKSDB_ONLY)
// See: frontend/tpch/CLAUDE.md §CMake Targets.

#include <gflags/gflags.h>
#include <iostream>

#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
#include "../../shared/RocksDB.hpp"
#include "../backend.hpp"
#include "../tpch_tables.hpp"
#include "../tpch_workload.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tpch_scale_factor, 1, "TPC-H scale factor");
DEFINE_int32(storage_structure, 1,
             "1=base merge-join, 2=pipeline view, 3=MI[0] premerged, 4=base hash-join");
DEFINE_int32(tx_seconds, 15, "Seconds to run each transaction type");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");
DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");
DEFINE_int32(bgw_pct, 0, "Percentage of background write transactions");
DEFINE_bool(log_progress, true, "Log loading/query progress");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q9 workload — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);

   using B = tpch::RocksDBBackend;

   // Base TPC-H tables
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // Q9-specific adapters
   B::Adapter<tpch::q9::q9_pipeline_view_t>  pipeline_view(rocks_db);
   B::MergedAdapter<orders_t, lineitem_t>    merged_ol(rocks_db);

   rocks_db.open();  // must be called after all adapters register their CFs

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   tpch::q9::Q9Workload<B> q9(tpch, orders, lineitem, nation, supplier,
                                part, partsupp, pipeline_view, merged_ol);

   if (!FLAGS_recover) {
      q9.load();
      return 0;
   } else {
      tpch.recover_last_ids();
   }

   std::vector<tpch::q9::q9_agg_row_t> result;

   switch (FLAGS_storage_structure) {
      case 1: {
         // TODO(skeleton): Construct BaseQ9<B>{q9} and run query loop.
         // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 1
         //      (5 merge joins).
         tpch::q9::BaseQ9<B> wrapper{q9};
         wrapper.query(result);
         break;
      }
      case 2: {
         // TODO(skeleton): Construct ViewQ9<B>{q9} and run query loop.
         // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 2 & 4.
         tpch::q9::ViewQ9<B> wrapper{q9};
         wrapper.query(result);
         break;
      }
      case 3: {
         // TODO(skeleton): Construct MergedQ9<B>{q9} and run query loop.
         // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 3
         //      (PremergedJoin + 4 remaining joins / hash lookups).
         tpch::q9::MergedQ9<B> wrapper{q9};
         wrapper.query(result);
         break;
      }
      case 4: {
         // TODO(skeleton): Construct HashQ9<B>{q9} and run query loop.
         // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 2 & 4
         //      (hash join variant — all 5 joins hash-joined).
         tpch::q9::HashQ9<B> wrapper{q9};
         wrapper.query(result);
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }

   return 0;
}
