// RocksDB entry point for Q12 workload.
//
// Wires gflags, opens a RocksDB TransactionDB, declares the four adapters
// Q12 needs (orders, lineitem, pipeline_view, merged_ol), constructs the
// TPCHWorkload and Q12Workload, then dispatches to the chosen per-structure
// wrapper based on FLAGS_storage_structure.
//
// CMake target (to be added in a later pass):
//   add_executable(q12_lsm tpch/q12/executable_rocksdb.cpp)
//   target_link_libraries(q12_lsm RocksDB RocksDBLogger ...)
//   target_compile_definitions(q12_lsm PRIVATE ROCKSDB_ONLY)
// See: frontend/tpch/q12/CLAUDE.md §CMake Targets.

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

// Gflags defined in geo executable_rocksdb.cpp are re-declared here because
// there is no shared definition file yet. Once a shared flags header exists,
// replace these with DECLARE_*.
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
   gflags::SetUsageMessage("Q12 workload — RocksDB backend");
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

   // Q12-specific adapters
   B::Adapter<tpch::q12::q12_pipeline_view_t>  pipeline_view(rocks_db);
   B::MergedAdapter<orders_t, lineitem_t>       merged_ol(rocks_db);

   rocks_db.open();  // must be called after all adapters register their CFs

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   tpch::q12::Q12Workload<B> q12(tpch, orders, lineitem, pipeline_view, merged_ol);

   if (!FLAGS_recover) {
      q12.load();
      return 0;
   } else {
      tpch.recover_last_ids();
   }

   std::vector<tpch::q12::q12_agg_row_t> result;

   switch (FLAGS_storage_structure) {
      case 1: {
         // TODO(skeleton): Construct BaseQ12<B>{q12} and run query loop.
         // See: frontend/tpch/q12/CLAUDE.md §Storage Structure Options, structure 1.
         tpch::q12::BaseQ12<B> wrapper{q12};
         wrapper.query(result);
         break;
      }
      case 2: {
         // TODO(skeleton): Construct ViewQ12<B>{q12} and run query loop.
         // See: frontend/tpch/q12/CLAUDE.md §Storage Structure Options, structure 2.
         tpch::q12::ViewQ12<B> wrapper{q12};
         wrapper.query(result);
         break;
      }
      case 3: {
         // TODO(skeleton): Construct MergedQ12<B>{q12} and run query loop.
         // See: frontend/tpch/q12/CLAUDE.md §Storage Structure Options, structure 3.
         tpch::q12::MergedQ12<B> wrapper{q12};
         wrapper.query(result);
         break;
      }
      case 4: {
         // TODO(skeleton): Construct HashQ12<B>{q12} and run query loop.
         // See: frontend/tpch/q12/CLAUDE.md §Storage Structure Options, structure 4.
         tpch::q12::HashQ12<B> wrapper{q12};
         wrapper.query(result);
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }

   return 0;
}
