#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q3 workload.
//
// Same shape as executable_rocksdb.cpp but uses LeanStoreBackend.
// Wrapped in #ifndef ROCKSDB_ONLY so macOS builds (which lack the LeanStore
// B-tree headers) do not attempt to compile this file.
//
// CMake target (to be added in a later pass):
//   add_executable(q3_btree tpch/q3/executable_leanstore.cpp)
//   target_link_libraries(q3_btree leanstore LeanStoreLogger ...)
// See: frontend/tpch/CLAUDE.md §CMake Targets.

#include <gflags/gflags.h>
#include <iostream>

#include "../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
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
DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");
DEFINE_int32(bgw_pct, 0, "Percentage of background write transactions");
DEFINE_bool(log_progress, true, "Log loading/query progress");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3 workload — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;

   using B = tpch::LeanStoreBackend;

   // Base TPC-H tables (default-constructed; assigned inside crm callback).
   B::Adapter<part_t>      part;
   B::Adapter<supplier_t>  supplier;
   B::Adapter<partsupp_t>  partsupp;
   B::Adapter<customerh_t> customer;
   B::Adapter<orders_t>    orders;
   B::Adapter<lineitem_t>  lineitem;
   B::Adapter<nation_t>    nation;
   B::Adapter<region_t>    region;

   // Q3-specific adapters
   B::Adapter<tpch::q3::q3_pipeline_view_t>  pipeline_view;
   B::MergedAdapter<orders_t, lineitem_t>    merged_ol;

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
      pipeline_view = B::Adapter<tpch::q3::q3_pipeline_view_t>(db, "q3_pipeline_view");
      merged_ol     = B::MergedAdapter<orders_t, lineitem_t>(db, "q3_merged_ol");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   tpch::q3::Q3Workload<B> q3(tpch, orders, lineitem, customer,
                                pipeline_view, merged_ol);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         q3.load();
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   } else {
      tpch.recover_last_ids();
   }

   std::vector<tpch::q3::q3_agg_row_t> result;

   switch (FLAGS_storage_structure) {
      case 1: {
         // TODO(skeleton): Construct BaseQ3<B>{q3} and run query loop.
         // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 1.
         tpch::q3::BaseQ3<B> wrapper{q3};
         wrapper.query(result);
         break;
      }
      case 2: {
         // TODO(skeleton): Construct ViewQ3<B>{q3} and run query loop.
         // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 2 & 4.
         tpch::q3::ViewQ3<B> wrapper{q3};
         wrapper.query(result);
         break;
      }
      case 3: {
         // TODO(skeleton): Construct MergedQ3<B>{q3} and run query loop.
         // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 3.
         tpch::q3::MergedQ3<B> wrapper{q3};
         wrapper.query(result);
         break;
      }
      case 4: {
         // TODO(skeleton): Construct HashQ3<B>{q3} and run query loop.
         // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 2 & 4
         //      (hash join variant).
         tpch::q3::HashQ3<B> wrapper{q3};
         wrapper.query(result);
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }

   return 0;
}

#endif  // ROCKSDB_ONLY
