#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q3 workload.
//
// Wires gflags, opens a LeanStore instance, declares the adapters Q3 needs,
// constructs the TPCHWorkload and Q3Workload, then dispatches to the chosen
// per-structure wrapper via tpch::TpchExecutableHelper, which drives a
// multi-second throughput loop and emits per-structure TPut.csv rows
// (parity with q3i_btree wiring).

#include <gflags/gflags.h>
#include <iostream>

#include "../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../backend.hpp"
#include "../tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_executable_helper.hpp"

#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3 workload — LeanStore backend");
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

   B::Adapter<tpch::q3::q3_pipeline_view_t> pipeline_view;

   // COL 3-table merged index (S3) + custkey-sorted split indexes (S1)
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col;
   B::Adapter<tpch::orders_coli_t>        split_orders;
   B::Adapter<tpch::lineitem_col_t>       split_lineitem;

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
      merged_col    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                       tpch::lineitem_col_t>(db, "q3_merged_col");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "q3_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "q3_split_lineitem");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               pipeline_view, merged_col,
                               split_orders, split_lineitem);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         q3.load();
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   tpch.recover_last_ids();

   using AggRow = tpch::q3::q3_agg_row_t;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q3::BaseQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "base_merge_join");
         helper.run();
         break;
      }
      case 2: {
         tpch::q3::ViewQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "pipeline_view");
         helper.run();
         break;
      }
      case 3: {
         tpch::q3::MergedQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "mi_col_walk");
         helper.run();
         break;
      }
      case 4: {
         tpch::q3::HashQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "base_hash_join");
         helper.run();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }
   return 0;
}

#endif  // ROCKSDB_ONLY
