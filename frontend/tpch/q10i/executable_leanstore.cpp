#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q10I workload — COLI-pipeline standalone.
// Not plugged into the Q3I+Q5I family cohort; loads independently.
// The COLI image layout (B-tree names) matches the Q3I/Q5I btree images
// so the same persisted DB can be opened by Q3I, Q5I, or Q10I btree
// executables interchangeably.

#include <gflags/gflags.h>
#include <iostream>

#include "../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "../backend.hpp"
#include "../tpchi_family/tpchi_tables.hpp"
#include "../tpchi_family/tpchi_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_executable_helper.hpp"

#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q10I workload — LeanStore backend (COLI family)");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   // Base TPC-H tables (lineitem uses invoice-extended variant).
   B::Adapter<part_t>        part;
   B::Adapter<supplier_t>    supplier;
   B::Adapter<partsupp_t>    partsupp;
   B::Adapter<customerh_t>   customer;
   B::Adapter<orders_t>      orders;
   B::Adapter<lineitem_i_t>  lineitem;
   B::Adapter<nation_t>      nation;
   B::Adapter<region_t>      region;
   B::Adapter<invoice_t>     invoice;

   // Q10I-specific view.
   B::Adapter<tpch::q10i::q10i_pipeline_view_t> q10i_view;

   // Shared COLI pipeline (image-compatible with Q3I / Q5I btree).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli;
   B::Adapter<tpch::orders_coli_t>   split_orders;
   B::Adapter<tpch::lineitem_coli_t> split_lineitem;
   B::Adapter<tpch::invoice_coli_t>  split_invoice;

   // aCOLI MI (Q3I S5 — declared to keep B-tree name set image-compatible).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli;

   // S2 variant B (per-order preagg view) + S5 Q10I aCOLI MI (2-type).
   B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t> q10i_view_preagg;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acoli_q10i_t> acoli_q10i;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part           = B::Adapter<part_t>(db, "part");
      supplier       = B::Adapter<supplier_t>(db, "supplier");
      partsupp       = B::Adapter<partsupp_t>(db, "partsupp");
      customer       = B::Adapter<customerh_t>(db, "customer");
      orders         = B::Adapter<orders_t>(db, "orders");
      lineitem       = B::Adapter<lineitem_i_t>(db, "lineitem");
      nation         = B::Adapter<nation_t>(db, "nation");
      region         = B::Adapter<region_t>(db, "region");
      invoice        = B::Adapter<invoice_t>(db, "invoice");
      q10i_view      = B::Adapter<tpch::q10i::q10i_pipeline_view_t>(db, "q10i_pipeline_view");
      merged_coli    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                        tpch::lineitem_coli_t, tpch::invoice_coli_t>(db, "coli_merged");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "coli_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_coli_t>(db, "coli_split_lineitem");
      split_invoice  = B::Adapter<tpch::invoice_coli_t>(db, "coli_split_invoice");
      acoli          = B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                                        tpch::lineitem_acoli_t>(db, "coli_acoli");
      q10i_view_preagg = B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t>(db, "q10i_pipeline_view_preagg");
      acoli_q10i     = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acoli_q10i_t>(db, "q10i_acoli_merged");
   });

   LeanStoreLogger logger(db);
   TPCHIWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                   orders, lineitem, nation, region, invoice, logger);
   tpch::q10i::Q10IWorkload<B> q10i(tpch, customer, orders, lineitem, invoice,
                                     nation, q10i_view, merged_coli,
                                     split_orders, split_lineitem, split_invoice,
                                     acoli, q10i_view_preagg, acoli_q10i);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         q10i.load();
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   tpch.recover_last_ids();

   using AggRow = tpch::q10i::q10i_agg_row_t;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q10i::BaseQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "base_merge_join");
         helper.run();
         break;
      }
      case 2: {
         tpch::q10i::ViewQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "pipeline_view");
         helper.run();
         break;
      }
      case 3: {
         tpch::q10i::MergedQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "mi_coli_walk");
         helper.run();
         break;
      }
      case 4: {
         tpch::q10i::HashQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "base_hash_join");
         helper.run();
         break;
      }
      case 5: {
         tpch::q10i::AggregatedQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "mi_acoli_preagg");
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
