#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q10I workload — TPCHi family cohort {Q3I, Q5I, Q10I}.
// See q10i/executable_rocksdb.cpp header for the family-loader rationale.

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
#include "../tpchi_family.hpp"

#include "../tpchi_family/coli_pipeline.hpp"
#include "../q3i/per_structure_workload.hpp"
#include "../q3i/workload.hpp"
#include "../q5i/per_structure_workload.hpp"
#include "../q5i/workload.hpp"
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

   // Per-query views (family cohort: Q3I + Q5I + Q10I).
   B::Adapter<tpch::q3i::q3i_pipeline_view_t>           q3i_view;
   B::Adapter<tpch::q5i::q5i_pipeline_view_t>           q5i_view;
   B::Adapter<tpch::q10i::q10i_pipeline_view_t>         q10i_view;
   B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t>  q10i_view_preagg;

   // Shared COLI pipeline (image-compatible with Q3I / Q5I btree).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli;
   B::Adapter<tpch::orders_coli_t>   split_orders;
   B::Adapter<tpch::lineitem_coli_t> split_lineitem;
   B::Adapter<tpch::invoice_coli_t>  split_invoice;

   // aCOLI MI (Q3I S5 — declared to keep B-tree name set image-compatible).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli;

   // Q10I S5 aCOLI 2-type MI.
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
      q3i_view       = B::Adapter<tpch::q3i::q3i_pipeline_view_t>(db, "q3i_pipeline_view");
      q5i_view       = B::Adapter<tpch::q5i::q5i_pipeline_view_t>(db, "q5i_pipeline_view");
      q10i_view      = B::Adapter<tpch::q10i::q10i_pipeline_view_t>(db, "q10i_pipeline_view");
      q10i_view_preagg = B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t>(db, "q10i_pipeline_view_preagg");
      merged_coli    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                        tpch::lineitem_coli_t, tpch::invoice_coli_t>(db, "coli_merged");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "coli_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_coli_t>(db, "coli_split_lineitem");
      split_invoice  = B::Adapter<tpch::invoice_coli_t>(db, "coli_split_invoice");
      acoli          = B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                                        tpch::lineitem_acoli_t>(db, "coli_acoli");
      acoli_q10i     = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acoli_q10i_t>(db, "q10i_acoli_merged");
   });

   LeanStoreLogger logger(db);
   TPCHIWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                   orders, lineitem, nation, region, invoice, logger);
   tpch::q3i::Q3IWorkload<B> q3i(tpch, customer, orders, lineitem, invoice,
                                   q3i_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);
   tpch::q5i::Q5IWorkload<B> q5i(tpch, customer, orders, lineitem, invoice,
                                   supplier, nation, region,
                                   q5i_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);
   tpch::q10i::Q10IWorkload<B> q10i(tpch, customer, orders, lineitem, invoice,
                                     nation, q10i_view, merged_coli,
                                     split_orders, split_lineitem, split_invoice,
                                     acoli, q10i_view_preagg, acoli_q10i);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpch::load_tpchi_family<B>(tpch, q3i, q5i, q10i);
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   tpch.recover_last_ids();

   LeanStoreTraits db_traits(crm);

   using AggRow = tpch::q10i::q10i_agg_row_t;
   auto bg_catalog = FLAGS_bg_query_thread
                       ? tpch::register_tpchi_bg_catalog<B>(db_traits, tpch, q3i, q5i, q10i,
                                                          FLAGS_storage_structure,
                                                          FLAGS_bg_point_lookups)
                       : std::vector<tpch::BgCatalogFn>{};
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q10i::BaseQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 2: {
         tpch::q10i::ViewQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 3: {
         tpch::q10i::MergedQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "mi_coli_walk");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 4: {
         tpch::q10i::HashQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 5: {
         tpch::q10i::AggregatedQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "mi_acoli_preagg");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 7: {
         tpch::q10i::PreaggViewQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             crm, std::move(w), tpch, "preagg_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
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
