#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q10 workload (Returned Item Reporting).
//
// Q10 is part of the vanilla TPC-H family cohort {Q3, Q5, Q10}; see
// q10/executable_rocksdb.cpp header for the family-loader rationale.

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
#include "../tpch_vanilla_family.hpp"

#include "../q3/per_structure_workload.hpp"
#include "../q3/workload.hpp"
#include "../q5/per_structure_workload.hpp"
#include "../q5/workload.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");
DEFINE_bool(q10_stats, false,
            "After the run, print the Q10Stats cardinality counters (rows scanned / "
            "passing filters / aggregated) for the chosen --storage_structure. "
            "Investigation instrumentation; keep --bg_query_thread=false (a bg worker "
            "races the shared counters).");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q10 workload — LeanStore backend");
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

   B::Adapter<tpch::q3::q3_pipeline_view_t> q3_view;
   B::Adapter<tpch::q5::q5_pipeline_view_t> q5_view;
   B::Adapter<tpch::q10::q10_pipeline_view_t>        q10_view;
   B::Adapter<tpch::q10::q10_pipeline_view_preagg_t> q10_view_preagg;

   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col;
   B::Adapter<tpch::orders_coli_t>        split_orders;
   B::Adapter<tpch::lineitem_col_t>       split_lineitem;

   // S5: aCOL MI — per-order pre-aggregated COL merged index (no lineitems).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t>  q10_acol;

   // S6: COL-family shared materialised view (family-shared with q3/q5).
   B::Adapter<tpch::col_shared_view_t>  shared_view;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part           = B::Adapter<part_t>(db, "part");
      supplier       = B::Adapter<supplier_t>(db, "supplier");
      partsupp       = B::Adapter<partsupp_t>(db, "partsupp");
      customer       = B::Adapter<customerh_t>(db, "customer");
      orders         = B::Adapter<orders_t>(db, "orders");
      lineitem       = B::Adapter<lineitem_t>(db, "lineitem");
      nation         = B::Adapter<nation_t>(db, "nation");
      region         = B::Adapter<region_t>(db, "region");
      q3_view        = B::Adapter<tpch::q3::q3_pipeline_view_t>(db, "q3_pipeline_view");
      q5_view        = B::Adapter<tpch::q5::q5_pipeline_view_t>(db, "q5_pipeline_view");
      q10_view       = B::Adapter<tpch::q10::q10_pipeline_view_t>(db, "q10_pipeline_view");
      q10_view_preagg = B::Adapter<tpch::q10::q10_pipeline_view_preagg_t>(db, "q10_pipeline_view_preagg");
      // Family-shared COL pipeline names so q3_btree / q5_btree / q10_btree
      // all mount the same trees.
      merged_col     = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                        tpch::lineitem_col_t>(db, "col_merged");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "col_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "col_split_lineitem");
      q10_acol       = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t>(db, "q10_acol");
      shared_view    = B::Adapter<tpch::col_shared_view_t>(db, "col_shared_view");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               q3_view, merged_col,
                               split_orders, split_lineitem, shared_view);
   tpch::q5::Q5Workload<B> q5(tpch, customer, orders, lineitem,
                               supplier, nation, region,
                               q5_view, merged_col,
                               split_orders, split_lineitem, shared_view);
   tpch::q10::Q10Workload<B> q10(tpch, customer, orders, lineitem, nation,
                                  q10_view, q10_view_preagg, merged_col,
                                  split_orders, split_lineitem, q10_acol, shared_view);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpch::load_vanilla_family<B>(FLAGS_storage_structure, tpch, q3, q5, q10);
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   tpch.recover_last_ids();

   LeanStoreTraits db_traits(crm);

   using AggRow = tpch::q10::q10_agg_row_t;
   tpch::q10::Q10Stats qstats;
   if (FLAGS_q10_stats) q10.stats = &qstats;
   long q10_txc = 0;
   auto bg_catalog = FLAGS_bg_query_thread
                       ? tpch::register_vanilla_bg_catalog<B>(db_traits, tpch, q3, q5, q10,
                                                            FLAGS_storage_structure,
                                                            FLAGS_bg_point_lookups)
                       : std::vector<tpch::BgCatalogFn>{};
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q10::BaseQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             crm, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 2: {
         tpch::q10::ViewQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             crm, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 3: {
         tpch::q10::MergedQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             crm, std::move(w), tpch, "mi_col_walk");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 4: {
         tpch::q10::HashQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             crm, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 5: {
         tpch::q10::AggregatedQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             crm, std::move(w), tpch, "mi_acol_preagg");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 6: {
         tpch::q10::SharedViewQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             crm, std::move(w), tpch, "shared_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 7: {
         tpch::q10::PreaggViewQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             crm, std::move(w), tpch, "preagg_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }
   if (FLAGS_q10_stats) tpch::q10::print_q10_stats(std::cout, qstats, q10_txc);
   return 0;
}

#endif  // ROCKSDB_ONLY
