// RocksDB entry point for Q10 workload (Returned Item Reporting).
//
// Q10 is COL-pipeline-based (CUSTOMER × ORDERS × LINEITEM merged index),
// reusing customer_coli_t / orders_coli_t / lineitem_col_t from Q3 / Q5
// verbatim. NATION attaches as a per-customer INL on PK at emit (D6) —
// no NATION-side secondary, no sidetable hashmap.
//
// Q10 is part of the vanilla TPC-H family cohort {Q3, Q5, Q10}. It mounts
// the same image layout as q3_lsm / q5_lsm and registers a bg-query cohort
// so --bg_query_thread=true cycles all three at the same
// --storage_structure as the foreground. Q10 owns the native S5 (aCOL MI)
// and S7 (per-order preagg view) variants — Q3/Q5 fall back to S3/S2 in
// those cohort positions.

#include <gflags/gflags.h>
#include <iostream>

#include "../../shared/RocksDB.hpp"
#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
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

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");
DEFINE_bool(q10_stats, false,
            "After the run, print the Q10Stats cardinality counters (rows scanned / "
            "passing filters / aggregated) for the chosen --storage_structure. "
            "Investigation instrumentation; keep --bg_query_thread=false (a bg worker "
            "races the shared counters).");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q10 workload — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // Base TPC-H tables.
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // Per-query views (Q3, Q5, Q10 — vanilla family cohort).
   B::Adapter<tpch::q3::q3_pipeline_view_t> q3_view(rocks_db);
   B::Adapter<tpch::q5::q5_pipeline_view_t> q5_view(rocks_db);
   B::Adapter<tpch::q10::q10_pipeline_view_t>        q10_view(rocks_db);
   B::Adapter<tpch::q10::q10_pipeline_view_preagg_t> q10_view_preagg(rocks_db);

   // COL pipeline (shared with Q3 / Q5).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col(rocks_db);
   B::Adapter<tpch::orders_coli_t>        split_orders(rocks_db);
   B::Adapter<tpch::lineitem_col_t>       split_lineitem(rocks_db);

   // S5: aCOL MI — per-order pre-aggregated COL merged index (no lineitems).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t>  q10_acol(rocks_db);

   // S6: COL-family shared materialised view — ONE table shared by Q3/Q5/Q10.
   B::Adapter<tpch::col_shared_view_t>  shared_view(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
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
      tpch::load_vanilla_family<B>(tpch, q3, q5, q10);
      return 0;
   }
   tpch.recover_last_ids();

   RocksDBTraits db_traits(rocks_db);

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
             rocks_db, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 2: {
         tpch::q10::ViewQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             rocks_db, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 3: {
         tpch::q10::MergedQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             rocks_db, std::move(w), tpch, "mi_col_walk");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 4: {
         tpch::q10::HashQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             rocks_db, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 5: {
         tpch::q10::AggregatedQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             rocks_db, std::move(w), tpch, "mi_acol_preagg");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 6: {
         tpch::q10::SharedViewQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             rocks_db, std::move(w), tpch, "shared_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         q10_txc = helper.tx_count();
         break;
      }
      case 7: {
         tpch::q10::PreaggViewQ10<B> w{q10};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(
             rocks_db, std::move(w), tpch, "preagg_view");
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
