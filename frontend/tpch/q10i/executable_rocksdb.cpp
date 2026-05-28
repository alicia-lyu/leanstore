// RocksDB entry point for Q10I workload (Returned Item Reporting × Invoice).
//
// Q10I is COLI-pipeline-based (CUSTOMER × ORDERS × LINEITEM × INVOICE merged
// index), extending Q10's COL chain with the invoice join. NATION attaches as
// a per-customer INL on PK at emit (D9) — no NATION sidetable hashmap.
//
// Q10I is part of the TPCHi invoice-extended family cohort {Q3I, Q5I, Q10I}.
// It mounts the same image layout as q3i_lsm / q5i_lsm and registers a
// bg-query cohort so --bg_query_thread=true cycles all three at the same
// --storage_structure as the foreground. Q10I owns the native S5 (aCOLI MI)
// and S7 (per-order preagg view) variants — Q3I/Q5I fall back to S3/S2 in
// those cohort positions.

#include <gflags/gflags.h>
#include <iostream>

#include "../../shared/RocksDB.hpp"
#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
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

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q10I workload — RocksDB backend (COLI family)");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // Base TPC-H tables (lineitem uses invoice-extended variant).
   B::Adapter<part_t>        part(rocks_db);
   B::Adapter<supplier_t>    supplier(rocks_db);
   B::Adapter<partsupp_t>    partsupp(rocks_db);
   B::Adapter<customerh_t>   customer(rocks_db);
   B::Adapter<orders_t>      orders(rocks_db);
   B::Adapter<lineitem_i_t>  lineitem(rocks_db);
   B::Adapter<nation_t>      nation(rocks_db);
   B::Adapter<region_t>      region(rocks_db);
   B::Adapter<invoice_t>     invoice(rocks_db);

   // Per-query views (family cohort: Q3I + Q5I + Q10I).
   B::Adapter<tpch::q3i::q3i_pipeline_view_t>           q3i_view(rocks_db);
   B::Adapter<tpch::q5i::q5i_pipeline_view_t>           q5i_view(rocks_db);
   B::Adapter<tpch::q10i::q10i_pipeline_view_t>         q10i_view(rocks_db);
   B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t>  q10i_view_preagg(rocks_db);

   // Shared COLI pipeline (image-compatible with Q3I / Q5I).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // aCOLI MI (Q3I S5 — declared to keep column-family set image-compatible).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli(rocks_db);

   // Q10I S5 aCOLI 2-type MI.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acoli_q10i_t> acoli_q10i(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
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
      tpch::load_tpchi_family<B>(tpch, q3i, q5i, q10i);
      return 0;
   }
   tpch.recover_last_ids();

   RocksDBTraits db_traits(rocks_db);

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
             rocks_db, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 2: {
         tpch::q10i::ViewQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 3: {
         tpch::q10i::MergedQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "mi_coli_walk");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 4: {
         tpch::q10i::HashQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 5: {
         tpch::q10i::AggregatedQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "mi_acoli_preagg");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 7: {
         tpch::q10i::PreaggViewQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "preagg_view");
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
