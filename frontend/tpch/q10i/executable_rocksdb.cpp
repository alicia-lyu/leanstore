// RocksDB entry point for Q10I workload (Returned Item Reporting × Invoice).
//
// Q10I is COLI-pipeline-based (CUSTOMER × ORDERS × LINEITEM × INVOICE merged
// index), extending Q10's COL chain with the invoice join. NATION attaches as
// a per-customer INL on PK at emit (D9) — no NATION sidetable hashmap.
//
// Q10I is standalone: not plugged into the Q3I+Q5I family cohort loader.
// The COLI image layout is shared across Q3I / Q5I / Q10I — all four
// adapters (merged_coli, split_orders, split_lineitem, split_invoice, acoli)
// must be declared to keep the column-family set identical so the image is
// mountable by any of the three queries.
//
// Phase 1 commit 1: standalone executable; query_by_* bodies are stubs.

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

   // Q10I-specific view.
   B::Adapter<tpch::q10i::q10i_pipeline_view_t> q10i_view(rocks_db);

   // Shared COLI pipeline (image-compatible with Q3I / Q5I).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // aCOLI MI (Q3I S5 — declared to keep column-family set image-compatible).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHIWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                   orders, lineitem, nation, region, invoice, logger);
   tpch::q10i::Q10IWorkload<B> q10i(tpch, customer, orders, lineitem, invoice,
                                     nation, q10i_view, merged_coli,
                                     split_orders, split_lineitem, split_invoice,
                                     acoli);

   if (!FLAGS_recover) {
      q10i.load();
      return 0;
   }
   tpch.recover_last_ids();

   using AggRow = tpch::q10i::q10i_agg_row_t;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q10i::BaseQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "base_merge_join");
         helper.run();
         break;
      }
      case 2: {
         tpch::q10i::ViewQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "pipeline_view");
         helper.run();
         break;
      }
      case 3: {
         tpch::q10i::MergedQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "mi_coli_walk");
         helper.run();
         break;
      }
      case 4: {
         tpch::q10i::HashQ10I<B> w{q10i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(
             rocks_db, std::move(w), tpch, "base_hash_join");
         helper.run();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }
   return 0;
}
