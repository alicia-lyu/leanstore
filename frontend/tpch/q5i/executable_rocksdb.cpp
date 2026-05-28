// RocksDB entry point for Q5I workload.
//
// Q5I belongs to the TPCHi invoice-extended family cohort {Q3I, Q5I}; see
// q3i/executable_rocksdb.cpp for the family-loader rationale. Mounts the
// same image layout as q3i_lsm.

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
#include "../q10i/per_structure_workload.hpp"
#include "../q10i/workload.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q5I workload — RocksDB backend (TPCHi family)");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   // Q5I has no native S5/S7 path — those are Q10I-only foreground variants
   // in the TPCHi family cohort. Reject early with a clear message.
   if (FLAGS_storage_structure == 5 || FLAGS_storage_structure == 7) {
      std::cerr << "q5i_lsm does not support --storage_structure="
                << FLAGS_storage_structure
                << " (Q5I has no native S5/S7). Use q10i_lsm." << std::endl;
      return 1;
   }

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
   B::Adapter<region_t>      region_table(rocks_db);
   B::Adapter<invoice_t>     invoice(rocks_db);

   // Per-query views (family cohort: Q3I + Q5I + Q10I).
   B::Adapter<tpch::q3i::q3i_pipeline_view_t>           q3i_view(rocks_db);
   B::Adapter<tpch::q5i::q5i_pipeline_view_t>           q5i_view(rocks_db);
   B::Adapter<tpch::q10i::q10i_pipeline_view_t>         q10i_view(rocks_db);
   B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t>  q10i_view_preagg(rocks_db);

   // Shared COLI pipeline.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // aCOLI MI (Q3I-only at S5; declared to keep the column-family set
   // identical across the cohort).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli(rocks_db);

   // Q10I S5 aCOLI 2-type MI.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acoli_q10i_t> acoli_q10i(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHIWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                   orders, lineitem, nation, region_table, invoice, logger);
   tpch::q3i::Q3IWorkload<B> q3i(tpch, customer, orders, lineitem, invoice,
                                   q3i_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);
   tpch::q5i::Q5IWorkload<B> q5i(tpch, customer, orders, lineitem, invoice,
                                   supplier, nation, region_table,
                                   q5i_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);
   tpch::q10i::Q10IWorkload<B> q10i(tpch, customer, orders, lineitem, invoice,
                                     nation, q10i_view, merged_coli,
                                     split_orders, split_lineitem, split_invoice,
                                     acoli, q10i_view_preagg, acoli_q10i);

   if (!FLAGS_recover) {
      tpch::load_tpchi_family<B>(FLAGS_storage_structure, tpch, q3i, q5i, q10i);
      return 0;
   }
   tpch.recover_last_ids();

   RocksDBTraits db_traits(rocks_db);
   auto bg_catalog = FLAGS_bg_query_thread
                       ? tpch::register_tpchi_bg_catalog<B>(db_traits, tpch, q3i, q5i, q10i,
                                                          FLAGS_storage_structure,
                                                          FLAGS_bg_point_lookups)
                       : std::vector<tpch::BgCatalogFn>{};
   tpch::BgCatalogFn bg_lookup_step =
       (FLAGS_bg_query_thread && FLAGS_bg_point_lookups)
           ? tpch::make_tpchi_point_lookup_step<B>(db_traits, tpch)
           : tpch::BgCatalogFn{};

   using AggRow = tpch::q5i::q5i_agg_row_t;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q5i::BaseQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         break;
      }
      case 2: {
         tpch::q5i::ViewQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         break;
      }
      case 3: {
         tpch::q5i::MergedQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "mi_coli_walk");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         break;
      }
      case 4: {
         tpch::q5i::HashQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }

   return 0;
}
