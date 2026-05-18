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
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q5I workload — RocksDB backend (TPCHi family)");
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
   B::Adapter<region_t>      region_table(rocks_db);
   B::Adapter<invoice_t>     invoice(rocks_db);

   // Per-query views (family cohort: Q3I + Q5I).
   B::Adapter<tpch::q3i::q3i_pipeline_view_t> q3i_view(rocks_db);
   B::Adapter<tpch::q5i::q5i_pipeline_view_t> q5i_view(rocks_db);

   // Shared COLI pipeline.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // aCOLI MI (Q3I-only at S5; declared to keep the column-family set
   // identical across q3i_lsm and q5i_lsm so the image is mountable by either).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli(rocks_db);

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

   if (!FLAGS_recover) {
      tpch::load_tpchi_family<B>(tpch, q3i, q5i);
      return 0;
   }
   tpch.recover_last_ids();

   RocksDBTraits db_traits(rocks_db);
   auto bg_steps = FLAGS_bg_query_thread
                       ? tpch::register_tpchi_bg_steps<B>(db_traits, tpch, q3i, q5i,
                                                          FLAGS_storage_structure,
                                                          FLAGS_bg_point_lookups)
                       : std::vector<tpch::BgStepFn>{};

   using AggRow = tpch::q5i::q5i_agg_row_t;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q5i::BaseQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      case 2: {
         tpch::q5i::ViewQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      case 3: {
         tpch::q5i::MergedQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "mi_coli_walk");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      case 4: {
         tpch::q5i::HashQ5I<B> w{q5i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(rocks_db, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }

   return 0;
}
