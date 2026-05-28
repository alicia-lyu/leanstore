// RocksDB entry point for Q5 workload.
//
// Q5 belongs to the vanilla TPC-H family cohort {Q3, Q5}; see
// q3/executable_rocksdb.cpp header for the family-loader rationale. This
// binary mounts the same image layout as q3_lsm; both can serve as
// foreground for the family load.

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
#include "../q10/per_structure_workload.hpp"
#include "../q10/workload.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q5 workload — RocksDB backend (vanilla family)");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   // Q5 has no native S5/S7 path — those are Q10-only foreground variants
   // in the vanilla family cohort. Reject early with a clear message.
   if (FLAGS_storage_structure == 5 || FLAGS_storage_structure == 7) {
      std::cerr << "q5_lsm does not support --storage_structure="
                << FLAGS_storage_structure
                << " (Q5 has no native S5/S7). Use q10_lsm." << std::endl;
      return 1;
   }

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

   // Shared COL pipeline.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col(rocks_db);
   B::Adapter<tpch::orders_coli_t>        split_orders(rocks_db);
   B::Adapter<tpch::lineitem_col_t>       split_lineitem(rocks_db);

   // Q10 S5 aCOL MI (orders_acol_t + customer_coli_t).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t> q10_acol(rocks_db);

   // S6: COL-family shared materialised view — ONE table shared by Q3/Q5/Q10.
   B::Adapter<tpch::col_shared_view_t>    shared_view(rocks_db);

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
                                  q10_view, q10_view_preagg,
                                  merged_col, split_orders, split_lineitem,
                                  q10_acol, shared_view);

   if (!FLAGS_recover) {
      tpch::load_vanilla_family<B>(FLAGS_storage_structure, tpch, q3, q5, q10);
      return 0;
   }
   tpch.recover_last_ids();

   RocksDBTraits db_traits(rocks_db);

   using AggRow = tpch::q5::q5_agg_row_t;
   auto bg_catalog = FLAGS_bg_query_thread
                       ? tpch::register_vanilla_bg_catalog<B>(db_traits, tpch, q3, q5, q10,
                                                            FLAGS_storage_structure,
                                                            FLAGS_bg_point_lookups)
                       : std::vector<tpch::BgCatalogFn>{};

   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q5::BaseQ5<B> w{q5};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 2: {
         tpch::q5::ViewQ5<B> w{q5};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 3: {
         tpch::q5::MergedQ5<B> w{q5};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "mi_col_walk");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 4: {
         tpch::q5::HashQ5<B> w{q5};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.run();
         break;
      }
      case 6: {
         tpch::q5::SharedViewQ5<B> w{q5};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "shared_view");
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
