// RocksDB entry point for Q3 workload.
//
// Q3 belongs to the vanilla TPC-H family cohort {Q3, Q5}; both queries use
// the COL pipeline so a single loaded image can serve either binary as
// foreground. This executable declares the family-union adapter set, loads
// every family member's secondaries, and registers a bg-query cohort so
// --bg_query_thread=true cycles Q3 and Q5 at the same --storage_structure
// as the foreground. Q12 is excluded from the cohort because it uses a
// different pipeline (OL merged index over orders + lineitem).

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

#include "../q5/per_structure_workload.hpp"
#include "../q5/workload.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3 workload — RocksDB backend (vanilla family)");
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

   // Per-query views (one for Q3, one for Q5 — family-cohort).
   B::Adapter<tpch::q3::q3_pipeline_view_t> q3_view(rocks_db);
   B::Adapter<tpch::q5::q5_pipeline_view_t> q5_view(rocks_db);

   // Shared COL pipeline: one MI + two split adapters, used by both Q3 and Q5.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col(rocks_db);
   B::Adapter<tpch::orders_coli_t>        split_orders(rocks_db);
   B::Adapter<tpch::lineitem_col_t>       split_lineitem(rocks_db);

   // S6: COL-family shared materialised view — ONE table shared by Q3 and Q5
   // (same record id ⇒ same physical key space, like merged_col).
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

   if (!FLAGS_recover) {
      tpch::load_vanilla_family<B>(tpch, q3, q5);
      return 0;
   }
   tpch.recover_last_ids();

   // db_traits is constructed inside TpchExecutableHelper; we need a stable
   // reference to it from the bg-step closures, so build a single instance
   // here and reuse it for both the helper and the bg registry.
   RocksDBTraits db_traits(rocks_db);

   using AggRow = tpch::q3::q3_agg_row_t;
   auto bg_steps = FLAGS_bg_query_thread
                       ? tpch::register_vanilla_bg_steps<B>(db_traits, tpch, q3, q5,
                                                            FLAGS_storage_structure,
                                                            FLAGS_bg_point_lookups)
                       : std::vector<tpch::BgStepFn>{};

   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q3::BaseQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      case 2: {
         tpch::q3::ViewQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      case 3: {
         tpch::q3::MergedQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "mi_col_walk");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      case 4: {
         tpch::q3::HashQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_steps(std::move(bg_steps));
         helper.run();
         break;
      }
      case 6: {
         tpch::q3::SharedViewQ3<B> w{q3};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "shared_view");
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
