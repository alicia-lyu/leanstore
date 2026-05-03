// RocksDB entry point for Q3I workload.
//
// Wires gflags, opens a RocksDB TransactionDB, declares the adapters Q3I
// needs, constructs the TPCHWorkload and Q3IWorkload, then dispatches to
// the chosen per-structure wrapper via tpch::TpchExecutableHelper.
//
// Phase 1 requires only --storage_structure=3 (query_by_merged) to fully
// work. Cases 1/2/4 load and run but query_by_* bodies return 0 (Phase 2).

#include <gflags/gflags.h>
#include <iomanip>
#include <iostream>

#include "../../shared/RocksDB.hpp"
#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
#include "../backend.hpp"
#include "../tpch_tables.hpp"
#include "../tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_executable_helper.hpp"

#include "../coli_pipeline.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3I workload — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // Base TPC-H tables
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);
   B::Adapter<invoice_t>   invoice(rocks_db);

   // Q3I-specific adapters
   B::Adapter<tpch::q3i::q3i_pipeline_view_t> pipeline_view(rocks_db);
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);

   // S1 custkey-sorted split indexes (populated by load for --storage_structure=1).
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // S5 aCOLI 2-type MI: customer_acoli_t + orders_acoli_t.
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_acoli_t> acoli(rocks_db);

   rocks_db.open();  // must be called after all adapters register their CFs

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, invoice, logger);
   tpch::q3i::Q3IWorkload<B> q3i(tpch, customer, orders, lineitem, invoice,
                                   pipeline_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);

   if (!FLAGS_recover) {
      q3i.load();
      return 0;
   }
   tpch.recover_last_ids();

   // Plumb per-query stats so the post-experiment summary can report
   // stage cardinalities and per-stage wall-clock. Counters accumulate
   // across all queries in the 15s window; divide by count for per-query.
   tpch::q3i::Q3IStats stats;
   q3i.stats = &stats;

   using AggRow = tpch::q3i::q3i_agg_row_t;
   long tx_count = 0;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q3i::BaseQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "base_merge_join");
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 2: {
         tpch::q3i::ViewQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "pipeline_view");
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 3: {
         tpch::q3i::MergedQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "mi_coli_walk");
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 4: {
         tpch::q3i::HashQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "base_hash_join");
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 5: {
         tpch::AggregatedStructure<tpch::q3i::Q3IWorkload<B>, AggRow> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(rocks_db, std::move(w), tpch, "acoli_aggregated");
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }

   // Print accumulated Q3I stats and per-query averages.
   // Counters are totals across all tx_count queries in the run window;
   // per-query averages normalise for the different TX/s across structures.
   auto per_q = [&](long total) -> double {
      return tx_count > 0 ? static_cast<double>(total) / tx_count : 0.0;
   };
   long total_stage_us = stats.stage_us_scan_filter + stats.stage_us_aggregator
                       + stats.stage_us_join + stats.stage_us_topN;

   std::cout << "\n[q3i] cardinality totals across all queries (tx=" << tx_count << "):"
             << "\n  customers_scanned       = " << stats.customers_scanned
             << "\n  customers_passing_filter= " << stats.customers_passing_filter
             << "\n  orders_scanned          = " << stats.orders_scanned
             << "\n  orders_passing_filter   = " << stats.orders_passing_filter
             << "\n  lineitems_scanned       = " << stats.lineitems_scanned
             << "\n  lineitems_passing_filter= " << stats.lineitems_passing_filter
             << "\n  invoices_scanned        = " << stats.invoices_scanned
             << "\n  invoices_passing_filter = " << stats.invoices_passing_filter
             << "\n  join1_output_rows       = " << stats.join1_output_rows
             << "\n  join2_output_rows       = " << stats.join2_output_rows
             << "\n  join3_output_rows       = " << stats.join3_output_rows
             << "\n  topN_candidates         = " << stats.topN_candidates
             << "\n  aggregator_rows_out     = " << stats.aggregator_rows_out
             << "\n  mi_records_visited      = " << stats.mi_records_visited
             << "\n  mi_groups_skipped       = " << stats.mi_groups_skipped
             << "\n[q3i] per-stage wall-clock totals (us) | total_us=" << total_stage_us << ":"
             << "\n  scan_filter             = " << stats.stage_us_scan_filter
             << "\n  aggregator              = " << stats.stage_us_aggregator
             << "\n  join                    = " << stats.stage_us_join
             << "\n  topN                    = " << stats.stage_us_topN
             << "\n[q3i] per-stage wall-clock per-query (us/query):"
             << "\n  scan_filter             = " << std::fixed << std::setprecision(1) << per_q(stats.stage_us_scan_filter)
             << "\n  aggregator              = " << per_q(stats.stage_us_aggregator)
             << "\n  join                    = " << per_q(stats.stage_us_join)
             << "\n  topN                    = " << per_q(stats.stage_us_topN)
             << "\n  total                   = " << per_q(total_stage_us)
             << "\n[q3i] stage attribution:"
             << "\n  S1/S3/S4 fuse scan + filter + aggregate into the join chain and attribute"
             << "\n  that work to stage_us_join. S2 has no query-time aggregator (pre-materialised"
             << "\n  view); its stage_us_scan_filter covers the view scan + per-row filters."
             << "\n  Compare per-query averages, not totals — totals reflect the run window,"
             << "\n  not per-query cost."
             << std::endl;
   return 0;
}
