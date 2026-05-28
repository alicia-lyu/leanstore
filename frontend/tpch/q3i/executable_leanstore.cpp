#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q3I workload — TPCHi family cohort {Q3I, Q5I}.
// See executable_rocksdb.cpp for the family-loader rationale.

#include <gflags/gflags.h>
#include <iomanip>
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
#include "../q5i/per_structure_workload.hpp"
#include "../q5i/workload.hpp"
#include "../q10i/per_structure_workload.hpp"
#include "../q10i/workload.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3I workload — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   // Q3I has no native S5/S7 path — those are Q10I-only foreground variants
   // in the TPCHi family cohort. Reject early with a clear message.
   if (FLAGS_storage_structure == 5 || FLAGS_storage_structure == 7) {
      std::cerr << "q3i_btree does not support --storage_structure="
                << FLAGS_storage_structure
                << " (Q3I has no native S5/S7). Use q10i_btree." << std::endl;
      return 1;
   }

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   // Base TPC-H tables (default-constructed; assigned inside crm callback).
   B::Adapter<part_t>      part;
   B::Adapter<supplier_t>  supplier;
   B::Adapter<partsupp_t>  partsupp;
   B::Adapter<customerh_t> customer;
   B::Adapter<orders_t>    orders;
   B::Adapter<lineitem_i_t>  lineitem;
   B::Adapter<nation_t>    nation;
   B::Adapter<region_t>    region;
   B::Adapter<invoice_t>   invoice;

   // Per-query views (family cohort: Q3I + Q5I + Q10I).
   B::Adapter<tpch::q3i::q3i_pipeline_view_t>           q3i_view;
   B::Adapter<tpch::q5i::q5i_pipeline_view_t>           q5i_view;
   B::Adapter<tpch::q10i::q10i_pipeline_view_t>         q10i_view;
   B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t>  q10i_view_preagg;

   // Shared COLI pipeline.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli;
   B::Adapter<tpch::orders_coli_t>   split_orders;
   B::Adapter<tpch::lineitem_coli_t> split_lineitem;
   B::Adapter<tpch::invoice_coli_t>  split_invoice;

   // S5 aCOLI MI (Q3I-only at S5; declared to keep CF set identical).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_acoli_t,
                    tpch::lineitem_acoli_t> acoli;

   // Q10I S5 aCOLI 2-type MI.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acoli_q10i_t> acoli_q10i;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part              = B::Adapter<part_t>(db, "part");
      supplier          = B::Adapter<supplier_t>(db, "supplier");
      partsupp          = B::Adapter<partsupp_t>(db, "partsupp");
      customer          = B::Adapter<customerh_t>(db, "customer");
      orders            = B::Adapter<orders_t>(db, "orders");
      lineitem          = B::Adapter<lineitem_i_t>(db, "lineitem");
      nation            = B::Adapter<nation_t>(db, "nation");
      region            = B::Adapter<region_t>(db, "region");
      invoice           = B::Adapter<invoice_t>(db, "invoice");
      q3i_view          = B::Adapter<tpch::q3i::q3i_pipeline_view_t>(db, "q3i_pipeline_view");
      q5i_view          = B::Adapter<tpch::q5i::q5i_pipeline_view_t>(db, "q5i_pipeline_view");
      q10i_view         = B::Adapter<tpch::q10i::q10i_pipeline_view_t>(db, "q10i_pipeline_view");
      q10i_view_preagg  = B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t>(db, "q10i_pipeline_view_preagg");
      // Family-shared COLI pipeline names (q3i_* / q5i_* prefixes dropped).
      merged_coli       = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                           tpch::lineitem_coli_t, tpch::invoice_coli_t>(db, "coli_merged");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "coli_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_coli_t>(db, "coli_split_lineitem");
      split_invoice  = B::Adapter<tpch::invoice_coli_t>(db, "coli_split_invoice");
      acoli          = B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_acoli_t,
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

   tpch::q3i::Q3IStats stats;
   q3i.stats      = &stats;
   q3i.micro_perf = FLAGS_micro_perf;

   LeanStoreTraits db_traits(crm);
   auto bg_catalog = FLAGS_bg_query_thread
                       ? tpch::register_tpchi_bg_catalog<B>(db_traits, tpch, q3i, q5i, q10i,
                                                          FLAGS_storage_structure,
                                                          FLAGS_bg_point_lookups)
                       : std::vector<tpch::BgCatalogFn>{};
   tpch::BgCatalogFn bg_lookup_step =
       (FLAGS_bg_query_thread && FLAGS_bg_point_lookups)
           ? tpch::make_tpchi_point_lookup_step<B>(db_traits, tpch)
           : tpch::BgCatalogFn{};

   using AggRow = tpch::q3i::q3i_agg_row_t;
   long tx_count = 0;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q3i::BaseQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(crm, std::move(w), tpch, "base_merge_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 2: {
         tpch::q3i::ViewQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(crm, std::move(w), tpch, "pipeline_view");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 3: {
         tpch::q3i::MergedQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(crm, std::move(w), tpch, "mi_coli_walk");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 4: {
         tpch::q3i::HashQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(crm, std::move(w), tpch, "base_hash_join");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      case 5: {
         tpch::AggregatedStructure<tpch::q3i::Q3IWorkload<B>, AggRow> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter, lineitem_i_t> helper(crm, std::move(w), tpch, "acoli_aggregated");
         helper.set_bg_query_catalog(std::move(bg_catalog));
         helper.set_bg_lookup_step(bg_lookup_step);
         helper.run();
         tx_count = helper.tx_count();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }

   auto per_q = [&](long total) -> double {
      return tx_count > 0 ? static_cast<double>(total) / tx_count : 0.0;
   };
   long total_stage_us = stats.stage_us_scan_filter + stats.stage_us_aggregator
                       + stats.stage_us_join + stats.stage_us_topN;

   std::cout << "\n[q3i] coli_walker_variant=" << FLAGS_coli_walker_variant << "\n";
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
             << "\n  bj_groups_skipped       = " << stats.bj_groups_skipped
             << "\n  bj_ord_skips            = " << stats.bj_ord_skips
             << "\n  bj_lin_skips            = " << stats.bj_lin_skips
             << "\n  hj_groups_skipped       = " << stats.hj_groups_skipped
             << "\n  acoli_customers_scanned = " << stats.acoli_customers_scanned
             << "\n  acoli_customers_passing = " << stats.acoli_customers_passing_filter
             << "\n  acoli_orders_scanned    = " << stats.acoli_orders_scanned
             << "\n  acoli_orders_emitted    = " << stats.acoli_orders_emitted
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

   // --micro_perf block: LeanStore WorkerCounters totals + chrono-instrumented
   // scanner_perf::iter_next_ns. Mapped to A1 metrics in PERFORMANCE.md §3 A1
   // (Linux q3i_btree result). Field semantics:
   //   tuples_advanced/q  ≈ user_key_comparison_count/q (proxy)
   //   bytes_read/q       ≈ block_read_byte/q
   //   hit_rate           ≈ block_cache_hit_rate
   //   iter_next_cpu/q    ≈ iter_next_cpu_nanos/q (chrono-instrumented)
   if (FLAGS_micro_perf) {
      auto per_q_u64 = [&](uint64_t total) -> double {
         return tx_count > 0 ? static_cast<double>(total) / tx_count : 0.0;
      };
      auto ns_to_ms = [](uint64_t ns) -> double { return static_cast<double>(ns) / 1e6; };
      auto b_to_mib = [](uint64_t b)  -> double { return static_cast<double>(b)  / (1024.0 * 1024.0); };

      const uint64_t hits     = stats.ls_hot_hit + stats.ls_cold_hit;
      const uint64_t misses   = stats.ls_dt_page_reads;
      const uint64_t total_acc = hits + misses;
      const double   hit_rate = total_acc > 0
                                ? 100.0 * static_cast<double>(hits) / static_cast<double>(total_acc)
                                : 0.0;
      const uint64_t bytes_read =
          stats.ls_dt_page_reads * leanstore::storage::EFFECTIVE_PAGE_SIZE;

      std::cout << std::fixed << std::setprecision(3)
                << "\n[q3i] leanstore perf-context totals (tx=" << tx_count << "):"
                << "\n  tuples_advanced (Σ dt_next_tuple) = " << stats.ls_dt_next_tuple
                << "\n  page_reads     (Σ dt_page_reads)  = " << stats.ls_dt_page_reads
                << "\n  bytes_read (page_reads*page_size) = " << b_to_mib(bytes_read) << " MiB"
                << "\n  hot_hit                           = " << stats.ls_hot_hit
                << "\n  cold_hit                          = " << stats.ls_cold_hit
                << "\n  iter_next_cpu (chrono)            = " << ns_to_ms(stats.ls_iter_next_ns) << " ms"
                << "\n  iter_next_calls                   = " << stats.ls_iter_next_calls
                << "\n[q3i] leanstore perf-context per-query averages (tx=" << tx_count << "):"
                << "\n  tuples_advanced/q                 = " << std::setprecision(1) << per_q_u64(stats.ls_dt_next_tuple)
                << "\n  block_cache_hit_rate              = " << hit_rate << "%"
                << "\n  page_reads/q                      = " << per_q_u64(stats.ls_dt_page_reads)
                << "\n  bytes_read/q                      = " << std::setprecision(3)
                                                              << per_q_u64(bytes_read) / 1024.0 << " KiB"
                << "\n  iter_next_cpu/q                   = " << per_q_u64(stats.ls_iter_next_ns) / 1e3 << " us"
                << "\n  iter_next_calls/q                 = " << std::setprecision(0) << per_q_u64(stats.ls_iter_next_calls)
                << std::endl;
   }

   // --cfstats: LeanStore has no per-CF concept. Emit a one-line note so the
   // diagnostic column is filled out for cross-backend comparison; H8
   // (shared-DB cache pollution) tracks the LeanStore-side question.
   if (FLAGS_cfstats) {
      std::cout << "\n[q3i] leanstore buffer-pool snapshot (--cfstats):"
                << "\n  (no per-CF analog on LeanStore; each adapter is a separate B-tree"
                << "\n   competing for the same buffer pool — see H8 in PERFORMANCE.md)"
                << std::endl;
   }

   return 0;
}

#endif  // ROCKSDB_ONLY
