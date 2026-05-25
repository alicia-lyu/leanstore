#ifndef ROCKSDB_ONLY
// Phase 5 harness for Q10 — LeanStore backend.
//
// Mirrors test_query_q10_rocksdb.cpp: loads base tables + COL secondaries
// once, runs all four query_by_* paths under per-path Q10Stats, and asserts
// strict 4-way XOR parity at TWO distinct param sets (iter=0 default,
// iter=1 off-default for the param-bake regression guard per PLAYBOOK §10).
//
// Differences from the RocksDB harness:
//   - No filesystem wipe of --ssd_path; LeanStore opens fresh each run.
//   - All work runs inside crm.scheduleJobSync(0, ...) closures.

#include <gflags/gflags.h>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../../backend.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../q10/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

// XOR digest — byte-identical to the RocksDB harness so digests compare
// across backends. Covers c_custkey, revenue, and the 6 FD output cols +
// n_name so Phase 4 cross-structure parity catches both revenue and
// FD-col mismatches.
static uint64_t row_digest(const tpch::q10::q10_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   auto var_hash = [](const char* p, size_t n) {
      return std::hash<std::string>{}(std::string(p, strnlen(p, n)));
   };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.c_custkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   d = rotl64(d, 13) ^ var_hash(r.c_name.data,    sizeof(r.c_name.data));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.c_acctbal) * 1e6);
   d = rotl64(d, 13) ^ var_hash(r.n_name.data,    sizeof(r.n_name.data));
   d = rotl64(d, 13) ^ var_hash(r.c_address.data, sizeof(r.c_address.data));
   d = rotl64(d, 13) ^ var_hash(r.c_phone.data,   sizeof(r.c_phone.data));
   d = rotl64(d, 13) ^ var_hash(r.c_comment.data, sizeof(r.c_comment.data));
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q10::q10_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q10 unified query test — LeanStore backend");
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

   B::Adapter<tpch::q10::q10_pipeline_view_t>        pipeline_view;
   B::Adapter<tpch::q10::q10_pipeline_view_preagg_t> pipeline_view_preagg;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col;
   B::Adapter<tpch::orders_coli_t>        split_orders;
   B::Adapter<tpch::lineitem_col_t>       split_lineitem;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t>  acol;  // S5 aCOL MI

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
      pipeline_view  = B::Adapter<tpch::q10::q10_pipeline_view_t>(db, "q10_pipeline_view");
      pipeline_view_preagg = B::Adapter<tpch::q10::q10_pipeline_view_preagg_t>(db, "q10_pipeline_view_preagg");
      merged_col     = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                        tpch::lineitem_col_t>(db, "col_merged");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "col_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "col_split_lineitem");
      acol           = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t>(db, "acol_merged");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q10::Q10Workload<B> q10(tpch, customer, orders, lineitem, nation,
                                  pipeline_view, pipeline_view_preagg, merged_col,
                                  split_orders, split_lineitem, acol);

   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q10.load();
      leanstore::cr::Worker::my().commitTX();
   });

   // Per-iter run helper: invokes all four query_by_* paths under per-path
   // Q10Stats, asserts strict 4-way parity, returns diagnostics. Called
   // once per param iter (default + iter=1 off-default) for param-bake
   // guard (PLAYBOOK §10).
   auto print_digest = [](const char* name, size_t n, uint64_t d) {
      std::cout << std::left << std::setw(14) << name
                << " rows=" << std::setw(4) << n
                << " digest=0x" << std::hex << d << std::dec << "\n";
   };

   struct IterResult {
      bool   ok            = false;
      bool   s3_nonzero    = false;
      uint64_t d_merged    = 0;   // S3 reference digest (for the preagg A/B)
      tpch::q10::Q10Stats s3_stats{};
      tpch::q10::Q10Stats s4_stats{};
   };

   auto run_iter = [&](long iter, const char* tag) -> IterResult {
      IterResult res;
      std::vector<tpch::q10::q10_agg_row_t> r_base, r_view, r_merged, r_hash;
      tpch::q10::Q10Stats s1_stats{}, s2_stats{}, s3_stats{}, s4_stats{};

      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
         q10.set_params_for_iter(iter);
         q10.stats = &s1_stats; q10.query_by_base  (r_base);
         q10.stats = &s2_stats; q10.query_by_view  (r_view);
         q10.stats = &s3_stats; q10.query_by_merged(r_merged);
         q10.stats = &s4_stats; q10.query_by_hash  (r_hash);
         q10.stats = nullptr;
         leanstore::cr::Worker::my().commitTX();
      });

      uint64_t d_base   = digest_rows(r_base);
      uint64_t d_view   = digest_rows(r_view);
      uint64_t d_merged = digest_rows(r_merged);
      uint64_t d_hash   = digest_rows(r_hash);

      std::cout << "\n--- Results " << tag << " ---\n";
      print_digest("S1 (base)",   r_base.size(),   d_base);
      print_digest("S2 (view)",   r_view.size(),   d_view);
      print_digest("S3 (merged)", r_merged.size(), d_merged);
      print_digest("S4 (hash)",   r_hash.size(),   d_hash);

      bool s3_nonzero = (d_merged != 0);
      bool s1_ok = (d_base == d_merged) && (r_base.size() == r_merged.size());
      bool s2_ok = (d_view == d_merged) && (r_view.size() == r_merged.size());
      bool s4_ok = (d_hash == d_merged) && (r_hash.size() == r_merged.size());
      std::cout << (s3_nonzero ? "[OK]   " : "[FAIL] ") << "S3 sanity " << tag << "\n";
      std::cout << (s1_ok ? "[OK]   " : "[FAIL] ") << "S1 vs S3 " << tag << "\n";
      std::cout << (s2_ok ? "[OK]   " : "[FAIL] ") << "S2 vs S3 " << tag << "\n";
      std::cout << (s4_ok ? "[OK]   " : "[FAIL] ") << "S4 vs S3 " << tag << "\n";

      res.ok         = s3_nonzero && s1_ok && s2_ok && s4_ok;
      res.s3_nonzero = s3_nonzero;
      res.d_merged   = d_merged;
      res.s3_stats   = s3_stats;
      res.s4_stats   = s4_stats;
      return res;
   };

   IterResult iter0 = run_iter(0, "[iter=0]");
   IterResult iter1 = run_iter(1, "[iter=1]");

   const auto& s3_stats = iter0.s3_stats;
   const auto& s4_stats = iter0.s4_stats;

   std::cout << "\n=== Q10Stats post-S3 (iter=0) ===\n";
   std::cout << "[stat] customers_scanned            = " << s3_stats.customers_scanned << "\n";
   std::cout << "[stat] orders_scanned               = " << s3_stats.orders_scanned << "\n";
   std::cout << "[stat] orders_passing_date          = " << s3_stats.orders_passing_date << "\n";
   std::cout << "[stat] lineitems_scanned            = " << s3_stats.lineitems_scanned << "\n";
   std::cout << "[stat] lineitems_passing_returnflag = " << s3_stats.lineitems_passing_returnflag << "\n";
   std::cout << "[stat] aggregator_rows_out          = " << s3_stats.aggregator_rows_out << "\n";
   std::cout << "[stat] topn_offers                  = " << s3_stats.topn_offers << "\n";
   std::cout << "[stat] topn_evictions               = " << s3_stats.topn_evictions << "\n";
   std::cout << "[stat] nation_inl_lookups           = " << s3_stats.nation_inl_lookups << "\n";

   std::cout << "\n=== Q10Stats post-S4 (iter=0) ===\n";
   std::cout << "[stat] orders_inl_lookups           = " << s4_stats.orders_inl_lookups << "\n";
   std::cout << "[stat] customer_inl_lookups         = " << s4_stats.customer_inl_lookups << "\n";

   // ------------------------------------------------------------------
   // S2 variant B (per-order pre-aggregated view) A/B parity. Both views
   // live in this one image; flip --q10_view_variant=preagg and re-run S2,
   // asserting it still matches S3 at both param iters.
   std::cout << "\n=== S2 variant B (per-order preagg view) ===\n";
   FLAGS_q10_view_variant = "preagg";
   bool preagg_ok = true;
   for (long iter : {0L, 1L}) {
      std::vector<tpch::q10::q10_agg_row_t> r_preagg;
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
         q10.set_params_for_iter(iter);
         tpch::q10::Q10Stats sp{};
         q10.stats = &sp; q10.query_by_view(r_preagg); q10.stats = nullptr;
         leanstore::cr::Worker::my().commitTX();
      });
      uint64_t d_preagg = digest_rows(r_preagg);
      uint64_t d_ref    = (iter == 0) ? iter0.d_merged : iter1.d_merged;
      bool ok = (d_preagg == d_ref);
      preagg_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ")
                << "S2-preagg vs S3 [iter=" << iter << "] digest=0x"
                << std::hex << d_preagg << std::dec << " rows=" << r_preagg.size()
                << " (S3 digest=0x" << std::hex << d_ref << std::dec << ")\n";
   }
   FLAGS_q10_view_variant = "lineitem";  // restore default

   // ------------------------------------------------------------------
   // S3 variant B (physical SkipOrder seek) A/B parity. Flip
   // --skip_order_physical=1 and re-run S3 (col_group_walk), asserting it
   // still matches the logical S3 at both param iters.
   std::cout << "\n=== S3 variant B (physical SkipOrder seek) ===\n";
   std::cout << "[info] S3-logical mi_records_visited (iter=0) = "
             << iter0.s3_stats.mi_records_visited << "\n";
   FLAGS_skip_order_physical = 1;
   bool skip_phys_ok = true;
   for (long iter : {0L, 1L}) {
      std::vector<tpch::q10::q10_agg_row_t> r_phys;
      tpch::q10::Q10Stats sp{};
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
         q10.set_params_for_iter(iter);
         q10.stats = &sp; q10.query_by_merged(r_phys); q10.stats = nullptr;
         leanstore::cr::Worker::my().commitTX();
      });
      uint64_t d_phys = digest_rows(r_phys);
      uint64_t d_ref  = (iter == 0) ? iter0.d_merged : iter1.d_merged;
      bool ok = (d_phys == d_ref);
      skip_phys_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ")
                << "S3-physical vs S3-logical [iter=" << iter << "] digest=0x"
                << std::hex << d_phys << std::dec << " rows=" << r_phys.size()
                << " mi_visited=" << sp.mi_records_visited << "\n";
   }
   FLAGS_skip_order_physical = -1;  // restore default

   // S5 (aCOL MI — per-order pre-aggregated, hand-rolled acol_group_walk) A/B
   // parity vs the S3 reference at both param iters.
   std::cout << "\n=== S5 (aCOL MI, hand-rolled walk) ===\n";
   // .size() touches the btree → must run inside a worker (cr::Worker::my()).
   double acol_sz = 0, col_sz = 0;
   crm.scheduleJobSync(0, [&]() { acol_sz = acol.size(); col_sz = merged_col.size(); });
   std::cout << "[info] aCOL MI size=" << std::fixed << std::setprecision(3)
             << acol_sz << " MiB  (S3 merged_col=" << col_sz << " MiB)\n";
   bool acol_ok = true;
   for (long iter : {0L, 1L}) {
      std::vector<tpch::q10::q10_agg_row_t> r_acol;
      tpch::q10::Q10Stats sa{};
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
         q10.set_params_for_iter(iter);
         q10.stats = &sa; q10.query_by_aggregated(r_acol); q10.stats = nullptr;
         leanstore::cr::Worker::my().commitTX();
      });
      uint64_t d_acol = digest_rows(r_acol);
      uint64_t d_ref  = (iter == 0) ? iter0.d_merged : iter1.d_merged;
      bool ok = (d_acol == d_ref);
      acol_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ")
                << "S5-acol vs S3 [iter=" << iter << "] digest=0x"
                << std::hex << d_acol << std::dec << " rows=" << r_acol.size()
                << " mi_visited=" << sa.mi_records_visited << "\n";
   }

   if (!iter0.ok) std::cout << "[FAIL] iter=0 parity / S3 sanity failed\n";
   if (!iter1.ok) std::cout << "[FAIL] iter=1 parity / S3 sanity failed — "
                                "suggests a param-bake regression (some path "
                                "hardcoded the iter=0 date)\n";
   if (!preagg_ok)    std::cout << "[FAIL] S2-preagg (variant B) parity vs S3 failed\n";
   if (!skip_phys_ok) std::cout << "[FAIL] S3-physical (SkipOrder seek) parity vs S3-logical failed\n";
   if (!acol_ok)      std::cout << "[FAIL] S5-acol (aCOL MI) parity vs S3 failed\n";
   return (iter0.ok && iter1.ok && preagg_ok && skip_phys_ok && acol_ok) ? 0 : 1;
}

#endif  // ROCKSDB_ONLY
