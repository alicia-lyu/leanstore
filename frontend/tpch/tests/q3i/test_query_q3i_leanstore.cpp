#ifndef ROCKSDB_ONLY
// Unified Q3I parity harness — LeanStore backend.
//
// Mirrors test_query_q3i_rocksdb.cpp end-to-end: loads ALL secondary
// structures once, then runs all five query_by_* paths and asserts
// cross-structure result parity (XOR digest), shape, and Q3IStats
// cardinality consistency.
//
// Differences from the RocksDB harness:
//   - No filesystem wipe of --ssd_path: LeanStore's KV store opens the
//     ssd_path file fresh each run by default; recovery only happens with
//     --recover, which this test never sets.
//   - No raw-byte / per-CF metadata reporting: LeanStore has no column
//     families, so the "raw_bytes_in_cf" / "[overhead]" / "[content/row]"
//     blocks from the RocksDB harness are dropped. Logical correctness is
//     covered by the digest, shape, and Q3IStats checks.
//   - All work runs inside crm.scheduleJobSync(0, ...) closures with
//     INSTANTLY_VISIBLE_BULK_INSERT for loads and OLAP for queries, matching
//     test_query_q12_leanstore.cpp.
//
// Usage:
//   ./build/frontend/test_query_q3i_btree \
//       --ssd_path=/mnt/ssd/q3i_btree \
//       --tpch_scale_factor=1
//
// Expected: 5 identical digests, 5 identical row counts (in (0, 10]), exit 0.

#include <gflags/gflags.h>
#include <algorithm>
#include <chrono>
#include <climits>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

#include "../../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../../backend.hpp"
#include "../../tpchi_family/coli_pipeline.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpchi_family/tpchi_workload.hpp"
#include "../../q3i/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check). Identical to
// the RocksDB harness — keeping byte-exact so digests are comparable across
// backends at the same SF.

static uint64_t row_digest(const tpch::q3i::q3i_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderdate));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_shippriority));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.cust_open_due) * 1e6);
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q3i::q3i_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3I unified query test — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   // All 8 TPC-H base tables + invoice.
   B::Adapter<part_t>      part;
   B::Adapter<supplier_t>  supplier;
   B::Adapter<partsupp_t>  partsupp;
   B::Adapter<customerh_t> customer;
   B::Adapter<orders_t>    orders;
   B::Adapter<lineitem_i_t>  lineitem;
   B::Adapter<nation_t>    nation;
   B::Adapter<region_t>    region;
   B::Adapter<invoice_t>   invoice;

   // Q3I-specific adapters.
   B::Adapter<tpch::q3i::q3i_pipeline_view_t> pipeline_view;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli;
   B::Adapter<tpch::orders_coli_t>   split_orders;
   B::Adapter<tpch::lineitem_coli_t> split_lineitem;
   B::Adapter<tpch::invoice_coli_t>  split_invoice;

   // S5: aCOLI 3-type MI (customer_acoli_t + orders_coli_t + lineitem_acoli_t).
   // orders_acoli_t was retired Step 4b and collapsed into orders_coli_t.
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part           = B::Adapter<part_t>(db, "part");
      supplier       = B::Adapter<supplier_t>(db, "supplier");
      partsupp       = B::Adapter<partsupp_t>(db, "partsupp");
      customer       = B::Adapter<customerh_t>(db, "customer");
      orders         = B::Adapter<orders_t>(db, "orders");
      lineitem       = B::Adapter<lineitem_i_t>(db, "lineitem");
      nation         = B::Adapter<nation_t>(db, "nation");
      region         = B::Adapter<region_t>(db, "region");
      invoice        = B::Adapter<invoice_t>(db, "invoice");
      pipeline_view  = B::Adapter<tpch::q3i::q3i_pipeline_view_t>(db, "q3i_pipeline_view");
      merged_coli    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                        tpch::lineitem_coli_t, tpch::invoice_coli_t>(
                          db, "q3i_merged_coli");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "q3i_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_coli_t>(db, "q3i_split_lineitem");
      split_invoice  = B::Adapter<tpch::invoice_coli_t>(db, "q3i_split_invoice");
      acoli          = B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                                        tpch::lineitem_acoli_t>(db, "q3i_acoli");
   });

   LeanStoreLogger logger(db);
   TPCHIWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, invoice, logger);

   tpch::q3i::Q3IWorkload<B> q3i(tpch, customer, orders, lineitem, invoice,
                                   pipeline_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);

   // Load base tables once (matches RocksDB harness rationale: TPC-H RNG
   // advances each tpch.load() call, so re-loading produces different data;
   // load once and populate every secondary up front).
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      leanstore::cr::Worker::my().commitTX();
   });

   std::cout << "=== Populating secondaries ===\n";
   // S2 view.
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch::q3i::populate_q3i_view<B>(customer, orders, lineitem, invoice, pipeline_view);
      leanstore::cr::Worker::my().commitTX();
   });
   // S1 split, S3 merged, S5 aggregated. (S4 hash needs no secondary.)
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q3i.coli_pipeline().populate_split();
      leanstore::cr::Worker::my().commitTX();
   });
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q3i.coli_pipeline().populate_merged();
      leanstore::cr::Worker::my().commitTX();
   });
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q3i.coli_pipeline().populate_aggregated();
      leanstore::cr::Worker::my().commitTX();
   });

   // -----------------------------------------------------------------------
   // Per-secondary cardinality (typed scan over each adapter).
   //
   // Same rationale as the RocksDB harness: a future regression that
   // mis-populates a secondary should fail this test rather than silently
   // produce a 0-row "win" downstream.

   auto count_typed = [](auto& adapter, auto record_tag) -> long {
      using R = decltype(record_tag);
      long n = 0;
      typename R::Key start{};
      adapter.scan(start, [&](const typename R::Key&, const R&) {
         ++n;
         return true;
      }, []{});
      return n;
   };

   long n_view, n_split_o, n_split_l, n_split_i;
   long n_customers, n_orders_b, n_lineitems_ref, n_invoices_ref;

   // CountVisitor for merged_coli: counts each record type plus per-group
   // distribution and sentinel-ordering check. Backend-agnostic; identical
   // body to the RocksDB harness.
   struct CountVisitor {
      long customers = 0, invoices = 0, orders = 0, lineitems = 0, groups = 0;
      long g_orders = 0, g_invoices = 0;
      long o_lineitems = 0;
      long min_o_per_g = LONG_MAX, max_o_per_g = 0, sum_o_per_g = 0;
      long min_i_per_g = LONG_MAX, max_i_per_g = 0, sum_i_per_g = 0;
      long min_l_per_o = LONG_MAX, max_l_per_o = 0, sum_l_per_o = 0, n_orders_for_l = 0;
      bool customer_seen_in_group = false;
      long sentinel_violations = 0;
      ::tpch::WalkAction on_customer(Integer, const tpch::customer_coli_t&) {
         ++customers; customer_seen_in_group = true;
         return ::tpch::WalkAction::Continue;
      }
      ::tpch::WalkAction on_invoice (const tpch::invoice_coli_t::Key&,  const tpch::invoice_coli_t&)  {
         ++invoices; ++g_invoices;
         if (!customer_seen_in_group) ++sentinel_violations;
         return ::tpch::WalkAction::Continue;
      }
      ::tpch::WalkAction on_order   (const tpch::orders_coli_t::Key&,   const tpch::orders_coli_t&)   {
         if (g_orders > 0) {
            min_l_per_o = std::min(min_l_per_o, o_lineitems);
            max_l_per_o = std::max(max_l_per_o, o_lineitems);
            sum_l_per_o += o_lineitems;
            ++n_orders_for_l;
         }
         o_lineitems = 0;
         ++orders; ++g_orders;
         if (!customer_seen_in_group) ++sentinel_violations;
         return ::tpch::WalkAction::Continue;
      }
      ::tpch::WalkAction on_lineitem(const tpch::lineitem_coli_t::Key&, const tpch::lineitem_coli_t&) {
         ++lineitems; ++o_lineitems;
         if (!customer_seen_in_group) ++sentinel_violations;
         return ::tpch::WalkAction::Continue;
      }
      void on_group_end(Integer) {
         ++groups;
         if (g_orders > 0) {
            min_l_per_o = std::min(min_l_per_o, o_lineitems);
            max_l_per_o = std::max(max_l_per_o, o_lineitems);
            sum_l_per_o += o_lineitems;
            ++n_orders_for_l;
         }
         min_o_per_g = std::min(min_o_per_g, g_orders);
         max_o_per_g = std::max(max_o_per_g, g_orders);
         sum_o_per_g += g_orders;
         min_i_per_g = std::min(min_i_per_g, g_invoices);
         max_i_per_g = std::max(max_i_per_g, g_invoices);
         sum_i_per_g += g_invoices;
         g_orders = g_invoices = o_lineitems = 0;
         customer_seen_in_group = false;
      }
   } cv;

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      n_view          = count_typed(pipeline_view,  tpch::q3i::q3i_pipeline_view_t{});
      n_split_o       = count_typed(split_orders,   tpch::orders_coli_t{});
      n_split_l       = count_typed(split_lineitem, tpch::lineitem_coli_t{});
      n_split_i       = count_typed(split_invoice,  tpch::invoice_coli_t{});
      n_customers     = count_typed(customer, customerh_t{});
      n_orders_b      = count_typed(orders,   orders_t{});
      n_lineitems_ref = count_typed(lineitem, lineitem_i_t{});
      n_invoices_ref  = count_typed(invoice,  invoice_t{});
      tpch::coli_group_walk<B>(merged_coli, cv);
      leanstore::cr::Worker::my().commitTX();
   });
   long n_merged_total = cv.customers + cv.invoices + cv.orders + cv.lineitems;

   auto check = [](const char* label, bool ok, long got, const std::string& expected) {
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << label
                << " rows=" << std::setw(8) << got
                << " (expected " << expected << ")\n";
   };

   std::cout << "\n=== Secondary cardinality / size ===\n";
   std::cout << "[base] customers=" << n_customers
             << " orders=" << n_orders_b
             << " lineitems=" << n_lineitems_ref
             << " invoices=" << n_invoices_ref << "\n";

   bool stats_ok = true;
   // Pipeline view: one row per (custkey, orderkey, linenumber) after schema
   // redesign 2026-05-03. Cardinality = |lineitem| (no shipdate filter at load).
   {
      bool ok = (n_view == n_lineitems_ref);
      stats_ok &= ok;
      check("pipeline_view", ok, n_view, "= " + std::to_string(n_lineitems_ref));
   }
   {
      bool ok = (n_split_o == n_orders_b);
      stats_ok &= ok;
      check("split_orders", ok, n_split_o, "= " + std::to_string(n_orders_b));
   }
   { bool ok = (n_split_l > 0); stats_ok &= ok; check("split_lineitem", ok, n_split_l, "> 0"); }
   { bool ok = (n_split_i > 0); stats_ok &= ok; check("split_invoice",  ok, n_split_i, "> 0"); }
   {
      long expected = n_customers + n_orders_b + n_lineitems_ref + n_invoices_ref;
      bool ok = (n_merged_total == expected);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << "merged_coli"
                << " rows=" << std::setw(8) << n_merged_total
                << " (c=" << cv.customers << " i=" << cv.invoices
                << " o=" << cv.orders << " l=" << cv.lineitems
                << " groups=" << cv.groups
                << ", expected " << expected << ")\n";
   }

   // LeanStore size paths walk inner pages via HybridPageGuard, which
   // needs a Worker TLS that the main thread does not have. Schedule on
   // worker 0 so the calls happen inside a Worker context (same fix as
   // 708daa84 applied to test_load_col_leanstore).
   double sz_view = 0, sz_so = 0, sz_sl = 0, sz_si = 0, sz_merge = 0, sz_acoli = 0;
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      sz_view  = pipeline_view.size();
      sz_so    = split_orders.size();
      sz_sl    = split_lineitem.size();
      sz_si    = split_invoice.size();
      sz_merge = merged_coli.size();
      sz_acoli = acoli.size();
      leanstore::cr::Worker::my().commitTX();
   });
   std::cout << "[size] pipeline_view=" << std::fixed << std::setprecision(3) << sz_view << " MiB"
             << "  split_orders=" << sz_so << " MiB"
             << "  split_lineitem=" << sz_sl << " MiB"
             << "  split_invoice=" << sz_si << " MiB"
             << "  merged_coli=" << sz_merge << " MiB"
             << "  acoli=" << sz_acoli << " MiB\n";

   // ------------------------------------------------------------------
   // A7: LeanStore content-walk (leaf-fill diagnostic).
   //
   // RocksDB harness has had this since 50fd2052; LeanStore lacked it
   // because there's no per-CF concept here. The size() above is
   // estimatePages() * EFFECTIVE_PAGE_SIZE — a page-occupancy estimate.
   // The content walk sums key+value bytes per record to give a
   // like-for-like content number; their ratio is the leaf-fill rate.
   //
   // Healthy B-tree fill ratio is ~0.5–0.7 (split-on-full pages average
   // ~70% utilisation). A ratio far below 0.5 implies (a) measurement
   // bug in size(), (b) extreme internal fragmentation, or (c) heavy
   // internal-node overhead at small SF.
   {
      std::pair<long, long> cw_view, cw_so, cw_sl, cw_si, cw_merge, cw_acoli;
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
         cw_view  = pipeline_view.content_bytes_walk();
         cw_so    = split_orders.content_bytes_walk();
         cw_sl    = split_lineitem.content_bytes_walk();
         cw_si    = split_invoice.content_bytes_walk();
         cw_merge = merged_coli.content_bytes_walk();
         cw_acoli = acoli.content_bytes_walk();
         leanstore::cr::Worker::my().commitTX();
      });

      auto mb  = [](long b) { return static_cast<double>(b) / (1024.0 * 1024.0); };
      auto cpr = [](long bytes, long rows) {
         return rows > 0 ? static_cast<double>(bytes) / static_cast<double>(rows) : 0.0;
      };
      auto fill = [](double content_mib, double reported_mib) {
         return reported_mib > 0 ? content_mib / reported_mib : 0.0;
      };

      auto print_walk = [&](const char* tag, long bytes, long rows, double reported_mib) {
         double content_mib = mb(bytes);
         std::cout << "[content/row] " << std::left << std::setw(15) << tag
                   << " " << std::fixed << std::setprecision(1) << cpr(bytes, rows)
                   << "  rows=" << rows << "\n"
                   << "[fill]        " << std::left << std::setw(15) << tag
                   << " content=" << std::fixed << std::setprecision(3) << content_mib << " MiB"
                   << "  reported=" << reported_mib << " MiB"
                   << "  ratio=" << std::setprecision(3) << fill(content_mib, reported_mib) << "\n";
      };
      // Reuse the sz_* values computed inside the scheduleJobSync above —
      // calling .size() here on the main thread crashes (no Worker TLS).
      print_walk("pipeline_view", cw_view.first,  cw_view.second,  sz_view);
      print_walk("split_orders",  cw_so.first,    cw_so.second,    sz_so);
      print_walk("split_lineitem",cw_sl.first,    cw_sl.second,    sz_sl);
      print_walk("split_invoice", cw_si.first,    cw_si.second,    sz_si);
      print_walk("merged_coli",   cw_merge.first, cw_merge.second, sz_merge);
      print_walk("acoli",         cw_acoli.first, cw_acoli.second, sz_acoli);
   }

   std::cout << "\n=== merged_coli per-group distribution ===\n";
   auto dist_line = [](const char* label, long min_v, long max_v, long sum_v, long n) {
      double mean = (n > 0) ? (double)sum_v / (double)n : 0.0;
      std::cout << "[dist] " << std::left << std::setw(22) << label
                << "min=" << std::right << std::setw(4) << (min_v == LONG_MAX ? 0 : min_v)
                << " max=" << std::setw(5) << max_v
                << " mean=" << std::fixed << std::setprecision(2) << mean
                << " (n=" << n << ")\n";
   };
   dist_line("orders/customer",   cv.min_o_per_g, cv.max_o_per_g, cv.sum_o_per_g, cv.groups);
   dist_line("invoices/customer", cv.min_i_per_g, cv.max_i_per_g, cv.sum_i_per_g, cv.groups);
   dist_line("lineitems/order",   cv.min_l_per_o, cv.max_l_per_o, cv.sum_l_per_o, cv.n_orders_for_l);

   {
      bool ok = (cv.sentinel_violations == 0);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "sentinel ordering"
                << " violations=" << cv.sentinel_violations << " (expected 0)\n";
   }

   // -----------------------------------------------------------------------
   // Run all five paths inside one OLAP transaction.

   std::cout << "\n=== Running queries ===\n";
   std::vector<tpch::q3i::q3i_agg_row_t> r_base, r_view, r_merged, r_hash, r_agg;
   tpch::q3i::Q3IStats st_base, st_view, st_merged, st_hash, st_agg;
   long us_base = 0, us_view = 0, us_merged = 0, us_hash = 0, us_agg = 0;

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      auto time_us = [](auto&& fn) {
         auto t0 = std::chrono::high_resolution_clock::now();
         fn();
         auto t1 = std::chrono::high_resolution_clock::now();
         return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
      };
      q3i.stats = &st_base;   us_base   = time_us([&] { q3i.query_by_base      (r_base);   });
      q3i.stats = &st_view;   us_view   = time_us([&] { q3i.query_by_view      (r_view);   });
      q3i.stats = &st_merged; us_merged = time_us([&] { q3i.query_by_merged    (r_merged); });
      q3i.stats = &st_hash;   us_hash   = time_us([&] { q3i.query_by_hash      (r_hash);   });
      q3i.stats = &st_agg;    us_agg    = time_us([&] { q3i.query_by_aggregated(r_agg);    });
      q3i.stats = nullptr;
      leanstore::cr::Worker::my().commitTX();
   });

   auto print_timing = [](const char* name, long us) {
      std::cout << "[time] " << std::left << std::setw(20) << name
                << std::right << std::setw(10) << us << " us  ("
                << std::fixed << std::setprecision(3) << (us / 1000.0) << " ms)\n";
   };
   print_timing("query_by_base",       us_base);
   print_timing("query_by_view",       us_view);
   print_timing("query_by_merged",     us_merged);
   print_timing("query_by_hash",       us_hash);
   print_timing("query_by_aggregated", us_agg);

   // Sort by orderkey for digest stability.
   auto by_orderkey = [](const tpch::q3i::q3i_agg_row_t& a,
                         const tpch::q3i::q3i_agg_row_t& b) {
      return a.o_orderkey < b.o_orderkey;
   };
   std::sort(r_base.begin(),   r_base.end(),   by_orderkey);
   std::sort(r_view.begin(),   r_view.end(),   by_orderkey);
   std::sort(r_merged.begin(), r_merged.end(), by_orderkey);
   std::sort(r_hash.begin(),   r_hash.end(),   by_orderkey);
   std::sort(r_agg.begin(),    r_agg.end(),    by_orderkey);

   uint64_t d_base   = digest_rows(r_base);
   uint64_t d_view   = digest_rows(r_view);
   uint64_t d_merged = digest_rows(r_merged);
   uint64_t d_hash   = digest_rows(r_hash);
   uint64_t d_agg    = digest_rows(r_agg);

   auto print_digest = [](const char* name, size_t n, uint64_t d) {
      std::cout << std::left << std::setw(14) << name
                << " rows=" << std::setw(4) << n
                << " digest=0x" << std::hex << d << std::dec << "\n";
   };

   std::cout << "\n=== Results ===\n";
   print_digest("S1 (base)",   r_base.size(),   d_base);
   print_digest("S2 (view)",   r_view.size(),   d_view);
   print_digest("S3 (merged)", r_merged.size(), d_merged);
   print_digest("S4 (hash)",   r_hash.size(),   d_hash);
   print_digest("S5 (acoli)",  r_agg.size(),    d_agg);

   // Per-stage cardinality (Q3IStats — backend-agnostic).
   bool card_ok = true;
   std::cout << "\n=== Per-path cardinality: scanned ===\n";
   std::cout << "[scan] " << std::left << std::setw(11) << "path"
             << std::right << std::setw(10) << "cust"
             << std::setw(10) << "orders"
             << std::setw(11) << "lineitems"
             << std::setw(10) << "invoices"
             << std::setw(9)  << "joins"
             << std::setw(9)  << "agg"
             << "\n";
   auto card_line = [](const char* name, const tpch::q3i::Q3IStats& s) {
      std::cout << "[scan] " << std::left << std::setw(11) << name
                << std::right << std::setw(10) << s.customers_scanned
                << std::setw(10) << s.orders_scanned
                << std::setw(11) << s.lineitems_scanned
                << std::setw(10) << s.invoices_scanned
                << std::setw(9)  << s.join_callbacks
                << std::setw(9)  << s.aggregator_rows_out
                << "\n";
   };
   card_line("S1 base",   st_base);
   card_line("S2 view",   st_view);
   card_line("S3 merged", st_merged);
   card_line("S4 hash",   st_hash);
   std::cout << "[scan] " << std::left << std::setw(11) << "S5 acoli"
             << std::right
             << std::setw(10) << st_agg.acoli_customers_scanned
             << std::setw(10) << st_agg.acoli_orders_scanned
             << std::setw(11) << st_agg.acoli_lineitems_scanned
             << std::setw(10) << "(n/a)"
             << std::setw(9)  << st_agg.acoli_orders_emitted
             << std::setw(9)  << st_agg.aggregator_rows_out
             << "\n";
   std::cout << "  [scan] S5 acoli_customers_passing_filter="
             << st_agg.acoli_customers_passing_filter
             << " acoli_lineitems_passing=" << st_agg.acoli_lineitems_passing << "\n";

   {
      auto cross_check = [&](const char* label, long s1, long s3, long s4) {
         bool ok = (s1 == s3) && (s3 == s4);
         card_ok &= ok;
         std::cout << (ok ? "[OK]   " : "[FAIL] ")
                   << "cross-structure " << std::left << std::setw(20) << label
                   << " S1=" << s1 << " S3=" << s3 << " S4=" << s4 << "\n";
      };
      cross_check("customers_pass",
                  st_base.customers_passing_filter,
                  st_merged.customers_passing_filter,
                  st_hash.customers_passing_filter);
      cross_check("aggregator_rows_out",
                  st_base.aggregator_rows_out,
                  st_merged.aggregator_rows_out,
                  st_hash.aggregator_rows_out);
   }

   auto check_joins = [&](const char* tag, long joins) {
      bool ok = joins > 0;
      card_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << tag
                << " join_callbacks=" << joins << " (expected > 0)\n";
   };
   check_joins("S1 base  ", st_base.join_callbacks);
   check_joins("S2 view  ", st_view.join_callbacks);
   std::cout << "[--]   S3 merged mi_records_visited=" << st_merged.mi_records_visited
             << " mi_groups_skipped=" << st_merged.mi_groups_skipped
             << " agg_rows="          << st_merged.aggregator_rows_out << "\n";
   check_joins("S4 hash  ", st_hash.join_callbacks);

   // Fairness-fix counters (Q3 ports bbc15e68 + f62a0149 + 51ea87b0).
   std::cout << "[--]   S1 s1_groups_skipped  = " << st_base.s1_groups_skipped
             << "    (orders_scanned = " << st_base.orders_scanned << ")\n"
             << "[--]   S2 view_groups_skipped = " << st_view.view_groups_skipped << "\n"
             << "[--]   S4 s4_orderkey_seeks   = " << st_hash.s4_orderkey_seeks
             << "    (lineitems_scanned = " << st_hash.lineitems_scanned << ")\n"
             << "[--]   S4 s4_hashtable_bytes  = " << st_hash.s4_hashtable_bytes
             << " (" << std::fixed << std::setprecision(2)
             << (double(st_hash.s4_hashtable_bytes) / 1048576.0) << " MiB)\n";

   {
      bool ok = st_merged.mi_groups_skipped > 0;
      card_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "S3 mi_groups_skipped"
                << "=" << st_merged.mi_groups_skipped << " (expected > 0)\n";
   }

   // Top-10 of S3 by revenue DESC for inspection.
   auto r_merged_top = r_merged;
   std::sort(r_merged_top.begin(), r_merged_top.end(),
             [](const tpch::q3i::q3i_agg_row_t& a,
                const tpch::q3i::q3i_agg_row_t& b) { return a.revenue > b.revenue; });
   std::cout << "\n=== Top-10 S3 (merged) by revenue DESC ===\n";
   std::cout << "  o_orderkey\trevenue\to_orderdate\to_shippriority\tcust_open_due\n";
   for (const auto& r : r_merged_top) r.print(std::cout);

   // Top-K row-by-row cross-structure check (defensive, since digest is
   // order-independent and could pass with different tiebreaker orderings).
   {
      auto rows_eq = [](const tpch::q3i::q3i_agg_row_t& a,
                        const tpch::q3i::q3i_agg_row_t& b) {
         constexpr double kTol = 1e-3;
         auto close = [](double x, double y, double tol) {
            double d = x - y;
            return (d < 0 ? -d : d) <= tol;
         };
         return a.o_orderkey      == b.o_orderkey
             && close(static_cast<double>(a.revenue),       static_cast<double>(b.revenue),       kTol)
             && a.o_orderdate     == b.o_orderdate
             && a.o_shippriority  == b.o_shippriority
             && close(static_cast<double>(a.cust_open_due), static_cast<double>(b.cust_open_due), kTol);
      };
      bool topK_ok = true;
      auto check_path = [&](const char* tag, const std::vector<tpch::q3i::q3i_agg_row_t>& v) {
         if (v.size() != r_merged.size()) {
            topK_ok = false;
            std::cout << "[FAIL] " << tag << " topK size mismatch: "
                      << v.size() << " vs S3 " << r_merged.size() << "\n";
            return;
         }
         for (size_t i = 0; i < v.size(); ++i) {
            if (!rows_eq(v[i], r_merged[i])) {
               topK_ok = false;
               std::cout << "[FAIL] " << tag << " topK row " << i << " differs\n";
            }
         }
      };
      check_path("S1 base  ", r_base);
      check_path("S2 view  ", r_view);
      check_path("S4 hash  ", r_hash);
      if (topK_ok) std::cout << "[OK]   topK row-by-row equal across S1/S2/S3/S4\n";
      card_ok &= topK_ok;
   }

   // Parity check: all five digests must match.
   std::cout << "\n=== Parity check ===\n";
   uint64_t ref  = d_merged;
   bool     ok_b = (d_base   == ref);
   bool     ok_v = (d_view   == ref);
   bool     ok_m = (d_merged == ref);
   bool     ok_h = (d_hash   == ref);

   auto parity_line = [&](const char* tag, bool ok, uint64_t d) {
      std::ostringstream ss;
      ss << std::hex << ref;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << tag
                << " digest=0x" << std::hex << d << std::dec
                << (ok ? "" : "  (expected S3 0x" + ss.str() + ")")
                << "\n";
   };
   parity_line("S1 base  ", ok_b, d_base);
   parity_line("S2 view  ", ok_v, d_view);
   parity_line("S3 merged", ok_m, d_merged);
   parity_line("S4 hash  ", ok_h, d_hash);

   // S5 parity: unconditionally checked — pre_open_due is the only baked
   // field (i_status='O' is hardcoded by spec, parameter-independent).
   // Revenue is computed at query time from lineitem_acoli_t rows, so S5
   // is correct for any DATE / THRESHOLD param set.  [SKIP S5] guard removed
   // 2026-05-03 after schema redesign (plans/q3i-s5-rebuild-store-lineitems.md).
   bool ok_a = (d_agg == ref);
   parity_line("S5 acoli ", ok_a, d_agg);

   // Shape: all paths agree on row count, in (0, 10].
   bool shape_ok = true;
   size_t n_ref = r_merged.size();
   for (auto [tag, n] : {std::pair{"S1", r_base.size()},
                         std::pair{"S2", r_view.size()},
                         std::pair{"S3", r_merged.size()},
                         std::pair{"S4", r_hash.size()},
                         std::pair{"S5", r_agg.size()}}) {
      bool ok = (n == n_ref) && (n > 0) && (n <= 10);
      if (!ok) shape_ok = false;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << tag
                << " row_count=" << n
                << " (expected = " << n_ref << ", in (0, 10])\n";
   }

   // S5 scan-efficiency check: acoli_total must be > 0. The 3-type aCOLI
   // stores customer + order + lineitem rows, so its total is close to
   // COLI MI minus invoice rows (the only collapsed table).
   {
      long acoli_total = st_agg.acoli_customers_scanned
                       + st_agg.acoli_orders_scanned
                       + st_agg.acoli_lineitems_scanned;
      long coli_total  = st_merged.mi_records_visited;
      bool ok = (acoli_total > 0);
      std::cout << (ok ? "[OK]   " : "[FAIL] ")
                << "S5 acoli_total=" << acoli_total
                << " (c=" << st_agg.acoli_customers_scanned
                << " o=" << st_agg.acoli_orders_scanned
                << " l=" << st_agg.acoli_lineitems_scanned << ")"
                << " vs S3 mi_records_visited=" << coli_total
                << " (expected S5 > 0; aCOLI ≈ COLI − invoice rows)\n";
      card_ok &= ok;
   }

   bool all_ok = ok_b && ok_v && ok_m && ok_h && ok_a && shape_ok && stats_ok && card_ok;
   return all_ok ? 0 : 1;
}

#endif  // ROCKSDB_ONLY
