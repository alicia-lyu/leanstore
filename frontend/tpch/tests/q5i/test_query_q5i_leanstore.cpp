#ifndef ROCKSDB_ONLY
// Unified Q5I parity harness — LeanStore backend.
//
// Mirrors test_query_q5i_rocksdb.cpp end-to-end: loads base tables once,
// populates ALL secondaries (splits + merged_coli + Pattern B pipeline
// view), asserts strict-equality cardinality on every secondary plus
// sentinel ordering, then runs all four query_by_* paths and asserts
// strict 4-way XOR digest parity.
//
// Differences from the RocksDB harness:
//   - No filesystem wipe of --ssd_path: LeanStore opens fresh each run.
//   - All work runs inside crm.scheduleJobSync(0, ...) closures with a
//     Worker TX. LeanStore's .size() walks inner pages via HybridPageGuard,
//     which requires a Worker TLS that the main thread lacks.
//   - No raw-byte / per-CF metadata reporting: LeanStore has no column
//     families, so the "raw_bytes_in_cf" / "[content/row]" blocks from
//     the RocksDB harness are dropped; size() is reported instead.
//
// Usage:
//   ./build/frontend/test_query_q5i_btree \
//       --ssd_path=/mnt/ssd/q5i_btree \
//       --tpch_scale_factor=1

#include <gflags/gflags.h>
#include <algorithm>
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
#include "../../q5i/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).
// Byte-identical to the RocksDB harness so digests compare across backends.

static uint64_t row_digest(const tpch::q5i::q5i_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ std::hash<std::string>{}(r.n_name);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.nominal_revenue)  * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.realised_revenue) * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.open_revenue)     * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.late_revenue)     * 1e6);
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q5i::q5i_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q5I unified query test — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   // All 8 TPC-H base tables + invoice.
   B::Adapter<part_t>       part;
   B::Adapter<supplier_t>   supplier;
   B::Adapter<partsupp_t>   partsupp;
   B::Adapter<customerh_t>  customer;
   B::Adapter<orders_t>     orders;
   B::Adapter<lineitem_i_t> lineitem;
   B::Adapter<nation_t>     nation;
   B::Adapter<region_t>     region;
   B::Adapter<invoice_t>    invoice;

   // Q5I-specific adapters.
   B::Adapter<tpch::q5i::q5i_pipeline_view_t> pipeline_view;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli;
   B::Adapter<tpch::orders_coli_t>   split_orders;
   B::Adapter<tpch::lineitem_coli_t> split_lineitem;
   B::Adapter<tpch::invoice_coli_t>  split_invoice;

   // aCOLI (S5 deferred — required by Q5IWorkload ctor).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part          = B::Adapter<part_t>(db, "part");
      supplier      = B::Adapter<supplier_t>(db, "supplier");
      partsupp      = B::Adapter<partsupp_t>(db, "partsupp");
      customer      = B::Adapter<customerh_t>(db, "customer");
      orders        = B::Adapter<orders_t>(db, "orders");
      lineitem      = B::Adapter<lineitem_i_t>(db, "lineitem");
      nation        = B::Adapter<nation_t>(db, "nation");
      region        = B::Adapter<region_t>(db, "region");
      invoice       = B::Adapter<invoice_t>(db, "invoice");
      pipeline_view = B::Adapter<tpch::q5i::q5i_pipeline_view_t>(db, "q5i_pipeline_view");
      merged_coli   = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                       tpch::lineitem_coli_t, tpch::invoice_coli_t>(
                         db, "q5i_merged_coli");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "q5i_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_coli_t>(db, "q5i_split_lineitem");
      split_invoice  = B::Adapter<tpch::invoice_coli_t>(db, "q5i_split_invoice");
      acoli          = B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                                        tpch::lineitem_acoli_t>(db, "q5i_acoli");
   });

   LeanStoreLogger logger(db);
   TPCHIWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                   orders, lineitem, nation, region, invoice, logger);

   tpch::q5i::Q5IWorkload<B> q5i(tpch, customer, orders, lineitem, invoice,
                                   supplier, nation, region,
                                   pipeline_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);

   // Load base tables once — all four paths see the same data.
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      leanstore::cr::Worker::my().commitTX();
   });

   // Populate ALL secondaries up front so each query_by_* path sees the
   // same data.  The bug fixed here: prior versions of this harness called
   // only tpch.load() and skipped populate_split/populate_merged/
   // populate_q5i_view, causing S1/S2/S3 to silently scan empty secondaries
   // and return 0 rows while S4 (base-tables-only hash join) returned the
   // correct answer.  SF=1 masked the gap (default ASIA+1994 layout returns
   // 0 rows at SF=1) until SF=5 surfaced the divergence.
   std::cout << "=== Populating secondaries ===\n";
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q5i.coli_pipeline().populate_split();
      leanstore::cr::Worker::my().commitTX();
   });
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q5i.coli_pipeline().populate_merged();
      leanstore::cr::Worker::my().commitTX();
   });
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q5i.populate_q5i_view();
      leanstore::cr::Worker::my().commitTX();
   });

   // -----------------------------------------------------------------------
   // Per-secondary cardinality (typed scan over each adapter).
   //
   // A future regression that mis-populates a secondary should fail this
   // test rather than silently produce a 0-row "win" downstream.
   bool stats_ok = true;

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

   long n_view = 0, n_split_o = 0, n_split_l = 0, n_split_i = 0;
   long n_customers = 0, n_orders_b = 0, n_lineitems_ref = 0, n_invoices_ref = 0;

   // CountVisitor for merged_coli: counts each record type plus per-group
   // distribution and sentinel-ordering check.  Backend-agnostic; identical
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
      ::tpch::WalkAction on_invoice(const tpch::invoice_coli_t::Key&, const tpch::invoice_coli_t&) {
         ++invoices; ++g_invoices;
         if (!customer_seen_in_group) ++sentinel_violations;
         return ::tpch::WalkAction::Continue;
      }
      ::tpch::WalkAction on_order(const tpch::orders_coli_t::Key&, const tpch::orders_coli_t&) {
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
      n_view          = count_typed(pipeline_view,  tpch::q5i::q5i_pipeline_view_t{});
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

   // Pipeline view (Phase 4a Pattern B): one row per base lineitem.
   {
      bool ok = (n_view == n_lineitems_ref);
      stats_ok &= ok;
      check("pipeline_view", ok, n_view, "= " + std::to_string(n_lineitems_ref));
   }
   // Split adapters: strict 1:1 retagging of base tables.
   {
      bool ok = (n_split_o == n_orders_b);
      stats_ok &= ok;
      check("split_orders", ok, n_split_o, "= " + std::to_string(n_orders_b));
   }
   {
      bool ok = (n_split_l == n_lineitems_ref);
      stats_ok &= ok;
      check("split_lineitem", ok, n_split_l, "= " + std::to_string(n_lineitems_ref));
   }
   {
      bool ok = (n_split_i == n_invoices_ref);
      stats_ok &= ok;
      check("split_invoice", ok, n_split_i, "= " + std::to_string(n_invoices_ref));
   }
   // Merged COLI: total == sum of all four base tables; per-type breakdown
   // each matches the corresponding base count.
   {
      long expected = n_customers + n_orders_b + n_lineitems_ref + n_invoices_ref;
      bool total_ok = (n_merged_total == expected);
      bool c_ok     = (cv.customers == n_customers);
      bool o_ok     = (cv.orders    == n_orders_b);
      bool l_ok     = (cv.lineitems == n_lineitems_ref);
      bool i_ok     = (cv.invoices  == n_invoices_ref);
      bool ok = total_ok && c_ok && o_ok && l_ok && i_ok;
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << "merged_coli"
                << " rows=" << std::setw(8) << n_merged_total
                << " (c=" << cv.customers << " i=" << cv.invoices
                << " o=" << cv.orders << " l=" << cv.lineitems
                << " groups=" << cv.groups
                << ", expected " << expected << ")\n";
      if (!c_ok) std::cout << "       [FAIL] customers mismatch: got " << cv.customers << " expected " << n_customers << "\n";
      if (!o_ok) std::cout << "       [FAIL] orders mismatch: got " << cv.orders << " expected " << n_orders_b << "\n";
      if (!l_ok) std::cout << "       [FAIL] lineitems mismatch: got " << cv.lineitems << " expected " << n_lineitems_ref << "\n";
      if (!i_ok) std::cout << "       [FAIL] invoices mismatch: got " << cv.invoices << " expected " << n_invoices_ref << "\n";
   }

   // LeanStore size paths walk inner pages via HybridPageGuard, which
   // needs a Worker TLS that the main thread does not have.  Schedule on
   // worker 0 so the calls happen inside a Worker context.
   double sz_view = 0, sz_so = 0, sz_sl = 0, sz_si = 0, sz_merge = 0;
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      sz_view  = pipeline_view.size();
      sz_so    = split_orders.size();
      sz_sl    = split_lineitem.size();
      sz_si    = split_invoice.size();
      sz_merge = merged_coli.size();
      leanstore::cr::Worker::my().commitTX();
   });
   std::cout << "[size] pipeline_view=" << std::fixed << std::setprecision(3) << sz_view << " MiB"
             << "  split_orders=" << sz_so << " MiB"
             << "  split_lineitem=" << sz_sl << " MiB"
             << "  split_invoice=" << sz_si << " MiB"
             << "  merged_coli=" << sz_merge << " MiB\n";

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

   // Sentinel ordering: customer must precede invoice/orders/lineitems in
   // each group.
   {
      bool ok = (cv.sentinel_violations == 0);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "sentinel ordering"
                << " violations=" << cv.sentinel_violations << " (expected 0)\n";
   }

   // Run all four paths inside one OLAP transaction.
   std::cout << "\n=== Running queries ===\n";
   std::vector<tpch::q5i::q5i_agg_row_t> r_base, r_view, r_merged, r_hash;

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      q5i.query_by_base  (r_base);
      q5i.query_by_view  (r_view);
      q5i.query_by_merged(r_merged);
      q5i.query_by_hash  (r_hash);
      leanstore::cr::Worker::my().commitTX();
   });

   // Compute digests.
   uint64_t d_base   = digest_rows(r_base);
   uint64_t d_view   = digest_rows(r_view);
   uint64_t d_merged = digest_rows(r_merged);
   uint64_t d_hash   = digest_rows(r_hash);

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

   // Parity check: all four digests must match.
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

   bool parity_ok = ok_b && ok_v && ok_m && ok_h;
   if (parity_ok) std::cout << "[OK] parity\n";

   bool all_ok = stats_ok && parity_ok;
   if (!all_ok) {
      std::cerr << "\n[FAIL] One or more checks failed — see [FAIL] lines above.\n";
      return 1;
   }
   return 0;
}

#endif  // ROCKSDB_ONLY
