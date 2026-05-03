// Unified Phase 2C harness for Q3I: loads ALL secondary structures once, then
// runs all four query_by_* paths and asserts cross-structure result parity.
//
// Loading once (vs. per-structure wipe/reload) is essential for parity: the
// TPC-H data generator advances global RNG state during each tpch.load() call,
// so re-loading four times produces four different datasets.  By loading once
// and populating every secondary up front, all four paths see identical data.
//
// Usage:
//   mkdir -p test_data_q3i test_csv_q3i
//   ./build/frontend/test_query_q3i_lsm \
//       --ssd_path=./test_data_q3i \
//       --csv_path=./test_csv_q3i \
//       --tpch_scale_factor=1
//
// Expected: 4 identical digests, 4 identical row counts (10 at SF=1), exit 0.
//
// IMPORTANT: this harness wipes --ssd_path before opening the DB. RocksDB does
// not cleanly overwrite an existing DB; reusing a populated dir across runs
// causes tpch.load() to write new records on top of the prior state, growing
// the lineitem count (e.g. 6051 → 7765 → 10017 across re-runs at SF=1) and
// breaking the consistency invariants between base tables, the COLI MI, the
// pipeline view, and the split indexes — manifesting as a 4-distinct-digest
// parity failure that looks like a query-body bug. The wipe makes the harness
// re-runnable in place, matching how a developer naturally re-invokes it.

#include <gflags/gflags.h>
#include <algorithm>
#include <chrono>
#include <climits>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../coli_pipeline.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../q3i/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).
//
// Mixes each row's fields via rotate-left + XOR so single-field errors flip
// the digest.  Order-independent because XOR is commutative.

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

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3I unified query test — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   using B = tpch::RocksDBBackend;

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);

   // All 8 TPC-H base tables.
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);
   B::Adapter<invoice_t>   invoice(rocks_db);

   // Q3I-specific adapters.
   B::Adapter<tpch::q3i::q3i_pipeline_view_t> pipeline_view(rocks_db);
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // Defensive wipe: if --ssd_path holds a prior DB, remove it before opening.
   // See file-header comment for why this matters (parity-failure mode caused
   // by stale RocksDB state across re-runs).
   {
      namespace fs = std::filesystem;
      fs::path ssd(FLAGS_ssd_path);
      if (fs::exists(ssd) && !fs::is_empty(ssd)) {
         std::cout << "=== Wiping prior --ssd_path contents at " << ssd
                   << " (re-runnable harness) ===\n";
         fs::remove_all(ssd);
      }
      fs::create_directories(ssd);
   }

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, invoice, logger);

   tpch::q3i::Q3IWorkload<B> q3i(tpch, customer, orders, lineitem, invoice,
                                   pipeline_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice);

   // Load base tables ONCE — this is the key fix vs. Phase 2B's per-structure
   // wipe/reload pattern.  All four paths will see the same data.
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   tpch.load();

   // Populate every secondary up front so all four paths are ready.
   // DO NOT route through q3i.load() here — that dispatches on
   // FLAGS_storage_structure and would only populate one secondary.
   std::cout << "=== Populating secondaries ===\n";
   tpch::q3i::populate_q3i_view<B>(customer, orders, lineitem, invoice, pipeline_view);  // S2
   q3i.coli_pipeline().populate_split();    // S1
   q3i.coli_pipeline().populate_merged();   // S3
   // S4 needs no secondary.

   // ------------------------------------------------------------------
   // Per-secondary cardinality + size sanity check.
   //
   // Existence rationale: at SF=15, dram=0.1 the production Makefile flow
   // produced fantasy throughput (S2/S3 ≈ 250k TX/s, 0.00 ms/query) because
   // Q3IWorkload::load() only populates the secondary selected by
   // FLAGS_storage_structure at load time; subsequent --recover runs with a
   // different --storage_structure scanned an EMPTY adapter. The harness
   // itself doesn't hit this bug (we explicitly populate all three above),
   // but adding row-count + size [OK]/[FAIL] checks here means a future
   // regression that mis-populates a secondary fails this test instead of
   // silently producing a 0-row "win".

   // Truthful per-adapter content size (no per-CF metadata included).
   //
   // The adapter's `size()` method is NOT directly comparable across split
   // and merged variants:
   //   - RocksDBAdapter::size() (split / base / view) calls
   //     `GetApproximateSizes` on a key-prefix range in the SHARED default
   //     CF. This excludes per-CF SST footers, bloom filters, index blocks,
   //     and properties — and is allowed a 10% error margin.
   //   - RocksDBMergedAdapter::size() (merged_coli) returns the WHOLE-CF
   //     live data size, which includes every byte in the CF including
   //     fixed-cost metadata. The merged CF has only its own data so the
   //     fixed metadata fraction is large at small SF.
   //
   // For a like-for-like comparison, walk each adapter's iterator and sum
   // `key.size() + value.size()` per row. This is the actual encoded
   // payload — what one logically thinks of as "the row".
   auto raw_bytes_in_cf = [&](ColumnFamilyHandle* cf, int prefix_id_or_neg)
       -> std::pair<long, long> {
      long rows = 0, bytes = 0;
      auto* it = rocks_db.tx_db->NewIterator(rocks_db.iterator_ro, cf);
      if (prefix_id_or_neg >= 0) {
         u8 p = static_cast<u8>(prefix_id_or_neg);
         std::string buf(1, static_cast<char>(p));
         it->Seek(rocksdb::Slice(buf.data(), 1));
         while (it->Valid()) {
            const u8* k = reinterpret_cast<const u8*>(it->key().data());
            if (k[0] != p) break;
            rows++;
            bytes += static_cast<long>(it->key().size() + it->value().size());
            it->Next();
         }
      } else {
         it->SeekToFirst();
         while (it->Valid()) {
            rows++;
            bytes += static_cast<long>(it->key().size() + it->value().size());
            it->Next();
         }
      }
      delete it;
      return {rows, bytes};
   };

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

   long n_view     = count_typed(pipeline_view,  tpch::q3i::q3i_pipeline_view_t{});
   long n_split_o  = count_typed(split_orders,   tpch::orders_coli_t{});
   long n_split_l  = count_typed(split_lineitem, tpch::lineitem_coli_t{});
   long n_split_i  = count_typed(split_invoice,  tpch::invoice_coli_t{});

   // Walk merged_coli once via a counting Visitor that also collects a
   // per-customer-group distribution (orders/customer, lineitems/order,
   // invoices/customer) and per-record-type byte totals.
   struct CountVisitor {
      long customers = 0, invoices = 0, orders = 0, lineitems = 0, groups = 0;
      // Byte totals (sum of sizeof per record type).
      long bytes_customer = 0, bytes_invoice = 0, bytes_order = 0, bytes_lineitem = 0;
      // Per-group running counters (reset on each on_group_end).
      long g_orders = 0, g_invoices = 0, g_lineitems = 0;
      // Per-order running lineitem counter (reset on each on_order).
      long o_lineitems = 0;
      // Aggregated min/max/mean across groups and orders.
      long min_o_per_g = LONG_MAX, max_o_per_g = 0, sum_o_per_g = 0;
      long min_i_per_g = LONG_MAX, max_i_per_g = 0, sum_i_per_g = 0;
      long min_l_per_o = LONG_MAX, max_l_per_o = 0, sum_l_per_o = 0, n_orders_for_l = 0;
      // Sentinel ordering check: customer must arrive before invoices /
      // orders / lineitems in each group.
      bool customer_seen_in_group = false;
      long sentinel_violations = 0;
      bool on_customer(Integer, const tpch::customer_coli_t&) {
         ++customers; bytes_customer += sizeof(tpch::customer_coli_t);
         customer_seen_in_group = true;
         return true;
      }
      void on_invoice (const tpch::invoice_coli_t::Key&,  const tpch::invoice_coli_t&)  {
         ++invoices; ++g_invoices; bytes_invoice += sizeof(tpch::invoice_coli_t);
         if (!customer_seen_in_group) ++sentinel_violations;
      }
      void on_order   (const tpch::orders_coli_t::Key&,   const tpch::orders_coli_t&)   {
         // Close the previous order's lineitem counter.
         if (g_orders > 0) {
            min_l_per_o = std::min(min_l_per_o, o_lineitems);
            max_l_per_o = std::max(max_l_per_o, o_lineitems);
            sum_l_per_o += o_lineitems;
            ++n_orders_for_l;
         }
         o_lineitems = 0;
         ++orders; ++g_orders; bytes_order += sizeof(tpch::orders_coli_t);
         if (!customer_seen_in_group) ++sentinel_violations;
      }
      void on_lineitem(const tpch::lineitem_coli_t::Key&, const tpch::lineitem_coli_t&) {
         ++lineitems; ++o_lineitems; bytes_lineitem += sizeof(tpch::lineitem_coli_t);
         if (!customer_seen_in_group) ++sentinel_violations;
      }
      void on_group_end(Integer) {
         ++groups;
         // Close the final order's lineitem counter.
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
         g_orders = g_invoices = g_lineitems = o_lineitems = 0;
         customer_seen_in_group = false;
      }
   } cv;
   tpch::coli_group_walk<B>(merged_coli, cv);
   long n_merged_total = cv.customers + cv.invoices + cv.orders + cv.lineitems;

   // Expected base-table cardinalities. Don't use tpch.last_*_id —
   // those are SPARSE max keys (e.g. last_order_id at SF=1 is 5988
   // while the loader actually wrote only 1500 orders, because
   // orderkey_from_index strides by ~4). Count the base adapters
   // directly so the ground truth matches what populate_split copies.
   long n_customers     = count_typed(customer, customerh_t{});
   long n_orders        = count_typed(orders,   orders_t{});
   long n_lineitems_ref = count_typed(lineitem, lineitem_t{});
   long n_invoices_ref  = count_typed(invoice,  invoice_t{});

   auto check = [](const char* label, bool ok, long got, const std::string& expected) {
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << label
                << " rows=" << std::setw(8) << got
                << " (expected " << expected << ")\n";
   };

   std::cout << "\n=== Secondary cardinality / size ===\n";
   std::cout << "[base] customers=" << n_customers
             << " orders=" << n_orders
             << " lineitems=" << n_lineitems_ref
             << " invoices=" << n_invoices_ref << "\n";

   bool stats_ok = true;

   // Pipeline view: one row per (custkey, orderkey) after l_shipdate filter
   // and per-order revenue collapse. Bound: (0, |orders|]. Hard-fail on 0.
   {
      bool ok = (n_view > 0) && (n_view <= n_orders);
      stats_ok &= ok;
      check("pipeline_view", ok, n_view,
            "(0, " + std::to_string(n_orders) + "]");
   }
   // Split adapters: 1:1 retag of base tables.
   {
      bool ok = (n_split_o == n_orders);
      stats_ok &= ok;
      check("split_orders", ok, n_split_o, "= " + std::to_string(n_orders));
   }
   {
      bool ok = (n_split_l > 0);  // no exact base count; trust > 0
      stats_ok &= ok;
      check("split_lineitem", ok, n_split_l, "> 0");
   }
   {
      bool ok = (n_split_i > 0);
      stats_ok &= ok;
      check("split_invoice", ok, n_split_i, "> 0");
   }
   // Merged COLI: sum of all four record types.
   {
      long expected = n_customers + n_orders + n_lineitems_ref + n_invoices_ref;
      bool ok = (n_merged_total == expected);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << "merged_coli"
                << " rows=" << std::setw(8) << n_merged_total
                << " (c=" << cv.customers << " i=" << cv.invoices
                << " o=" << cv.orders << " l=" << cv.lineitems
                << " groups=" << cv.groups
                << ", expected " << expected << ")\n";
   }

   std::cout << "[size] pipeline_view=" << std::fixed << std::setprecision(3) << pipeline_view.size() << " MiB"
             << "  split_orders=" << split_orders.size() << " MiB"
             << "  split_lineitem=" << split_lineitem.size() << " MiB"
             << "  split_invoice=" << split_invoice.size() << " MiB"
             << "  merged_coli=" << merged_coli.size() << " MiB\n";

   // Per-secondary average bytes per row. Quantifies the tagged-key overhead
   // in MI vs split adapters and view row width. RocksDB's reported size
   // includes all per-row metadata (key + value + WAL fragments before
   // compaction), so this is end-to-end physical bytes/row.
   auto bpr = [](double size_mib, long rows) -> double {
      if (rows <= 0) return 0.0;
      return size_mib * 1024.0 * 1024.0 / static_cast<double>(rows);
   };
   std::cout << "[bytes/row] pipeline_view="   << std::fixed << std::setprecision(1)
             << bpr(pipeline_view.size(),  n_view)
             << "  split_orders="   << bpr(split_orders.size(),   n_split_o)
             << "  split_lineitem=" << bpr(split_lineitem.size(), n_split_l)
             << "  split_invoice="  << bpr(split_invoice.size(),  n_split_i)
             << "  merged_coli="    << bpr(merged_coli.size(),    n_merged_total) << "\n"
             << "          (RocksDB-reported size / row count; not directly\n"
             << "          comparable across CFs because per-CF SST metadata\n"
             << "          inflates merged_coli at small SF — see below)\n";

   // Truthful content bytes (raw key+value sizes summed across the iterator).
   // For split adapters and the pipeline view we filter the default CF by
   // the record-id prefix (their first byte). For merged_coli we walk the
   // entire dedicated CF.
   auto rb_view  = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::q3i::q3i_pipeline_view_t::id);
   auto rb_so    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::orders_coli_t::id);
   auto rb_sl    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::lineitem_coli_t::id);
   auto rb_si    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::invoice_coli_t::id);
   auto rb_merge = raw_bytes_in_cf(merged_coli.cf_handle, -1);

   auto cpr = [](long bytes, long rows) {
      return rows > 0 ? static_cast<double>(bytes) / static_cast<double>(rows) : 0.0;
   };
   std::cout << "[content/row] pipeline_view=" << std::fixed << std::setprecision(1)
             << cpr(rb_view.second,  rb_view.first)
             << "  split_orders="   << cpr(rb_so.second,    rb_so.first)
             << "  split_lineitem=" << cpr(rb_sl.second,    rb_sl.first)
             << "  split_invoice="  << cpr(rb_si.second,    rb_si.first)
             << "  merged_coli="    << cpr(rb_merge.second, rb_merge.first) << "\n"
             << "          (key.size() + value.size() summed per row from the\n"
             << "          underlying iterator — like-for-like across adapters)\n";

   // Per-CF metadata overhead = RocksDB-reported size − actual content bytes.
   auto mb = [](long b) { return static_cast<double>(b) / (1024.0 * 1024.0); };
   double merged_reported_mib = merged_coli.size();
   double merged_content_mib  = mb(rb_merge.second);
   double overhead_pct = merged_reported_mib > 0
       ? 100.0 * (merged_reported_mib - merged_content_mib) / merged_reported_mib
       : 0.0;
   std::cout << "[overhead] merged_coli reported=" << std::fixed << std::setprecision(3)
             << merged_reported_mib << " MiB  content=" << merged_content_mib
             << " MiB  metadata=" << (merged_reported_mib - merged_content_mib)
             << " MiB (" << std::setprecision(1) << overhead_pct << "%)\n";

   // Per-customer-group MI distribution. Min/max/mean of orders/customer,
   // invoices/customer, and lineitems/order. Sanity-checks the data shape
   // against TPC-H spec and catches generator drift.
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

   // Sentinel-ordering check: customer must arrive before invoices/orders/
   // lineitems within each group. coli_group_walk relies on this for the
   // mktsegment short-circuit.
   {
      bool ok = (cv.sentinel_violations == 0);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "sentinel ordering"
                << " violations=" << cv.sentinel_violations << " (expected 0)\n";
   }

   // Per-record-type byte distribution within MI. Confirms no record type
   // is unexpectedly bloated.
   long total_bytes = cv.bytes_customer + cv.bytes_invoice + cv.bytes_order + cv.bytes_lineitem;
   auto pct = [&](long b) {
      return total_bytes > 0 ? 100.0 * (double)b / (double)total_bytes : 0.0;
   };
   std::cout << "[mi-bytes] customer=" << cv.bytes_customer << " (" << std::fixed
             << std::setprecision(1) << pct(cv.bytes_customer) << "%)"
             << "  invoice=" << cv.bytes_invoice << " (" << pct(cv.bytes_invoice) << "%)"
             << "  order="   << cv.bytes_order   << " (" << pct(cv.bytes_order)   << "%)"
             << "  lineitem="<< cv.bytes_lineitem<< " (" << pct(cv.bytes_lineitem)<< "%)\n";

   // Run all four paths.
   std::cout << "=== Running queries ===\n";
   std::vector<tpch::q3i::q3i_agg_row_t> r_base, r_view, r_merged, r_hash;
   tpch::q3i::Q3IStats st_base, st_view, st_merged, st_hash;

   // Per-query wall-clock timing. Preliminary signal for relative path cost
   // before a full q3i_lsm experiment harness exists; mirrors the pattern
   // used in tests/q12/test_query_q12_rocksdb.cpp.
   auto time_us = [](auto&& fn) {
      auto t0 = std::chrono::high_resolution_clock::now();
      fn();
      auto t1 = std::chrono::high_resolution_clock::now();
      return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
   };
   q3i.stats = &st_base;   long us_base   = time_us([&] { q3i.query_by_base  (r_base);   });
   q3i.stats = &st_view;   long us_view   = time_us([&] { q3i.query_by_view  (r_view);   });
   q3i.stats = &st_merged; long us_merged = time_us([&] { q3i.query_by_merged(r_merged); });
   q3i.stats = &st_hash;   long us_hash   = time_us([&] { q3i.query_by_hash  (r_hash);   });
   q3i.stats = nullptr;

   auto print_timing = [](const char* name, long us) {
      std::cout << "[time] " << std::left << std::setw(16) << name
                << std::right << std::setw(10) << us << " us  ("
                << std::fixed << std::setprecision(3) << (us / 1000.0) << " ms)\n";
   };
   print_timing("query_by_base",   us_base);
   print_timing("query_by_view",   us_view);
   print_timing("query_by_merged", us_merged);
   print_timing("query_by_hash",   us_hash);

   // Sort each result by o_orderkey ASC for digest stability (rows that tie on
   // revenue would be ordered non-deterministically across paths otherwise).
   auto by_orderkey = [](const tpch::q3i::q3i_agg_row_t& a,
                         const tpch::q3i::q3i_agg_row_t& b) {
      return a.o_orderkey < b.o_orderkey;
   };
   std::sort(r_base.begin(),   r_base.end(),   by_orderkey);
   std::sort(r_view.begin(),   r_view.end(),   by_orderkey);
   std::sort(r_merged.begin(), r_merged.end(), by_orderkey);
   std::sort(r_hash.begin(),   r_hash.end(),   by_orderkey);

   // Compute digests.
   uint64_t d_base   = digest_rows(r_base);
   uint64_t d_view   = digest_rows(r_view);
   uint64_t d_merged = digest_rows(r_merged);
   uint64_t d_hash   = digest_rows(r_hash);

   // Print result sizes and digests.
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

   // Per-stage cardinality from Q3IStats.
   //
   // Existence rationale: even if the secondary-cardinality block above
   // passes, an empty-secondary regression could still slip past if a
   // future schema lets `populate_*` write a non-empty but mis-shaped
   // adapter. join_callbacks > 0 on every path is the second line of
   // defence — zero callbacks means the query never saw a row.
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

   // Per-stage cardinality: rows passing each filter and rows surviving
   // each join stage. Should be cross-structure consistent at the
   // aggregate-output level (S1/S3/S4 emit the same set; S2 reads from a
   // pre-aggregated view so its post-filter counts are smaller).
   std::cout << "\n=== Per-path cardinality: post-filter / post-join ===\n";
   std::cout << "[stage] " << std::left << std::setw(10) << "path"
             << std::right << std::setw(9)  << "cust+"
             << std::setw(9)  << "ord+"
             << std::setw(10) << "lin+"
             << std::setw(9)  << "inv+"
             << std::setw(8)  << "j1"
             << std::setw(8)  << "j2"
             << std::setw(8)  << "j3"
             << std::setw(7)  << "topN"
             << "\n";
   auto stage_line = [](const char* name, const tpch::q3i::Q3IStats& s) {
      std::cout << "[stage] " << std::left << std::setw(10) << name
                << std::right << std::setw(9)  << s.customers_passing_filter
                << std::setw(9)  << s.orders_passing_filter
                << std::setw(10) << s.lineitems_passing_filter
                << std::setw(9)  << s.invoices_passing_filter
                << std::setw(8)  << s.join1_output_rows
                << std::setw(8)  << s.join2_output_rows
                << std::setw(8)  << s.join3_output_rows
                << std::setw(7)  << s.topN_candidates
                << "\n";
   };
   stage_line("S1 base",   st_base);
   stage_line("S2 view",   st_view);
   stage_line("S3 merged", st_merged);
   stage_line("S4 hash",   st_hash);

   // Per-stage wall-clock breakdown (microseconds).
   std::cout << "\n=== Per-path stage wall-clock (us) ===\n";
   std::cout << "[time] " << std::left << std::setw(11) << "path"
             << std::right << std::setw(11) << "scan_filt"
             << std::setw(11) << "aggregator"
             << std::setw(11) << "join"
             << std::setw(11) << "topN"
             << "\n";
   auto time_line = [](const char* name, const tpch::q3i::Q3IStats& s) {
      std::cout << "[time] " << std::left << std::setw(11) << name
                << std::right << std::setw(11) << s.stage_us_scan_filter
                << std::setw(11) << s.stage_us_aggregator
                << std::setw(11) << s.stage_us_join
                << std::setw(11) << s.stage_us_topN
                << "\n";
   };
   time_line("S1 base",   st_base);
   time_line("S2 view",   st_view);
   time_line("S3 merged", st_merged);
   time_line("S4 hash",   st_hash);

   // Cross-structure stage cardinality consistency: S1/S3/S4 should agree
   // on customers passing mktsegment, lineitems passing shipdate, invoices
   // passing status='O', and post-aggregate row count. S2 is exempt
   // (operates on the pre-aggregated view).
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
   // S3 merged: per-record join_callbacks aren't tracked through the
   // COLIGroupWalkVisitor (would require an extra hook). Use
   // mi_records_visited / aggregator_rows_out as the proxy.
   std::cout << "[--]   S3 merged mi_records_visited=" << st_merged.mi_records_visited
             << " mi_groups_skipped="  << st_merged.mi_groups_skipped
             << " agg_rows="           << st_merged.aggregator_rows_out << "\n";
   check_joins("S4 hash  ", st_hash.join_callbacks);

   // S3 walk-efficiency invariant: at least one group should be skipped
   // (mktsegment selectivity ≈ 20%, so ~80% of customers fail). If zero,
   // the physical-skip optimisation regressed.
   {
      bool ok = st_merged.mi_groups_skipped > 0;
      card_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "S3 mi_groups_skipped"
                << "=" << st_merged.mi_groups_skipped << " (expected > 0)\n";
   }

   // Print top-10 of S3 (merged oracle) sorted by revenue DESC for inspection.
   auto r_merged_top = r_merged;
   std::sort(r_merged_top.begin(), r_merged_top.end(),
             [](const tpch::q3i::q3i_agg_row_t& a,
                const tpch::q3i::q3i_agg_row_t& b) { return a.revenue > b.revenue; });
   std::cout << "\n=== Top-10 S3 (merged) by revenue DESC ===\n";
   std::cout << "  o_orderkey\trevenue\to_orderdate\to_shippriority\tcust_open_due\n";
   for (const auto& r : r_merged_top) r.print(std::cout);

   // Top-K row-by-row cross-structure check: parity digest is
   // order-independent, so it can pass even when two paths disagree on
   // tiebreaker ordering. This block compares the by-orderkey-sorted top-K
   // pointwise across S1/S2/S3/S4.
   {
      // Float tolerance: revenue and cust_open_due are Numeric (float-based)
      // and accumulation order differs slightly between paths (S4 hash-map
      // iteration vs S1/S3 sorted-stream order). Sub-ulp diffs print as
      // identical and the XOR digest's `* 1e6` cast smooths them over;
      // strict `==` would false-positive on these. Tolerance of 1e-3 is
      // well below the 6-decimal print precision and the XOR-digest
      // resolution.
      auto rows_eq = [](const tpch::q3i::q3i_agg_row_t& a,
                        const tpch::q3i::q3i_agg_row_t& b) {
         constexpr double kTol = 1e-3;
         auto close = [](double x, double y, double tol) {
            double d = x - y;
            return (d < 0 ? -d : d) <= tol;
         };
         return a.o_orderkey      == b.o_orderkey
             && close(static_cast<double>(a.revenue), static_cast<double>(b.revenue), kTol)
             && a.o_orderdate     == b.o_orderdate
             && a.o_shippriority  == b.o_shippriority
             && close(static_cast<double>(a.cust_open_due), static_cast<double>(b.cust_open_due), kTol);
      };
      bool topK_ok = true;
      // Use S3 (merged) as the oracle. r_merged is already sorted by orderkey.
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
               const auto& a = v[i];
               const auto& b = r_merged[i];
               std::cout << "[FAIL] " << tag << " topK row " << i << " differs:\n"
                         << "       got: orderkey=" << a.o_orderkey
                         << " revenue=" << a.revenue
                         << " orderdate=" << a.o_orderdate
                         << " shippri=" << a.o_shippriority
                         << " open_due=" << a.cust_open_due << "\n"
                         << "       S3 : orderkey=" << b.o_orderkey
                         << " revenue=" << b.revenue
                         << " orderdate=" << b.o_orderdate
                         << " shippri=" << b.o_shippriority
                         << " open_due=" << b.cust_open_due << "\n";
            }
         }
      };
      check_path("S1 base  ", r_base);
      check_path("S2 view  ", r_view);
      check_path("S4 hash  ", r_hash);
      if (topK_ok) std::cout << "[OK]   topK row-by-row equal across S1/S2/S3/S4\n";
      card_ok &= topK_ok;
   }

   // Parity check: all four digests must match.
   std::cout << "\n=== Parity check ===\n";
   uint64_t ref  = d_merged;  // S3 is the oracle
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

   // Shape check: all four paths must agree on row count and return at
   // least one row. The exact count is data-dependent (≤ 10 by LIMIT 10;
   // can be < 10 at small SFs because few orders pass the date / shipdate
   // / mktsegment filters). What we care about is cross-structure
   // agreement — divergent counts mean a query-body bug.
   bool shape_ok = true;
   size_t n_ref = r_merged.size();
   for (auto [tag, n] : {std::pair{"S1", r_base.size()},
                         std::pair{"S2", r_view.size()},
                         std::pair{"S3", r_merged.size()},
                         std::pair{"S4", r_hash.size()}}) {
      bool ok = (n == n_ref) && (n > 0) && (n <= 10);
      if (!ok) shape_ok = false;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << tag
                << " row_count=" << n
                << " (expected = " << n_ref << ", in (0, 10])\n";
   }

   bool all_ok = ok_b && ok_v && ok_m && ok_h && shape_ok && stats_ok && card_ok;
   return all_ok ? 0 : 1;
}
