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

   // Walk merged_coli once via a counting Visitor.
   struct CountVisitor {
      long customers = 0, invoices = 0, orders = 0, lineitems = 0, groups = 0;
      bool on_customer(Integer, const tpch::customer_coli_t&) { ++customers; return true; }
      void on_invoice (const tpch::invoice_coli_t::Key&,  const tpch::invoice_coli_t&)  { ++invoices; }
      void on_order   (const tpch::orders_coli_t::Key&,   const tpch::orders_coli_t&)   { ++orders; }
      void on_lineitem(const tpch::lineitem_coli_t::Key&, const tpch::lineitem_coli_t&) { ++lineitems; }
      void on_group_end(Integer) { ++groups; }
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
   std::cout << "\n=== Per-path cardinality (Q3IStats) ===\n";
   std::cout << "[card] " << std::left << std::setw(11) << "path"
             << std::right << std::setw(10) << "cust"
             << std::setw(10) << "orders"
             << std::setw(11) << "lineitems"
             << std::setw(10) << "invoices"
             << std::setw(9)  << "joins"
             << std::setw(9)  << "agg"
             << "\n";
   auto card_line = [](const char* name, const tpch::q3i::Q3IStats& s) {
      std::cout << "[card] " << std::left << std::setw(11) << name
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

   bool card_ok = true;
   auto check_joins = [&](const char* tag, long joins) {
      bool ok = joins > 0;
      card_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << tag
                << " join_callbacks=" << joins << " (expected > 0)\n";
   };
   check_joins("S1 base  ", st_base.join_callbacks);
   check_joins("S2 view  ", st_view.join_callbacks);
   // S3 merged: query_by_merged does not currently increment Q3IStats
   // counters (the COLIGroupWalkVisitor doesn't thread stats through).
   // Empty-secondary regressions for S3 are caught by the merged_coli
   // row count check above. Wiring stats into the merged path is a TODO.
   std::cout << "[--]   S3 merged join_callbacks=" << st_merged.join_callbacks
             << " (stats not wired through Visitor; see merged_coli rows)\n";
   check_joins("S4 hash  ", st_hash.join_callbacks);

   // Print top-10 of S3 (merged oracle) sorted by revenue DESC for inspection.
   auto r_merged_top = r_merged;
   std::sort(r_merged_top.begin(), r_merged_top.end(),
             [](const tpch::q3i::q3i_agg_row_t& a,
                const tpch::q3i::q3i_agg_row_t& b) { return a.revenue > b.revenue; });
   std::cout << "\n=== Top-10 S3 (merged) by revenue DESC ===\n";
   std::cout << "  o_orderkey\trevenue\to_orderdate\to_shippriority\tcust_open_due\n";
   for (const auto& r : r_merged_top) r.print(std::cout);

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

   // Shape check: all four must return 10 rows at SF=1.
   bool shape_ok = true;
   for (auto [tag, n] : {std::pair{"S1", r_base.size()},
                         std::pair{"S2", r_view.size()},
                         std::pair{"S3", r_merged.size()},
                         std::pair{"S4", r_hash.size()}}) {
      bool ok = (n == 10);
      if (!ok) shape_ok = false;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << tag
                << " row_count=" << n << " (expected 10)\n";
   }

   bool all_ok = ok_b && ok_v && ok_m && ok_h && shape_ok && stats_ok && card_ok;
   return all_ok ? 0 : 1;
}
