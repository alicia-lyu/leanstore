// Cross-structure parity harness for Q3: loads ALL secondary structures
// once, runs all four query_by_* paths, and asserts strict XOR-digest
// parity across S1 / S2 / S3 / S4.
//
// Phase 4 complete (2026-05-04): all four paths have real bodies; the
// harness enforces strict parity (no [SKIP] tolerance).  S5 is omitted
// for Q3 by design (q3/CLAUDE.md §Open Questions — S5: omit).
//
// Usage:
//   mkdir -p build/scratch/q3/{data,csv}
//   ./build/frontend/test_query_q3_lsm \
//       --ssd_path=./build/scratch/q3/data \
//       --csv_path=./build/scratch/q3/csv \
//       --tpch_scale_factor=1
//
// Expected: 4 identical digests, 4 × row_count > 0, exit 0.
//
// IMPORTANT: wipes --ssd_path before opening the DB (same rationale as
// test_query_q3i_rocksdb.cpp — RocksDB does not cleanly overwrite).

#include <gflags/gflags.h>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <vector>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../tpch_workload.hpp"
#include "../../q3/workload.hpp"
#include "../../tpch_family/refresh.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).

static uint64_t row_digest(const tpch::q3::q3_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderdate));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_shippriority));
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q3::q3_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3 unified query test — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   using B = tpch::RocksDBBackend;

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);

   // All 8 TPC-H base tables (vanilla schema — Q3 uses lineitem_t).
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // Q3-specific adapters.
   B::Adapter<tpch::q3::q3_pipeline_view_t> pipeline_view(rocks_db);

   // COL 3-table merged index (S3) + custkey-sorted split indexes (S1).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col(rocks_db);
   B::Adapter<tpch::orders_coli_t>        split_orders(rocks_db);
   B::Adapter<tpch::lineitem_col_t>       split_lineitem(rocks_db);

   // Defensive wipe: stale RocksDB state across re-runs corrupts cardinality.
   {
      namespace fs = std::filesystem;
      fs::path ssd(FLAGS_ssd_path);
      if (fs::exists(ssd) && !fs::is_empty(ssd)) {
         std::cout << "=== Wiping prior --ssd_path at " << ssd
                   << " (re-runnable harness) ===\n";
         fs::remove_all(ssd);
      }
      fs::create_directories(ssd);
   }

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               pipeline_view, merged_col,
                               split_orders, split_lineitem);

   // Load base tables ONCE — all four paths must see the same data.
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   tpch.load();

   // Populate every secondary up front so all four paths are ready.
   // Do NOT route through q3.load() — that dispatches on FLAGS_storage_structure
   // and would only populate one secondary per run.
   std::cout << "=== Populating secondaries ===\n";
   q3.col_pipeline().populate_split();   // S1: custkey-sorted split indexes
   tpch::q3::populate_q3_view<B>(        // S2: per-lineitem pipeline view
       customer, orders, lineitem, pipeline_view);
   q3.col_pipeline().populate_merged();  // S3: COL merged index
   // S4: base tables only — nothing to populate.

   // ------------------------------------------------------------------
   // Run all four paths.
   std::cout << "=== Running queries ===\n";
   std::vector<tpch::q3::q3_agg_row_t> r_base, r_view, r_merged, r_hash;
   tpch::q3::Stats st_base, st_view, st_merged, st_hash;

   auto time_us = [](auto&& fn) {
      auto t0 = std::chrono::high_resolution_clock::now();
      fn();
      auto t1 = std::chrono::high_resolution_clock::now();
      return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
   };

   q3.stats = &st_base;   long us_base   = time_us([&] { q3.query_by_base  (r_base);   });
   q3.stats = &st_view;   long us_view   = time_us([&] { q3.query_by_view  (r_view);   });
   q3.stats = &st_merged; long us_merged = time_us([&] { q3.query_by_merged(r_merged); });
   q3.stats = &st_hash;   long us_hash   = time_us([&] { q3.query_by_hash  (r_hash);   });
   q3.stats = nullptr;

   auto print_timing = [](const char* name, long us) {
      std::cout << "[time] " << std::left << std::setw(20) << name
                << std::right << std::setw(10) << us << " us  ("
                << std::fixed << std::setprecision(3) << (us / 1000.0) << " ms)\n";
   };
   print_timing("query_by_base",   us_base);
   print_timing("query_by_view",   us_view);
   print_timing("query_by_merged", us_merged);
   print_timing("query_by_hash",   us_hash);

   // ------------------------------------------------------------------
   // Compute digests.
   uint64_t d_base   = digest_rows(r_base);
   uint64_t d_view   = digest_rows(r_view);
   uint64_t d_merged = digest_rows(r_merged);
   uint64_t d_hash   = digest_rows(r_hash);

   std::cout << "\n=== Results ===\n";
   auto print_digest = [](const char* name, size_t n, uint64_t d) {
      std::cout << std::left << std::setw(14) << name
                << " rows=" << std::setw(4) << n
                << " digest=0x" << std::hex << d << std::dec << "\n";
   };
   print_digest("S1 (base)",   r_base.size(),   d_base);
   print_digest("S2 (view)",   r_view.size(),   d_view);
   print_digest("S3 (merged)", r_merged.size(), d_merged);
   print_digest("S4 (hash)",   r_hash.size(),   d_hash);

   // ------------------------------------------------------------------
   // Strict parity check: all four digests must agree (Phase 4 complete).
   std::cout << "\n=== Parity check ===\n";
   uint64_t ref = d_merged;  // S3 is the canonical oracle.

   auto status = [&](uint64_t d) -> const char* {
      return d == ref ? "[OK]   " : "[FAIL] ";
   };
   auto parity_line = [&](const char* tag, uint64_t d, long n) {
      const char* s = status(d);
      std::ostringstream ss; ss << std::hex << ref;
      bool show_expected = (s[1] == 'F');
      std::cout << s << tag
                << " rows=" << std::dec << n
                << " digest=0x" << std::hex << d << std::dec
                << (show_expected ? "  (expected 0x" + ss.str() + ")" : "")
                << "\n";
   };
   parity_line("S1 base  ", d_base,   (long)r_base.size());
   parity_line("S2 view  ", d_view,   (long)r_view.size());
   parity_line("S3 merged", d_merged, (long)r_merged.size());
   parity_line("S4 hash  ", d_hash,   (long)r_hash.size());

   // ------------------------------------------------------------------
   // Param-rotation fairness (audit-response). The perf harness now runs
   // set_params_for_iter(param_seed + count), so the first measured query
   // need not be pinned to PARAM_TABLE[0] (the validation default). The
   // sweep passes one param_seed to all four structure processes of a
   // given rep; the property that makes that fair is that every structure
   // sees the SAME parameter. Verify (a) strict 4-way parity still holds
   // at a non-default parameter (hard fail otherwise) and (b) a different
   // parameter actually changes the result (informational). Reset to
   // iter 0 so the RF round-trip section below runs at the default.
   {
      constexpr long kRotIter = 3;  // a different (segment, date) than [0]
      q3.set_params_for_iter(kRotIter);
      std::vector<tpch::q3::q3_agg_row_t> a, b, c, d;
      q3.query_by_base(a); q3.query_by_view(b);
      q3.query_by_merged(c); q3.query_by_hash(d);
      uint64_t da = digest_rows(a), db = digest_rows(b),
               dc = digest_rows(c), dd = digest_rows(d);
      bool agree = (da == db && db == dc && dc == dd);
      bool rotated = (dc != d_merged);
      std::cout << "\n=== Param-rotation parity (iter=" << kRotIter << ") ===\n"
                << (agree ? "[OK]   " : "[FAIL] ")
                << "4-way agreement at non-default param: 0x" << std::hex << dc
                << std::dec << " (rows=" << c.size() << ")\n"
                << (rotated ? "[OK]   " : "[WARN] ")
                << "digest differs from iter=0 0x" << std::hex << d_merged
                << std::dec << " (rotation exercised)\n";
      q3.set_params_for_iter(0);  // restore validation default
      if (!agree) {
         std::cout << "[FAIL] param-rotation broke cross-structure parity\n";
         return 1;
      }
   }

   // Skip-seek counters across all four paths.  S1 has two physical streams
   // (orders + lineitem), so s1_groups_skipped can be up to ~2× the
   // single-stream counters (S2/S3); what matters is that it is > 0 and of
   // the same order of magnitude.  S4 seeks per qualifying orderkey, not
   // per custkey, so its counter is naturally larger.
   std::cout << "\n=== Per-customer skip-seek (S1 vs S2 vs S3 vs S4) ===\n"
             << "S1 s1_groups_skipped   = " << st_base.s1_groups_skipped
             << "    (orders_scanned = " << st_base.orders_scanned << ")\n"
             << "S2 view_groups_skipped = " << st_view.view_groups_skipped
             << "    (lineitems_scanned = " << st_view.lineitems_scanned << ")\n"
             << "S3 mi_groups_skipped   = " << st_merged.mi_groups_skipped
             << "    (mi_records_visited = " << st_merged.mi_records_visited << ")\n"
             << "S4 s4_orderkey_seeks   = " << st_hash.s4_orderkey_seeks
             << "    (lineitems_scanned = " << st_hash.lineitems_scanned << ")\n"
             << "  S4 s4_hashtable_bytes=" << st_hash.s4_hashtable_bytes
             << " (" << std::fixed << std::setprecision(2)
             << (double(st_hash.s4_hashtable_bytes) / 1048576.0) << " MiB)\n";

   if (r_merged.empty()) {
      std::cout << "\n[FAIL] S3 returned 0 rows — query_by_merged body broken.\n";
      return 1;
   }
   bool pre_ok = (d_base == ref) && (d_view == ref) && (d_hash == ref);
   if (!pre_ok) {
      std::cout << "\n[FAIL] pre-update parity broken — stopping before RF1/RF2.\n";
      return 1;
   }
   const uint64_t pre_update_ref = ref;

   // ------------------------------------------------------------------
   // RF1 + RF2 round-trip parity (refresh_sales experiment).
   //
   // Insert N orders into base + ALL secondaries; verify all four query
   // paths still agree. Delete the same orderkeys; verify all four paths
   // agree AND the digest matches the pre-update value (true round-trip).
   constexpr int N = 10;
   std::cout << "\n=== RF1+RF2 round-trip (N=" << N << ") ===\n";

   tpch::RefreshState<B::Adapter> refresh(tpch);
   struct Inserted {
      orders_t::Key key;
      Integer       custkey;
      std::vector<Integer> linenumbers;
   };
   std::vector<Inserted> inserted;
   inserted.reserve(N);

   // Inline helper: insert into base + every secondary (the test owns all four
   // structures simultaneously, unlike the production refresh_sales binary).
   auto apply_rf1_all = [&](const orders_t::Key& ok, const orders_t& ov,
                             const std::vector<lineitem_t>& lines) {
      const Integer custkey = ov.o_custkey;
      orders.insert(ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         lineitem.insert(lk, lines[j]);
      }
      // S1 split
      q3.col_pipeline().insert_order_to_split(custkey, ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         q3.col_pipeline().insert_lineitem_to_split(custkey, lk, lines[j]);
      }
      // S3 merged
      q3.col_pipeline().insert_order_to_merged(custkey, ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         q3.col_pipeline().insert_lineitem_to_merged(custkey, lk, lines[j]);
      }
      // S2 view (reuse build_q3_view_row from q3/load.tpp)
      Varchar<10> mktseg{};
      customer.lookup1(customerh_t::Key{custkey},
                       [&](const customerh_t& c) { mktseg = c.c_mktsegment; });
      for (size_t j = 0; j < lines.size(); ++j) {
         auto [vk, vv] = tpch::q3::build_q3_view_row(
             custkey, ok.o_orderkey, static_cast<Integer>(j + 1),
             ov, lines[j], mktseg);
         pipeline_view.insert(vk, vv);
      }
   };

   auto apply_rf2_all = [&](const orders_t::Key& ok, Integer custkey,
                             const std::vector<Integer>& linenumbers) {
      // Per-structure secondary first; then base last.
      for (Integer ln : linenumbers)
         q3.col_pipeline().erase_lineitem_from_split(custkey,
             lineitem_t::Key{ok.o_orderkey, ln});
      q3.col_pipeline().erase_order_from_split(custkey, ok);
      for (Integer ln : linenumbers)
         q3.col_pipeline().erase_lineitem_from_merged(custkey,
             lineitem_t::Key{ok.o_orderkey, ln});
      q3.col_pipeline().erase_order_from_merged(custkey, ok);
      for (Integer ln : linenumbers)
         pipeline_view.erase(tpch::q3::q3_pipeline_view_t::Key{
             custkey, ok.o_orderkey, ln});
      for (Integer ln : linenumbers)
         lineitem.erase(lineitem_t::Key{ok.o_orderkey, ln});
      orders.erase(ok);
   };

   for (int i = 0; i < N; ++i) {
      auto r = refresh.next_rf1();
      Inserted rec;
      rec.key     = r.key;
      rec.custkey = r.order.o_custkey;
      rec.linenumbers.reserve(r.lines.size());
      for (size_t j = 0; j < r.lines.size(); ++j)
         rec.linenumbers.push_back(static_cast<Integer>(j + 1));
      apply_rf1_all(r.key, r.order, r.lines);
      inserted.push_back(std::move(rec));
   }
   std::cout << "[ok] applied " << N << " RF1 inserts\n";

   // Re-run all four queries; verify 4-way agreement (digest expected to differ
   // from pre_update_ref because new orders may qualify).
   {
      std::vector<tpch::q3::q3_agg_row_t> a, b, c, d;
      q3.query_by_base(a); q3.query_by_view(b);
      q3.query_by_merged(c); q3.query_by_hash(d);
      uint64_t da = digest_rows(a), db = digest_rows(b),
               dc = digest_rows(c), dd = digest_rows(d);
      std::cout << "[post-RF1] S1=0x" << std::hex << da
                << " S2=0x" << db << " S3=0x" << dc
                << " S4=0x" << dd << std::dec << "\n";
      if (!(da == dc && db == dc && dd == dc)) {
         std::cout << "[FAIL] post-RF1 parity broken across structures.\n";
         return 1;
      }
      std::cout << "[ok]   post-RF1 4-way digest agrees\n";
   }

   // RF2: delete the same orderkeys (test-only round-trip property).
   for (const auto& rec : inserted)
      apply_rf2_all(rec.key, rec.custkey, rec.linenumbers);
   std::cout << "[ok] applied " << N << " RF2 deletes (round-trip)\n";

   {
      std::vector<tpch::q3::q3_agg_row_t> a, b, c, d;
      q3.query_by_base(a); q3.query_by_view(b);
      q3.query_by_merged(c); q3.query_by_hash(d);
      uint64_t da = digest_rows(a), db = digest_rows(b),
               dc = digest_rows(c), dd = digest_rows(d);
      std::cout << "[post-RF2] S1=0x" << std::hex << da
                << " S2=0x" << db << " S3=0x" << dc
                << " S4=0x" << dd << std::dec << "\n";
      bool agree   = (da == dc && db == dc && dd == dc);
      bool restored = (da == pre_update_ref);
      if (!agree) {
         std::cout << "[FAIL] post-RF2 parity broken across structures.\n";
         return 1;
      }
      if (!restored) {
         std::cout << "[FAIL] post-RF2 digest 0x" << std::hex << da
                   << " != pre-update 0x" << pre_update_ref << std::dec
                   << " — round-trip property broken.\n";
         return 1;
      }
      std::cout << "[ok]   post-RF2 digest matches pre-update — RF1+RF2 round-trip OK\n";
   }

   return 0;
}
