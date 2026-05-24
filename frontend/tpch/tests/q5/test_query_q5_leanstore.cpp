#ifndef ROCKSDB_ONLY
// Cross-structure parity harness for Q5 — LeanStore backend.
//
// Mirror of test_query_q5_rocksdb.cpp for the LeanStore (B-tree) backend.
// Phase 0.5: all four query_by_* stubs return empty; parity is vacuously [OK].
//
// Usage (Linux only — LeanStore does not build on macOS):
//   mkdir -p build/scratch/q5_skeleton_btree/{data,csv}
//   ./build/frontend/test_query_q5_btree \
//       --ssd_path=./build/scratch/q5_skeleton_btree/data \
//       --csv_path=./build/scratch/q5_skeleton_btree/csv \
//       --tpch_scale_factor=1

#include <gflags/gflags.h>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string_view>
#include <vector>

#include "../../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../../backend.hpp"
#include "../../tpch_workload.hpp"
#include "../../q5/workload.hpp"
#include "../../tpch_family/refresh.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).

static uint64_t row_digest(const tpch::q5::q5_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   // Hash n_name (the GROUP BY key) not n_nationkey: after Phase 10B the
   // aggregator keys by n_name and sets n_nationkey=0, so parity must be
   // established on the actual output key (matches test_query_q5_rocksdb.cpp).
   std::string_view sv(r.n_name.data, r.n_name.length);
   d = rotl64(d, 13) ^ std::hash<std::string_view>{}(sv);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q5::q5_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q5 unified query test — LeanStore backend");
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

   B::Adapter<tpch::q5::q5_pipeline_view_t> pipeline_view;

   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col;
   B::Adapter<tpch::orders_coli_t>        split_orders;
   B::Adapter<tpch::lineitem_col_t>       split_lineitem;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part          = B::Adapter<part_t>(db, "part");
      supplier      = B::Adapter<supplier_t>(db, "supplier");
      partsupp      = B::Adapter<partsupp_t>(db, "partsupp");
      customer      = B::Adapter<customerh_t>(db, "customer");
      orders        = B::Adapter<orders_t>(db, "orders");
      lineitem      = B::Adapter<lineitem_t>(db, "lineitem");
      nation        = B::Adapter<nation_t>(db, "nation");
      region        = B::Adapter<region_t>(db, "region");
      pipeline_view = B::Adapter<tpch::q5::q5_pipeline_view_t>(db, "q5_pipeline_view");
      merged_col    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                       tpch::lineitem_col_t>(db, "q5_merged_col");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "q5_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "q5_split_lineitem");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q5::Q5Workload<B> q5(tpch, customer, orders, lineitem,
                               supplier, nation, region,
                               pipeline_view, merged_col,
                               split_orders, split_lineitem);

   // Load base tables and all secondaries.
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      leanstore::cr::Worker::my().commitTX();
   });

   std::cout << "=== Populating secondaries ===\n";
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q5.col_pipeline().populate_split();
      tpch::q5::populate_q5_view<B>(customer, nation, orders, lineitem, pipeline_view);
      q5.col_pipeline().populate_merged();
      leanstore::cr::Worker::my().commitTX();
   });

   // ------------------------------------------------------------------
   // Run all four paths.
   std::cout << "=== Running queries ===\n";
   std::vector<tpch::q5::q5_agg_row_t> r_base, r_view, r_merged, r_hash;

   auto time_us = [](auto&& fn) {
      auto t0 = std::chrono::high_resolution_clock::now();
      fn();
      auto t1 = std::chrono::high_resolution_clock::now();
      return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
   };

   long us_base, us_view, us_merged, us_hash;
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
      us_base   = time_us([&] { q5.query_by_base  (r_base);   });
      us_view   = time_us([&] { q5.query_by_view  (r_view);   });
      us_merged = time_us([&] { q5.query_by_merged(r_merged); });
      us_hash   = time_us([&] { q5.query_by_hash  (r_hash);   });
      leanstore::cr::Worker::my().commitTX();
   });

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

   std::cout << "\n=== Parity check ===\n";
   uint64_t ref = d_merged;

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

   bool pre_ok = (d_base == ref) && (d_view == ref) && (d_hash == ref);
   if (!pre_ok) {
      std::cout << "\n[FAIL] pre-update parity broken — stopping before RF1/RF2.\n";
      return 1;
   }
   const uint64_t pre_update_ref = ref;

   // ------------------------------------------------------------------
   // RF1 + RF2 round-trip parity (refresh_sales experiment). Mirror of
   // test_query_q5_rocksdb.cpp; every adapter op runs inside a Worker TX
   // (scheduleJobSync) because LeanStore reads/writes need Worker TLS.
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

   auto apply_rf1_all = [&](const orders_t::Key& ok, const orders_t& ov,
                             const std::vector<lineitem_t>& lines) {
      const Integer custkey = ov.o_custkey;
      orders.insert(ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         lineitem.insert(lk, lines[j]);
      }
      // S1 split
      q5.col_pipeline().insert_order_to_split(custkey, ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         q5.col_pipeline().insert_lineitem_to_split(custkey, lk, lines[j]);
      }
      // S3 merged
      q5.col_pipeline().insert_order_to_merged(custkey, ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         q5.col_pipeline().insert_lineitem_to_merged(custkey, lk, lines[j]);
      }
      // S2 view: FD-attach c_nationkey + n_name (reuse build_q5_view_row).
      Integer c_nationkey = 0;
      customer.lookup1(customerh_t::Key{custkey},
                       [&](const customerh_t& c) { c_nationkey = c.c_nationkey; });
      Varchar<25> n_name{};
      nation.lookup1(nation_t::Key{c_nationkey},
                     [&](const nation_t& n) { n_name = n.n_name; });
      for (size_t j = 0; j < lines.size(); ++j) {
         auto [vk, vv] = tpch::q5::build_q5_view_row(
             custkey, ok.o_orderkey, static_cast<Integer>(j + 1),
             ov, lines[j], c_nationkey, n_name);
         pipeline_view.insert(vk, vv);
      }
   };

   auto apply_rf2_all = [&](const orders_t::Key& ok, Integer custkey,
                             const std::vector<Integer>& linenumbers) {
      // Per-structure secondary first; then base last.
      for (Integer ln : linenumbers)
         q5.col_pipeline().erase_lineitem_from_split(custkey,
             lineitem_t::Key{ok.o_orderkey, ln});
      q5.col_pipeline().erase_order_from_split(custkey, ok);
      for (Integer ln : linenumbers)
         q5.col_pipeline().erase_lineitem_from_merged(custkey,
             lineitem_t::Key{ok.o_orderkey, ln});
      q5.col_pipeline().erase_order_from_merged(custkey, ok);
      for (Integer ln : linenumbers)
         pipeline_view.erase(tpch::q5::q5_pipeline_view_t::Key{
             custkey, ok.o_orderkey, ln});
      for (Integer ln : linenumbers)
         lineitem.erase(lineitem_t::Key{ok.o_orderkey, ln});
      orders.erase(ok);
   };

   // RF1: generate + apply inside one Worker TX.
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
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
      leanstore::cr::Worker::my().commitTX();
   });
   std::cout << "[ok] applied " << N << " RF1 inserts\n";

   // Re-run all four queries; verify 4-way agreement.
   {
      std::vector<tpch::q5::q5_agg_row_t> a, b, c, d;
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX();
         q5.query_by_base(a); q5.query_by_view(b);
         q5.query_by_merged(c); q5.query_by_hash(d);
         leanstore::cr::Worker::my().commitTX();
      });
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
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
      for (const auto& rec : inserted)
         apply_rf2_all(rec.key, rec.custkey, rec.linenumbers);
      leanstore::cr::Worker::my().commitTX();
   });
   std::cout << "[ok] applied " << N << " RF2 deletes (round-trip)\n";

   {
      std::vector<tpch::q5::q5_agg_row_t> a, b, c, d;
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX();
         q5.query_by_base(a); q5.query_by_view(b);
         q5.query_by_merged(c); q5.query_by_hash(d);
         leanstore::cr::Worker::my().commitTX();
      });
      uint64_t da = digest_rows(a), db = digest_rows(b),
               dc = digest_rows(c), dd = digest_rows(d);
      std::cout << "[post-RF2] S1=0x" << std::hex << da
                << " S2=0x" << db << " S3=0x" << dc
                << " S4=0x" << dd << std::dec << "\n";
      bool agree    = (da == dc && db == dc && dd == dc);
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

#endif  // ROCKSDB_ONLY
