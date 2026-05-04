// Unified Phase-0.5 harness for Q3: loads ALL secondary structures once, then
// runs all four query_by_* paths and asserts cross-structure result parity.
//
// Phase-0.5 stub behaviour: all four query_by_* return empty vectors (0 rows).
// All four digests therefore equal 0 and trivially agree — [OK] parity at
// Phase 0.5 is expected and correct.  Real correctness signal arrives in
// Phase 1 when query_by_merged is implemented.
//
// Usage:
//   mkdir -p test_data_q3 test_csv_q3
//   ./build/frontend/test_query_q3_lsm \
//       --ssd_path=./test_data_q3 \
//       --csv_path=./test_csv_q3 \
//       --tpch_scale_factor=1
//
// Expected (Phase 0.5): 4 digests all 0x0, 4 × row_count=0, [OK] agreement,
// exit 0.
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
   // Parity check: all four digests must agree.
   std::cout << "\n=== Parity check ===\n";
   uint64_t ref  = d_merged;  // S3 is the oracle (first to be implemented)
   bool     ok_b = (d_base   == ref);
   bool     ok_v = (d_view   == ref);
   bool     ok_m = (d_merged == ref);
   bool     ok_h = (d_hash   == ref);

   auto parity_line = [&](const char* tag, bool ok, uint64_t d) {
      std::ostringstream ss;
      ss << std::hex << ref;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << tag
                << " digest=0x" << std::hex << d << std::dec
                << (ok ? "" : "  (expected 0x" + ss.str() + ")")
                << "\n";
   };
   parity_line("S1 base  ", ok_b, d_base);
   parity_line("S2 view  ", ok_v, d_view);
   parity_line("S3 merged", ok_m, d_merged);
   parity_line("S4 hash  ", ok_h, d_hash);

   // Shape note: at Phase 0.5, all stubs return 0 rows and digest 0.
   // row_count in (0, 10] is NOT required until Phase 1 bodies land.
   std::cout << "\n[note] Phase-0.5: all stubs return 0 rows — "
             << "digest=0 and [OK] agreement is expected.\n";

   bool all_ok = ok_b && ok_v && ok_m && ok_h;
   return all_ok ? 0 : 1;
}
