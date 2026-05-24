// Phase 1 commit 2 harness for Q10: loads base tables once, runs all four
// query_by_* paths (all stubs returning empty at this commit), and asserts
// cross-structure XOR digest parity. Expected: all four digests are 0x0
// and the harness exits 0 with "[OK] parity".
//
// Strict-equality cardinality assertions (split_orders / split_lineitem /
// merged_col per-type breakdown + sentinel-ordering) land in Phase 1
// commit 3 alongside the schema widening.
//
// IMPORTANT: this harness wipes --ssd_path before opening the DB. RocksDB
// does not cleanly overwrite an existing DB; reusing a populated dir across
// runs causes stale state. The wipe keeps the harness re-runnable in place.
//
// Usage:
//   mkdir -p build/scratch/q10/{data,csv}
//   ./build/frontend/test_query_q10_lsm \
//       --ssd_path=./build/scratch/q10/data \
//       --csv_path=./build/scratch/q10/csv \
//       --tpch_scale_factor=1

#include <gflags/gflags.h>
#include <cstdint>
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
#include "../../tpch_workload.hpp"
#include "../../q10/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).
// Each field is rotated-left then XORed in so single-field errors flip the
// digest.

static uint64_t row_digest(const tpch::q10::q10_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.c_custkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q10::q10_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q10 unified query test — RocksDB backend");
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

   // Q10-specific view.
   B::Adapter<tpch::q10::q10_pipeline_view_t> pipeline_view(rocks_db);

   // COL pipeline (shared with Q3 / Q5).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col(rocks_db);
   B::Adapter<tpch::orders_coli_t>        split_orders(rocks_db);
   B::Adapter<tpch::lineitem_col_t>       split_lineitem(rocks_db);

   // Defensive wipe: if --ssd_path holds a prior DB, remove it before opening.
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
                                  orders, lineitem, nation, region, logger);
   tpch::q10::Q10Workload<B> q10(tpch, customer, orders, lineitem, nation,
                                  pipeline_view, merged_col,
                                  split_orders, split_lineitem);

   // Load base tables + COL secondaries (S1 split + S3 merged).
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   q10.load();

   // Run all four paths. All stubs return empty in Phase 1 commit 2.
   std::cout << "=== Running queries ===\n";
   std::vector<tpch::q10::q10_agg_row_t> r_base, r_view, r_merged, r_hash;

   q10.query_by_base  (r_base);
   q10.query_by_view  (r_view);
   q10.query_by_merged(r_merged);
   q10.query_by_hash  (r_hash);

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

   // Parity check — S3 is the reference.
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

   bool all_ok = ok_b && ok_v && ok_m && ok_h;
   if (all_ok) {
      std::cout << "[OK] parity\n";
      return 0;
   }
   return 1;
}
