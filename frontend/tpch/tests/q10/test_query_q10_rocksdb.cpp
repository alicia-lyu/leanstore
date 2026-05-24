// Phase 1 commit 3 harness for Q10: loads base tables once, populates all
// secondaries (split + merged via COL; view deferred per Pattern B),
// asserts strict-equality cardinality on splits + merged_col, checks
// sentinel ordering inside merged_col, reports pipeline_view rows == 0
// (deferred to Phase 4a), and verifies cross-structure XOR digest parity
// at 0x0 (all four query_by_* return empty until Phase 4 bodies land).
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
//
// Expected:
//   - strict-equality [OK] cardinality on splits + merged_col
//   - sentinel ordering [OK]
//   - pipeline_view rows == 0 [OK] (deferred to Phase 4a, Pattern B)
//   - parity [OK] at digest 0x0 (query bodies stub)
//   - exit 0

#include <gflags/gflags.h>
#include <climits>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../tpch_family/col_pipeline.hpp"
#include "../../tpch_family/walk_action.hpp"
#include "../../tpch_workload.hpp"
#include "../../q10/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check). Each field
// is rotated-left then XORed in so single-field errors flip the digest.
// std::string fields are folded via std::hash for a stable integer.
//
// Phase 1 commit 3 widens the digest to cover the FD output columns + n_name
// so that when Phase 4 bodies land, cross-structure parity catches both
// revenue mismatches and FD-col mismatches. With current stubs all rows are
// absent, so the digest is trivially 0x0.

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

// ---------------------------------------------------------------------------
// CountVisitor: walks merged_col counting per-record-type rows, per-group
// distribution stats, and sentinel-ordering violations. Customer must be
// the first record in each custkey group (sentinel ordering: customer=1
// sorts before orders=3 and lineitem=4 by byte-lex within the group).

struct CountVisitor {
   long customers = 0, orders = 0, lineitems = 0, groups = 0;
   long g_orders = 0, o_lineitems = 0;
   long min_o_per_g = LONG_MAX, max_o_per_g = 0, sum_o_per_g = 0;
   long min_l_per_o = LONG_MAX, max_l_per_o = 0, sum_l_per_o = 0, n_orders_for_l = 0;
   bool customer_seen_in_group = false;
   long sentinel_violations = 0;

   ::tpch::WalkAction on_customer(Integer, const tpch::customer_coli_t&) {
      ++customers;
      customer_seen_in_group = true;
      return ::tpch::WalkAction::Continue;
   }
   ::tpch::WalkAction on_order(const tpch::orders_coli_t::Key&,
                               const tpch::orders_coli_t&) {
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
   ::tpch::WalkAction on_lineitem(const tpch::lineitem_col_t::Key&,
                                  const tpch::lineitem_col_t&) {
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
      g_orders = o_lineitems = 0;
      customer_seen_in_group = false;
   }
};

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

   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   q10.load();
   // load() populates split + merged unconditionally; S2 view is the
   // Pattern-B TODO (Phase 4a). Asserted as rows == 0 below.

   // ------------------------------------------------------------------
   // Per-secondary cardinality + sentinel-ordering checks.

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

   long n_view    = count_typed(pipeline_view,  tpch::q10::q10_pipeline_view_t{});
   long n_split_o = count_typed(split_orders,   tpch::orders_coli_t{});
   long n_split_l = count_typed(split_lineitem, tpch::lineitem_col_t{});

   CountVisitor cv;
   tpch::col_group_walk<B>(merged_col, cv);
   long n_merged_total = cv.customers + cv.orders + cv.lineitems;

   long n_customers     = count_typed(customer, customerh_t{});
   long n_orders        = count_typed(orders,   orders_t{});
   long n_lineitems_ref = count_typed(lineitem, lineitem_t{});

   auto check = [](const char* label, bool ok, long got, const std::string& expected) {
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << label
                << " rows=" << std::setw(8) << got
                << " (expected " << expected << ")\n";
   };

   bool stats_ok = true;

   std::cout << "\n=== Secondary cardinality ===\n";
   std::cout << "[base] customers=" << n_customers
             << " orders=" << n_orders
             << " lineitems=" << n_lineitems_ref << "\n";

   // Pipeline view: deferred to Phase 4a (Pattern B — view loader reuses
   // S3 walker). Rows must be 0 at Phase 1; this is expected and correct,
   // not a regression.
   {
      bool ok = (n_view == 0);
      stats_ok &= ok;
      if (ok) {
         std::cout << "[OK]   pipeline_view          rows=0"
                   << " (deferred to Phase 4a — Pattern B view loader)\n";
      } else {
         std::cout << "[FAIL] pipeline_view          rows=" << n_view
                   << " (expected 0 at Phase 1; view loader lands in Phase 4a)\n";
      }
   }

   // Split adapters: strict 1:1 retagging of base tables.
   {
      bool ok = (n_split_o == n_orders);
      stats_ok &= ok;
      check("split_orders",   ok, n_split_o, "= " + std::to_string(n_orders));
   }
   {
      bool ok = (n_split_l == n_lineitems_ref);
      stats_ok &= ok;
      check("split_lineitem", ok, n_split_l, "= " + std::to_string(n_lineitems_ref));
   }

   // Merged COL: total == customers + orders + lineitems; per-type matches.
   {
      long expected = n_customers + n_orders + n_lineitems_ref;
      bool total_ok = (n_merged_total == expected);
      bool c_ok     = (cv.customers == n_customers);
      bool o_ok     = (cv.orders    == n_orders);
      bool l_ok     = (cv.lineitems == n_lineitems_ref);
      bool ok = total_ok && c_ok && o_ok && l_ok;
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << "merged_col"
                << " rows=" << std::setw(8) << n_merged_total
                << " (c=" << cv.customers << " o=" << cv.orders << " l=" << cv.lineitems
                << " groups=" << cv.groups
                << ", expected " << expected << ")\n";
      if (!c_ok) std::cout << "       [FAIL] customers mismatch: got " << cv.customers << " expected " << n_customers << "\n";
      if (!o_ok) std::cout << "       [FAIL] orders mismatch: got "    << cv.orders    << " expected " << n_orders << "\n";
      if (!l_ok) std::cout << "       [FAIL] lineitems mismatch: got " << cv.lineitems << " expected " << n_lineitems_ref << "\n";
   }

   std::cout << "[size] pipeline_view=" << std::fixed << std::setprecision(3) << pipeline_view.size() << " MiB"
             << "  split_orders="   << split_orders.size()   << " MiB"
             << "  split_lineitem=" << split_lineitem.size() << " MiB"
             << "  merged_col="     << merged_col.size()     << " MiB\n";

   // Per-customer-group MI distribution.
   std::cout << "\n=== merged_col per-group distribution ===\n";
   auto dist_line = [](const char* label, long min_v, long max_v, long sum_v, long n) {
      double mean = (n > 0) ? (double)sum_v / (double)n : 0.0;
      std::cout << "[dist] " << std::left << std::setw(22) << label
                << "min=" << std::right << std::setw(4) << (min_v == LONG_MAX ? 0 : min_v)
                << " max=" << std::setw(5) << max_v
                << " mean=" << std::fixed << std::setprecision(2) << mean
                << " (n=" << n << ")\n";
   };
   dist_line("orders/customer", cv.min_o_per_g, cv.max_o_per_g, cv.sum_o_per_g, cv.groups);
   dist_line("lineitems/order", cv.min_l_per_o, cv.max_l_per_o, cv.sum_l_per_o, cv.n_orders_for_l);

   // Sentinel ordering: customer must precede orders/lineitems in each group.
   {
      bool ok = (cv.sentinel_violations == 0);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "sentinel ordering"
                << " violations=" << cv.sentinel_violations << " (expected 0)\n";
   }

   // ------------------------------------------------------------------
   // Run all four query_by_* paths (all stubs at Phase 1 — digest 0x0).

   std::cout << "\n=== Running queries (Phase 1 stubs — expect digest 0x0) ===\n";
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

   if (stats_ok && parity_ok) {
      return 0;
   }
   if (!stats_ok)  std::cout << "[FAIL] cardinality / sentinel check failed\n";
   if (!parity_ok) std::cout << "[FAIL] digest parity check failed\n";
   return 1;
}
