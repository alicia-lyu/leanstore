#ifndef ROCKSDB_ONLY
// Phase 1 commit 2 harness for Q10 — LeanStore backend.
//
// Mirrors test_query_q10_rocksdb.cpp: loads base tables + COL secondaries
// once, runs all four query_by_* paths (all stubs at this commit), and
// asserts cross-structure XOR digest parity. Expected: all four digests
// are 0x0, exit 0.
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

   B::Adapter<tpch::q10::q10_pipeline_view_t> pipeline_view;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col;
   B::Adapter<tpch::orders_coli_t>        split_orders;
   B::Adapter<tpch::lineitem_col_t>       split_lineitem;

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
      merged_col     = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                        tpch::lineitem_col_t>(db, "col_merged");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "col_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "col_split_lineitem");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q10::Q10Workload<B> q10(tpch, customer, orders, lineitem, nation,
                                  pipeline_view, merged_col,
                                  split_orders, split_lineitem);

   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      q10.load();
      leanstore::cr::Worker::my().commitTX();
   });

   std::cout << "=== Running queries (S3 live; S1/S2/S4 stubs) ===\n";
   std::vector<tpch::q10::q10_agg_row_t> r_base, r_view, r_merged, r_hash;
   tpch::q10::Q10Stats s3_stats{};
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      q10.stats = &s3_stats;
      q10.query_by_base  (r_base);
      q10.query_by_view  (r_view);
      q10.query_by_merged(r_merged);
      q10.query_by_hash  (r_hash);
      q10.stats = nullptr;
      leanstore::cr::Worker::my().commitTX();
   });

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

   std::cout << "\n=== Q10Stats post-S3 ===\n";
   std::cout << "[stat] customers_scanned            = " << s3_stats.customers_scanned << "\n";
   std::cout << "[stat] orders_scanned               = " << s3_stats.orders_scanned << "\n";
   std::cout << "[stat] orders_passing_date          = " << s3_stats.orders_passing_date << "\n";
   std::cout << "[stat] lineitems_scanned            = " << s3_stats.lineitems_scanned << "\n";
   std::cout << "[stat] lineitems_passing_returnflag = " << s3_stats.lineitems_passing_returnflag << "\n";
   std::cout << "[stat] aggregator_rows_out          = " << s3_stats.aggregator_rows_out << "\n";
   std::cout << "[stat] topn_offers                  = " << s3_stats.topn_offers << "\n";
   std::cout << "[stat] topn_evictions               = " << s3_stats.topn_evictions << "\n";
   std::cout << "[stat] nation_inl_lookups           = " << s3_stats.nation_inl_lookups << "\n";

   std::cout << "\n=== Parity check ===\n";
   bool s3_sanity = (d_merged != 0);
   std::cout << (s3_sanity ? "[OK]   " : "[FAIL] ") << "S3 sanity"
             << " digest=0x" << std::hex << d_merged << std::dec
             << " (expected non-zero; got "
             << r_merged.size() << " rows)\n";

   // S1 vs S3 parity (Phase 4b commit 1).
   bool s1_parity = (d_base == d_merged) && (r_base.size() == r_merged.size());
   std::cout << (s1_parity ? "[OK]   " : "[FAIL] ") << "S1 vs S3 parity"
             << " d_base=0x"   << std::hex << d_base
             << " d_merged=0x" << d_merged << std::dec
             << " (rows: " << r_base.size() << " vs " << r_merged.size() << ")\n";

   // S2 vs S3 parity (Phase 4b commit 2).
   bool s2_parity = (d_view == d_merged) && (r_view.size() == r_merged.size());
   std::cout << (s2_parity ? "[OK]   " : "[FAIL] ") << "S2 vs S3 parity"
             << " d_view=0x"   << std::hex << d_view
             << " d_merged=0x" << d_merged << std::dec
             << " (rows: " << r_view.size() << " vs " << r_merged.size() << ")\n";

   auto deferred_line = [](const char* tag, uint64_t d, size_t n,
                           const char* phase) {
      std::cout << "[DEFER] " << tag
                << " digest=0x" << std::hex << d << std::dec
                << " rows=" << n
                << "  (stub; query body lands in Phase 4 " << phase << ")\n";
   };
   deferred_line("S4 hash  ", d_hash, r_hash.size(), "§7.5");

   return (s3_sanity && s1_parity && s2_parity) ? 0 : 1;
}

#endif  // ROCKSDB_ONLY
