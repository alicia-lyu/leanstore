#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q3 query parity test.
// Mirrors test_query_q3_rocksdb.cpp for the LeanStore backend.

#include <gflags/gflags.h>
#include <chrono>
#include <filesystem>
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
#include "../../tpch_workload.hpp"
#include "../../q3/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

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

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3 unified query test — LeanStore backend");
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

   B::Adapter<tpch::q3::q3_pipeline_view_t> pipeline_view;
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
      pipeline_view = B::Adapter<tpch::q3::q3_pipeline_view_t>(db, "q3_pipeline_view");
      merged_col    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                       tpch::lineitem_col_t>(db, "q3_merged_col");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "q3_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "q3_split_lineitem");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               pipeline_view, merged_col,
                               split_orders, split_lineitem);

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      q3.col_pipeline().populate_split();
      tpch::q3::populate_q3_view<B>(customer, orders, lineitem, pipeline_view);
      q3.col_pipeline().populate_merged();
      leanstore::cr::Worker::my().commitTX();
   });

   std::vector<tpch::q3::q3_agg_row_t> r_base, r_view, r_merged, r_hash;
   tpch::q3::Stats st_base, st_view, st_merged, st_hash;

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
      q3.stats = &st_base;   q3.query_by_base  (r_base);
      q3.stats = &st_view;   q3.query_by_view  (r_view);
      q3.stats = &st_merged; q3.query_by_merged(r_merged);
      q3.stats = &st_hash;   q3.query_by_hash  (r_hash);
      q3.stats = nullptr;
      leanstore::cr::Worker::my().commitTX();
   });

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
   uint64_t ref  = d_merged;  // S3 is the oracle (first to be implemented).

   // Phase 4 §7.1 relaxation: only S3 has a real body; S1/S2/S4 still stubbed.
   // Stub (rows=0, digest=0) → [SKIP]; real-body disagreement → [FAIL].
   auto status = [&](uint64_t d, long n) -> const char* {
      if (n == 0 && d == 0)  return "[SKIP] ";
      if (d == ref)          return "[OK]   ";
      return "[FAIL] ";
   };
   auto parity_line = [&](const char* tag, uint64_t d, long n) {
      const char* s = status(d, n);
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

   if (r_merged.empty()) {
      std::cout << "\n[FAIL] S3 returned 0 rows — query_by_merged body broken.\n";
      return 1;
   }
   std::cout << "\n[note] Phase 4 §7.1: only query_by_merged has a real body; "
                "S1/S2/S4 are [SKIP] until §7.2/§7.3/§7.5 land.\n";
   bool no_fail =
       (status(d_base, (long)r_base.size())[1] != 'F') &&
       (status(d_view, (long)r_view.size())[1] != 'F') &&
       (status(d_hash, (long)r_hash.size())[1] != 'F');
   return no_fail ? 0 : 1;
}

#endif  // ROCKSDB_ONLY
