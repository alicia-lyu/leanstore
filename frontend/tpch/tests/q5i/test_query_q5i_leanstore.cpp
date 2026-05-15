#ifndef ROCKSDB_ONLY
// Phase 0.5 harness for Q5I — LeanStore backend.
//
// Mirrors test_query_q5i_rocksdb.cpp: loads base tables once, runs all four
// query_by_* paths (all stubs at Phase 0.5), and asserts cross-structure
// XOR digest parity.  Expected result: all four digests are 0x0, exit 0.
//
// Differences from the RocksDB harness:
//   - No filesystem wipe of --ssd_path: LeanStore opens fresh each run.
//   - All work runs inside crm.scheduleJobSync(0, ...) closures.
//
// Usage:
//   ./build/frontend/test_query_q5i_btree \
//       --ssd_path=/mnt/ssd/q5i_btree \
//       --tpch_scale_factor=1

#include <gflags/gflags.h>
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

   // Run all four paths inside one OLAP transaction.
   std::cout << "=== Running queries ===\n";
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

   bool all_ok = ok_b && ok_v && ok_m && ok_h;
   if (all_ok) {
      std::cout << "[OK] parity\n";
      return 0;
   }
   return 1;
}

#endif  // ROCKSDB_ONLY
