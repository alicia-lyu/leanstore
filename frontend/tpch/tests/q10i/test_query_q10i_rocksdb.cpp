// Q10I correctness harness: loads base tables once, populates all
// secondaries (splits + merged_coli), asserts strict-equality cardinality
// on every secondary, sentinel-ordering check including invoice tag,
// and verifies cross-structure XOR digest parity.
//
// Phase 1 (commits 1-2): all four query_by_* return empty → digest 0x0 parity.
// Phase 1 commit 3: strict-equality cardinality assertions + sentinel check.
//
// IMPORTANT: this harness wipes --ssd_path before opening the DB. RocksDB
// does not cleanly overwrite an existing DB; reusing a populated dir across
// runs causes stale state and breaks consistency invariants. The wipe makes
// the harness re-runnable in place.
//
// Usage:
//   mkdir -p build/scratch/q10i/{data,csv}
//   ./build/frontend/test_query_q10i_lsm \
//       --ssd_path=./build/scratch/q10i/data \
//       --csv_path=./build/scratch/q10i/csv \
//       --tpch_scale_factor=1
//
// Expected (Phase 1):
//   - [OK] pipeline_view rows == 0 (deferred to Phase 4a Pattern B)
//   - [OK] strict-equality cardinality on splits + merged_coli
//   - [OK] sentinel ordering: customer < invoice < orders < lineitem per custkey group
//   - [OK] parity at digest 0x0 (stubs return empty)
//   - exit 0

#include <gflags/gflags.h>
#include <climits>
#include <cstdint>
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
#include "../../tpchi_family/coli_pipeline.hpp"
#include "../../tpchi_family/tpchi_tables.hpp"
#include "../../tpchi_family/tpchi_workload.hpp"
#include "../../q10i/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).

static uint64_t row_digest(const tpch::q10i::q10i_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.c_custkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue)       * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.paid_returns)  * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.open_returns)  * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.late_returns)  * 1e6);
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q10i::q10i_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q10I unified query test — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   using B = tpch::RocksDBBackend;

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);

   // All 8 TPC-H base tables + invoice.
   B::Adapter<part_t>       part(rocks_db);
   B::Adapter<supplier_t>   supplier(rocks_db);
   B::Adapter<partsupp_t>   partsupp(rocks_db);
   B::Adapter<customerh_t>  customer(rocks_db);
   B::Adapter<orders_t>     orders(rocks_db);
   B::Adapter<lineitem_i_t> lineitem(rocks_db);
   B::Adapter<nation_t>     nation(rocks_db);
   B::Adapter<region_t>     region(rocks_db);
   B::Adapter<invoice_t>    invoice(rocks_db);

   // Q10I-specific adapters.
   B::Adapter<tpch::q10i::q10i_pipeline_view_t> pipeline_view(rocks_db);
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // aCOLI (S5 deferred — required by Q10IWorkload ctor to keep CF set compatible).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli(rocks_db);

   // S2 variant B (per-order preagg view) + S5 Q10I aCOLI MI (2-type).
   B::Adapter<tpch::q10i::q10i_pipeline_view_preagg_t> pipeline_view_preagg(rocks_db);
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acoli_q10i_t> acoli_q10i(rocks_db);

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
   TPCHIWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                   orders, lineitem, nation, region, invoice, logger);

   tpch::q10i::Q10IWorkload<B> q10i(tpch, customer, orders, lineitem, invoice,
                                     nation, pipeline_view, merged_coli,
                                     split_orders, split_lineitem, split_invoice, acoli,
                                     pipeline_view_preagg, acoli_q10i);

   // Load base tables and populate ALL secondaries up front so all four
   // query_by_* paths see the same data.
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   tpch.load();

   std::cout << "=== Populating secondaries ===\n";
   q10i.coli_pipeline().populate_split();    // S1 splits
   q10i.coli_pipeline().populate_merged();   // S3 COLI MI
   q10i.populate_q10i_view();                // S2 view (Pattern B, per-lineitem)
   q10i.populate_q10i_view_preagg();         // S2-B per-order pre-aggregated view
   q10i.populate_q10i_acoli();               // S5 aCOLI MI
   // S4 needs no secondary.

   // ------------------------------------------------------------------
   // Per-secondary cardinality + size sanity check.

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

   long n_view    = count_typed(pipeline_view,  tpch::q10i::q10i_pipeline_view_t{});
   long n_split_o = count_typed(split_orders,   tpch::orders_coli_t{});
   long n_split_l = count_typed(split_lineitem, tpch::lineitem_coli_t{});
   long n_split_i = count_typed(split_invoice,  tpch::invoice_coli_t{});

   // Walk merged_coli once via a counting visitor that also checks sentinel
   // ordering: within each custkey group, byte-lex order must be
   //   customer (tag=1) → invoice* (tag=2) → (order → lineitem*)+ (tags 3,4).
   struct CountVisitor {
      long customers = 0, invoices = 0, orders = 0, lineitems = 0, groups = 0;
      long g_orders = 0, g_invoices = 0;
      long o_lineitems = 0;
      long min_o_per_g = LONG_MAX, max_o_per_g = 0, sum_o_per_g = 0;
      long min_i_per_g = LONG_MAX, max_i_per_g = 0, sum_i_per_g = 0;
      long min_l_per_o = LONG_MAX, max_l_per_o = 0, sum_l_per_o = 0, n_orders_for_l = 0;
      bool customer_seen_in_group = false;
      long sentinel_violations = 0;

      ::tpch::WalkAction on_customer(Integer, const tpch::customer_coli_t&) {
         ++customers;
         customer_seen_in_group = true;
         return ::tpch::WalkAction::Continue;
      }
      ::tpch::WalkAction on_invoice(const tpch::invoice_coli_t::Key&, const tpch::invoice_coli_t&) {
         ++invoices; ++g_invoices;
         if (!customer_seen_in_group) ++sentinel_violations;
         return ::tpch::WalkAction::Continue;
      }
      ::tpch::WalkAction on_order(const tpch::orders_coli_t::Key&, const tpch::orders_coli_t&) {
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
      ::tpch::WalkAction on_lineitem(const tpch::lineitem_coli_t::Key&, const tpch::lineitem_coli_t&) {
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
         min_i_per_g = std::min(min_i_per_g, g_invoices);
         max_i_per_g = std::max(max_i_per_g, g_invoices);
         sum_i_per_g += g_invoices;
         g_orders = g_invoices = o_lineitems = 0;
         customer_seen_in_group = false;
      }
   } cv;
   tpch::coli_group_walk<B>(merged_coli, cv);
   long n_merged_total = cv.customers + cv.invoices + cv.orders + cv.lineitems;

   // Ground-truth base-table cardinalities.
   long n_customers     = count_typed(customer, customerh_t{});
   long n_orders        = count_typed(orders,   orders_t{});
   long n_lineitems_ref = count_typed(lineitem, lineitem_i_t{});
   long n_invoices_ref  = count_typed(invoice,  invoice_t{});

   auto check = [](const char* label, bool ok, long got, const std::string& expected) {
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << label
                << " rows=" << std::setw(8) << got
                << " (expected " << expected << ")\n";
   };

   bool stats_ok = true;

   std::cout << "\n=== Secondary cardinality / size ===\n";
   std::cout << "[base] customers=" << n_customers
             << " orders=" << n_orders
             << " lineitems=" << n_lineitems_ref
             << " invoices=" << n_invoices_ref << "\n";

   // Pipeline view: Pattern B loader emits one row per lineitem.
   {
      bool ok = (n_view == n_lineitems_ref);
      stats_ok &= ok;
      check("pipeline_view", ok, n_view,
            "= " + std::to_string(n_lineitems_ref));
   }

   // Split adapters: strict 1:1 retagging of base tables.
   {
      bool ok = (n_split_o == n_orders);
      stats_ok &= ok;
      check("split_orders", ok, n_split_o, "= " + std::to_string(n_orders));
   }
   {
      bool ok = (n_split_l == n_lineitems_ref);
      stats_ok &= ok;
      check("split_lineitem", ok, n_split_l, "= " + std::to_string(n_lineitems_ref));
   }
   {
      bool ok = (n_split_i == n_invoices_ref);
      stats_ok &= ok;
      check("split_invoice", ok, n_split_i, "= " + std::to_string(n_invoices_ref));
   }

   // Merged COLI: total == sum of all four base tables; per-type breakdown
   // must each match the corresponding base count.
   {
      long expected = n_customers + n_orders + n_lineitems_ref + n_invoices_ref;
      bool total_ok = (n_merged_total == expected);
      bool c_ok     = (cv.customers == n_customers);
      bool o_ok     = (cv.orders    == n_orders);
      bool l_ok     = (cv.lineitems == n_lineitems_ref);
      bool i_ok     = (cv.invoices  == n_invoices_ref);
      bool ok = total_ok && c_ok && o_ok && l_ok && i_ok;
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << std::left << std::setw(22) << "merged_coli"
                << " rows=" << std::setw(8) << n_merged_total
                << " (c=" << cv.customers << " i=" << cv.invoices
                << " o=" << cv.orders << " l=" << cv.lineitems
                << " groups=" << cv.groups
                << ", expected " << expected << ")\n";
      if (!c_ok) std::cout << "       [FAIL] customers mismatch: got " << cv.customers << " expected " << n_customers << "\n";
      if (!o_ok) std::cout << "       [FAIL] orders mismatch: got " << cv.orders << " expected " << n_orders << "\n";
      if (!l_ok) std::cout << "       [FAIL] lineitems mismatch: got " << cv.lineitems << " expected " << n_lineitems_ref << "\n";
      if (!i_ok) std::cout << "       [FAIL] invoices mismatch: got " << cv.invoices << " expected " << n_invoices_ref << "\n";
   }

   // Per-adapter size.
   std::cout << "[size] pipeline_view=" << std::fixed << std::setprecision(3) << pipeline_view.size() << " MiB"
             << "  split_orders=" << split_orders.size() << " MiB"
             << "  split_lineitem=" << split_lineitem.size() << " MiB"
             << "  split_invoice=" << split_invoice.size() << " MiB"
             << "  merged_coli=" << merged_coli.size() << " MiB\n";

   // Content-bytes per row.
   auto rb_so    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::orders_coli_t::id);
   auto rb_sl    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::lineitem_coli_t::id);
   auto rb_si    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::invoice_coli_t::id);
   auto rb_merge = raw_bytes_in_cf(merged_coli.cf_handle, -1);

   auto cpr = [](long bytes, long rows) {
      return rows > 0 ? static_cast<double>(bytes) / static_cast<double>(rows) : 0.0;
   };
   std::cout << "[content/row] split_orders=" << std::fixed << std::setprecision(1)
             << cpr(rb_so.second, rb_so.first)
             << "  split_lineitem=" << cpr(rb_sl.second, rb_sl.first)
             << "  split_invoice="  << cpr(rb_si.second, rb_si.first)
             << "  merged_coli="    << cpr(rb_merge.second, rb_merge.first) << "\n";

   // Per-customer-group distribution.
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

   // Sentinel ordering: customer (tag=1) must precede invoice (tag=2) which
   // must precede orders (tag=3) and lineitems (tag=4) within each custkey group.
   {
      bool ok = (cv.sentinel_violations == 0);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "sentinel ordering"
                << " violations=" << cv.sentinel_violations << " (expected 0)\n";
   }

   // ------------------------------------------------------------------
   // Run all four query_by_* paths (stubs at Phase 1 — digest 0x0).

   std::cout << "\n=== Running queries (all four query_by_* live) ===\n";
   std::vector<tpch::q10i::q10i_agg_row_t> r_base, r_view, r_merged, r_hash;

   q10i.query_by_base  (r_base);
   q10i.query_by_view  (r_view);
   q10i.query_by_merged(r_merged);
   q10i.query_by_hash  (r_hash);

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

   // Parity check: all four digests must be equal (trivially true at 0x0 for stubs).
   std::cout << "\n=== Parity check ===\n";
   uint64_t ref  = d_merged;
   bool     ok_b = (d_base   == ref);
   bool     ok_v = (d_view   == ref);
   bool     ok_m = (d_merged == ref);
   bool     ok_h = (d_hash   == ref);

   auto parity_line = [&](const char* tag, bool ok, uint64_t d) {
      const char* status = ok ? "[OK]   " : "[FAIL] ";
      std::cout << status << tag
                << " digest=0x" << std::hex << d << std::dec;
      if (!ok) std::cout << "  (expected S3 0x" << std::hex << ref << std::dec << ")";
      std::cout << "\n";
   };
   parity_line("S1 base  ", ok_b, d_base);
   parity_line("S2 view  ", ok_v, d_view);
   parity_line("S3 merged", ok_m, d_merged);
   parity_line("S4 hash  ", ok_h, d_hash);

   bool parity_ok = ok_b && ok_v && ok_m && ok_h;
   if (parity_ok) {
      std::cout << "[OK] parity (S1 ≡ S2 ≡ S3 ≡ S4 at matching digest)\n";
   }

   // ------------------------------------------------------------------
   // S2-B (per-order preagg view) and S5 (aCOLI MI) A/B vs S3. Both must
   // produce the identical digest; the preagg view + aCOLI bake per-order
   // paid/open/late, the query reads them back date-filtered.
   std::cout << "\n=== S2-B (preagg view) + S5 (aCOLI) vs S3 ===\n";
   long n_view_preagg = count_typed(pipeline_view_preagg,
                                    tpch::q10i::q10i_pipeline_view_preagg_t{});
   std::cout << "[info] preagg view rows=" << n_view_preagg
             << "  (per-lineitem view rows=" << n_view << ")\n";

   FLAGS_q10i_view_variant = "preagg";
   std::vector<tpch::q10i::q10i_agg_row_t> r_view_preagg;
   q10i.query_by_view(r_view_preagg);
   FLAGS_q10i_view_variant = "lineitem";
   uint64_t d_view_preagg = digest_rows(r_view_preagg);
   bool ok_vp = (d_view_preagg == ref) && (r_view_preagg.size() == r_merged.size());

   std::vector<tpch::q10i::q10i_agg_row_t> r_agg;
   q10i.query_by_aggregated(r_agg);
   uint64_t d_agg = digest_rows(r_agg);
   bool ok_s5 = (d_agg == ref) && (r_agg.size() == r_merged.size());

   parity_line("S2-preagg", ok_vp, d_view_preagg);
   parity_line("S5 aCOLI ", ok_s5, d_agg);

   bool all_ok = stats_ok && parity_ok && ok_vp && ok_s5;
   if (!all_ok) {
      std::cerr << "\n[FAIL] One or more checks failed — see [FAIL] lines above.\n";
      return 1;
   }
   return 0;
}
