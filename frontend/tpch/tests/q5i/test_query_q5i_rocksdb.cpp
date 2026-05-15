// Phase 1 harness for Q5I: loads base tables once, populates all secondaries,
// asserts strict-equality cardinality on base + splits + merged_coli, reports
// pipeline_view rows == 0 (deferred to Phase 4a — Pattern B view loader
// reuses S3 walker), and verifies cross-structure XOR digest parity at 0x0
// (all four query_by_* return empty until Phase 4 bodies land).
//
// IMPORTANT: this harness wipes --ssd_path before opening the DB. RocksDB
// does not cleanly overwrite an existing DB; reusing a populated dir across
// runs causes stale state and breaks consistency invariants.  The wipe makes
// the harness re-runnable in place.
//
// Usage:
//   mkdir -p build/scratch/q5i/{data,csv}
//   ./build/frontend/test_query_q5i_lsm \
//       --ssd_path=./build/scratch/q5i/data \
//       --csv_path=./build/scratch/q5i/csv \
//       --tpch_scale_factor=1
//
// Expected:
//   - strict-equality [OK] cardinality on splits + merged_coli
//   - sentinel ordering [OK]
//   - pipeline_view rows == 0 [OK] (deferred to Phase 4a)
//   - parity [OK] at digest 0x0 (query bodies stub)
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
#include "../../q5i/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).
//
// Each field is rotated-left then XORed in so single-field errors flip the
// digest.  n_name is hashed via std::hash for a stable integer projection.

static uint64_t row_digest(const tpch::q5i::q5i_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ std::hash<std::string>{}(r.n_name);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.nominal_revenue)   * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.realised_revenue)  * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.open_revenue)      * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.late_revenue)      * 1e6);
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
   gflags::SetUsageMessage("Q5I unified query test — RocksDB backend");
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

   // Q5I-specific adapters.
   B::Adapter<tpch::q5i::q5i_pipeline_view_t> pipeline_view(rocks_db);
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
   B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
   B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
   B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

   // aCOLI (S5 deferred — required by Q5IWorkload ctor).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_coli_t,
                    tpch::lineitem_acoli_t> acoli(rocks_db);

   // Defensive wipe: if --ssd_path holds a prior DB, remove it before opening.
   // See file-header comment for why this matters.
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

   tpch::q5i::Q5IWorkload<B> q5i(tpch, customer, orders, lineitem, invoice,
                                   supplier, nation, region,
                                   pipeline_view, merged_coli,
                                   split_orders, split_lineitem, split_invoice, acoli);

   // Load base tables and populate ALL secondaries up front so all four
   // query_by_* paths see the same data.
   std::cout << "=== Loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
   tpch.load();

   std::cout << "=== Populating secondaries ===\n";
   // S2 view: Pattern B — deferred to Phase 4a (reuses S3 walker).
   q5i.coli_pipeline().populate_split();    // S1 splits
   q5i.coli_pipeline().populate_merged();   // S3 MI
   // S4 needs no secondary.

   // ------------------------------------------------------------------
   // Per-secondary cardinality + size sanity check.
   //
   // Strict equality: splits must be 1:1 retaggings of base tables.
   // merged_coli total must equal |customers| + |orders| + |lineitems| + |invoices|
   // (verified via coli_group_walk counting visitor).

   // count_typed: scan an adapter and count all records of type R.
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

   // raw_bytes_in_cf: walk a CF (optionally filtered by 1-byte prefix) and
   // sum key.size() + value.size() per row.  Gives like-for-like content
   // bytes across adapters regardless of per-CF SST metadata overhead.
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

   long n_view     = count_typed(pipeline_view,  tpch::q5i::q5i_pipeline_view_t{});
   long n_split_o  = count_typed(split_orders,   tpch::orders_coli_t{});
   long n_split_l  = count_typed(split_lineitem, tpch::lineitem_coli_t{});
   long n_split_i  = count_typed(split_invoice,  tpch::invoice_coli_t{});

   // Walk merged_coli once via a counting Visitor that also checks sentinel
   // ordering (customer must precede invoice/orders/lineitems in each group).
   struct CountVisitor {
      long customers = 0, invoices = 0, orders = 0, lineitems = 0, groups = 0;
      // Per-group running counters (reset on each on_group_end).
      long g_orders = 0, g_invoices = 0;
      // Per-order running lineitem counter (reset on each on_order).
      long o_lineitems = 0;
      // Distribution stats.
      long min_o_per_g = LONG_MAX, max_o_per_g = 0, sum_o_per_g = 0;
      long min_i_per_g = LONG_MAX, max_i_per_g = 0, sum_i_per_g = 0;
      long min_l_per_o = LONG_MAX, max_l_per_o = 0, sum_l_per_o = 0, n_orders_for_l = 0;
      // Sentinel ordering: customer must arrive before all others in the group.
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

   // Ground-truth base-table cardinalities: count directly, not via sparse max keys.
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

   // Pipeline view: deferred to Phase 4a (Pattern B — view loader reuses S3
   // walker). Rows must be 0 at Phase 1. This is expected and correct; do not
   // treat it as a regression.
   {
      bool ok = (n_view == 0);
      stats_ok &= ok;
      if (ok) {
         std::cout << "[OK]   pipeline_view           rows=0"
                   << " (deferred to Phase 4a — Pattern B view loader)\n";
      } else {
         std::cout << "[FAIL] pipeline_view           rows=" << n_view
                   << " (expected 0 at Phase 1; view loader lands in Phase 4a)\n";
      }
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

   // Merged COLI: total == sum of all four base tables; per-type breakdown each
   // matches the corresponding base count.
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

   // Per-adapter RocksDB-reported size and content-bytes per row.
   std::cout << "[size] pipeline_view=" << std::fixed << std::setprecision(3) << pipeline_view.size() << " MiB"
             << "  split_orders=" << split_orders.size() << " MiB"
             << "  split_lineitem=" << split_lineitem.size() << " MiB"
             << "  split_invoice=" << split_invoice.size() << " MiB"
             << "  merged_coli=" << merged_coli.size() << " MiB\n";

   // Truthful content bytes (raw key+value sizes summed across the iterator).
   auto rb_view  = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::q5i::q5i_pipeline_view_t::id);
   auto rb_so    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::orders_coli_t::id);
   auto rb_sl    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::lineitem_coli_t::id);
   auto rb_si    = raw_bytes_in_cf(rocks_db.cf_handles[0], tpch::invoice_coli_t::id);
   auto rb_merge = raw_bytes_in_cf(merged_coli.cf_handle, -1);

   auto cpr = [](long bytes, long rows) {
      return rows > 0 ? static_cast<double>(bytes) / static_cast<double>(rows) : 0.0;
   };
   std::cout << "[content/row] pipeline_view=" << std::fixed << std::setprecision(1)
             << cpr(rb_view.second,  rb_view.first)
             << "  split_orders="   << cpr(rb_so.second,    rb_so.first)
             << "  split_lineitem=" << cpr(rb_sl.second,    rb_sl.first)
             << "  split_invoice="  << cpr(rb_si.second,    rb_si.first)
             << "  merged_coli="    << cpr(rb_merge.second, rb_merge.first) << "\n";

   // Per-customer-group MI distribution.
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

   // Sentinel ordering: customer must precede invoice/orders/lineitems in each group.
   {
      bool ok = (cv.sentinel_violations == 0);
      stats_ok &= ok;
      std::cout << (ok ? "[OK]   " : "[FAIL] ") << "sentinel ordering"
                << " violations=" << cv.sentinel_violations << " (expected 0)\n";
   }

   // ------------------------------------------------------------------
   // Run all four query_by_* paths (all stubs at Phase 1 — digest 0x0).

   std::cout << "\n=== Running queries (Phase 1 stubs — expect digest 0x0) ===\n";
   std::vector<tpch::q5i::q5i_agg_row_t> r_base, r_view, r_merged, r_hash;

   q5i.query_by_base  (r_base);
   q5i.query_by_view  (r_view);
   q5i.query_by_merged(r_merged);
   q5i.query_by_hash  (r_hash);

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

   // Parity check: all four digests must match (all 0x0 at Phase 1).
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
   if (parity_ok) {
      std::cout << "[OK] parity\n";
   }

   bool all_ok = stats_ok && parity_ok;
   if (!all_ok) {
      std::cerr << "\n[FAIL] One or more checks failed — see [FAIL] lines above.\n";
      return 1;
   }
   return 0;
}
