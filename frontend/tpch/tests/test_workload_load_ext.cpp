// End-to-end load contract test for TPCHIWorkload::loadInvoiceAndLinkLineitem.
//
// At SF=1, after tpchi.load(), verifies:
//   1. Every lineitem_i_t row has l_invoicekey != 0 (all lineitems linked).
//   2. Every l_invoicekey resolves to a present invoice_t row (FK closure).
//   3. Per-customer invariant: each lineitem's invoice is owned by the same
//      custkey as the lineitem's parent order (sample 100 lineitems).
//   4. Invoice count is ~2 × order_count, ±10% tolerance.
//
// CMake target: test_workload_load_ext
// Build:  cmake --build build --target test_workload_load_ext -j$(sysctl -n hw.ncpu)
// Run:    mkdir -p test_data_unit/test_workload_load_ext test_csv_unit/test_workload_load_ext
//         ./build/frontend/test_workload_load_ext \
//             --ssd_path=./test_data_unit/test_workload_load_ext \
//             --csv_path=./test_csv_unit/test_workload_load_ext \
//             --tpch_scale_factor=1

#include <gflags/gflags.h>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../../shared/RocksDB.hpp"
#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
#include "../backend.hpp"
#include "../tpchi_family/tpchi_tables.hpp"
#include "../tpchi_family/tpchi_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// Helper: pass/fail tag

static const char* pass(bool ok) { return ok ? "[OK]  " : "[FAIL]"; }

// ---------------------------------------------------------------------------
// count_rows: scan an adapter and return the number of rows.

template <typename Adapter>
static long count_rows(Adapter& adapter)
{
   long n = 0;
   auto sc = adapter.getScanner();
   while (sc->next()) ++n;
   return n;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("TPCHIWorkload load contract test — RocksDB");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // All 8 TPC-H base tables required by TPCHWorkload.
   B::Adapter<part_t>       part(rocks_db);
   B::Adapter<supplier_t>   supplier(rocks_db);
   B::Adapter<partsupp_t>   partsupp(rocks_db);
   B::Adapter<customerh_t>  customer(rocks_db);
   B::Adapter<orders_t>     orders(rocks_db);
   B::Adapter<lineitem_i_t> lineitem(rocks_db);
   B::Adapter<nation_t>     nation(rocks_db);
   B::Adapter<region_t>     region(rocks_db);
   B::Adapter<invoice_t>    invoice(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHIWorkload<B::Adapter> tpchi(part, supplier, partsupp, customer,
                                   orders, lineitem, nation, region, invoice, logger);
   tpchi.load();

   const int sf = FLAGS_tpch_scale_factor;
   bool any_fail = false;

   std::cout << "\n=== TPCHIWorkload load contract (scale_factor=" << sf << ") ===\n";

   // -----------------------------------------------------------------------
   // Case 1: every lineitem_i_t has l_invoicekey != 0.

   long total_lineitems = 0;
   long unlinked        = 0;

   lineitem.scan(
       lineitem_i_t::Key{std::numeric_limits<Integer>::min(),
                         std::numeric_limits<Integer>::min()},
       [&](const lineitem_i_t::Key& /*k*/, const lineitem_i_t& l) {
          ++total_lineitems;
          if (l.l_invoicekey == 0) ++unlinked;
          return true;
       },
       []() {});

   bool case1_ok = (unlinked == 0 && total_lineitems > 0);
   std::cout << pass(case1_ok)
             << " Case 1: all lineitems linked — "
             << total_lineitems << " rows, " << unlinked << " unlinked\n";
   if (!case1_ok) any_fail = true;

   // -----------------------------------------------------------------------
   // Case 2: every l_invoicekey resolves to a present invoice_t (FK closure).
   //
   // Build the set of valid invoicekeys from an invoice scan, then verify
   // every invoicekey referenced by a lineitem is in that set.

   std::unordered_set<Integer> valid_invoicekeys;
   invoice.scan(
       invoice_t::Key{std::numeric_limits<Integer>::min()},
       [&](const invoice_t::Key& k, const invoice_t& /*v*/) {
          valid_invoicekeys.insert(k.i_invoicekey);
          return true;
       },
       []() {});

   long fk_misses = 0;
   lineitem.scan(
       lineitem_i_t::Key{std::numeric_limits<Integer>::min(),
                         std::numeric_limits<Integer>::min()},
       [&](const lineitem_i_t::Key& /*k*/, const lineitem_i_t& l) {
          if (valid_invoicekeys.find(l.l_invoicekey) == valid_invoicekeys.end())
             ++fk_misses;
          return true;
       },
       []() {});

   bool case2_ok = (fk_misses == 0);
   std::cout << pass(case2_ok)
             << " Case 2: FK closure — " << valid_invoicekeys.size()
             << " distinct invoice keys, " << fk_misses << " unresolved\n";
   if (!case2_ok) any_fail = true;

   // -----------------------------------------------------------------------
   // Case 3: per-customer invariant — each lineitem's invoice belongs to the
   // same custkey as the lineitem's parent order.
   //
   // Sample 100 lineitems (deterministic seed) and for each:
   //   a. Look up the parent order to get o_custkey.
   //   b. Look up the invoice to get i_custkey.
   //   c. Assert o_custkey == i_custkey.

   // Collect all (key, l_invoicekey) pairs into a vector for sampling.
   struct LiSample {
      lineitem_i_t::Key key;
      Integer           invoicekey;
   };
   std::vector<LiSample> all_li;
   all_li.reserve(static_cast<size_t>(total_lineitems));

   lineitem.scan(
       lineitem_i_t::Key{std::numeric_limits<Integer>::min(),
                         std::numeric_limits<Integer>::min()},
       [&](const lineitem_i_t::Key& k, const lineitem_i_t& l) {
          all_li.push_back({k, l.l_invoicekey});
          return true;
       },
       []() {});

   constexpr int SAMPLE_SIZE = 100;
   std::mt19937 rng(42);  // fixed seed for determinism
   std::uniform_int_distribution<size_t> dist(0, all_li.size() - 1);

   long custkey_mismatches = 0;
   long samples_checked    = 0;
   const int actual_samples = std::min(SAMPLE_SIZE, static_cast<int>(all_li.size()));

   for (int i = 0; i < actual_samples; ++i) {
      const LiSample& ls = all_li[dist(rng)];

      // Resolve parent order → custkey.
      orders_t parent_order{};
      bool order_found = false;
      orders.lookup1(orders_t::Key{ls.key.l_orderkey},
                     [&](const orders_t& o) { parent_order = o; order_found = true; });
      if (!order_found) { ++custkey_mismatches; continue; }

      // Resolve invoice → custkey.
      invoice_t inv{};
      bool invoice_found = false;
      invoice.lookup1(invoice_t::Key{ls.invoicekey},
                      [&](const invoice_t& iv) { inv = iv; invoice_found = true; });
      if (!invoice_found) { ++custkey_mismatches; continue; }

      if (parent_order.o_custkey != inv.i_custkey) ++custkey_mismatches;
      ++samples_checked;
   }

   bool case3_ok = (custkey_mismatches == 0);
   std::cout << pass(case3_ok)
             << " Case 3: per-customer invariant — " << samples_checked
             << " sampled, " << custkey_mismatches << " custkey mismatches\n";
   if (!case3_ok) any_fail = true;

   // -----------------------------------------------------------------------
   // Case 4: invoice count ≈ 2 × order_count, ±10%.

   const long order_count   = count_rows(orders);
   const long invoice_count = static_cast<long>(valid_invoicekeys.size());

   const long inv_lo = static_cast<long>(order_count * 2 * 0.90);
   const long inv_hi = static_cast<long>(order_count * 2 * 1.10);
   bool case4_ok = (invoice_count >= inv_lo && invoice_count <= inv_hi);

   std::cout << pass(case4_ok)
             << " Case 4: invoice count ~2×orders — "
             << invoice_count << " invoices, " << order_count << " orders"
             << "  (expected [" << inv_lo << ", " << inv_hi << "])\n";
   if (!case4_ok) any_fail = true;

   std::cout << "\n";
   return any_fail ? 1 : 0;
}
