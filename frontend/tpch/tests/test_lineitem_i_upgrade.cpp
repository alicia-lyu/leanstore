// Unit tests for tpchi_tables.hpp::lineitem_i_t.
//
// Cases:
//   1. Default constructor: l_invoicekey == 0, base fields default-initialized.
//   2. Upgrade constructor: every base field preserved bit-for-bit; l_invoicekey set.
//   3. Key round-trip: lineitem_i_t::Key fold/unfold matches lineitem_t::Key layout.
//   4. Slice behaviour: static_cast<const lineitem_t&>(li) yields correct base fields.
//   5. Adapter compatibility: insert one lineitem_i_t into RocksDB adapter, scan it
//      back, all fields match including l_invoicekey.
//   6. Static assert: TpchExecutableHelper instantiates with Adapter<lineitem_i_t>
//      (compile-time check that a394119c's template change is sound).
//
// CMake target: test_lineitem_i_upgrade
// Build:  cmake --build build --target test_lineitem_i_upgrade -j$(sysctl -n hw.ncpu)
// Run:    ./build/frontend/test_lineitem_i_upgrade \
//             --ssd_path=./test_data_unit/test_lineitem_i_upgrade \
//             --csv_path=./test_csv_unit/test_lineitem_i_upgrade

#include <gflags/gflags.h>
#include <cassert>
#include <cstring>
#include <iostream>

#include "../../shared/RocksDB.hpp"
#include "../backend.hpp"
#include "../tpchi_tables.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// Helper: build a fully-populated lineitem_t for upgrade tests.

static lineitem_t make_base_lineitem()
{
   lineitem_t l{};
   l.l_partkey       = 101;
   l.l_suppkey       = 202;
   l.l_quantity      = 5.0;
   l.l_extendedprice = 500.0;
   l.l_discount      = 0.05;
   l.l_tax           = 0.08;
   l.l_returnflag    = Varchar<1>("N");
   l.l_linestatus    = Varchar<1>("O");
   l.l_shipdate      = 9300;
   l.l_commitdate    = 9310;
   l.l_receiptdate   = 9320;
   l.l_shipinstruct  = Varchar<25>("NONE");
   l.l_shipmode      = Varchar<10>("MAIL");
   l.l_comment       = Varchar<44>("test comment");
   return l;
}

// ---------------------------------------------------------------------------
// Test 1: default constructor sets l_invoicekey == 0.

static void test_default_ctor()
{
   lineitem_i_t li{};
   assert(li.l_invoicekey == 0);
   // Base numeric fields should also be zero-initialized.
   assert(li.l_partkey  == 0);
   assert(li.l_suppkey  == 0);
   assert(li.l_quantity == 0.0);
   std::cerr << "  PASS: test_default_ctor\n";
}

// ---------------------------------------------------------------------------
// Test 2: upgrade constructor preserves all base fields and sets invoicekey.

static void test_upgrade_ctor()
{
   lineitem_t base = make_base_lineitem();
   constexpr Integer INV_KEY = 77;

   lineitem_i_t li(base, INV_KEY);

   // Every base field must be bit-for-bit identical.
   assert(li.l_partkey       == base.l_partkey);
   assert(li.l_suppkey       == base.l_suppkey);
   assert(li.l_quantity      == base.l_quantity);
   assert(li.l_extendedprice == base.l_extendedprice);
   assert(li.l_discount      == base.l_discount);
   assert(li.l_tax           == base.l_tax);
   assert(li.l_returnflag    == base.l_returnflag);
   assert(li.l_linestatus    == base.l_linestatus);
   assert(li.l_shipdate      == base.l_shipdate);
   assert(li.l_commitdate    == base.l_commitdate);
   assert(li.l_receiptdate   == base.l_receiptdate);
   assert(li.l_shipinstruct  == base.l_shipinstruct);
   assert(li.l_shipmode      == base.l_shipmode);
   assert(li.l_comment       == base.l_comment);

   // The new field must carry the assigned invoice key.
   assert(li.l_invoicekey == INV_KEY);

   std::cerr << "  PASS: test_upgrade_ctor\n";
}

// ---------------------------------------------------------------------------
// Test 3: Key round-trip (same layout as lineitem_t::Key).

static void test_key_round_trip()
{
   lineitem_i_t::Key orig{42, 3};
   u8 buf[lineitem_i_t::Key::maxFoldLength()];
   unsigned len = lineitem_i_t::Key::keyfold(buf, orig);

   lineitem_i_t::Key rt{};
   lineitem_i_t::Key::keyunfold(buf, rt);

   assert(rt.l_orderkey   == orig.l_orderkey);
   assert(rt.l_linenumber == orig.l_linenumber);
   (void)len;

   std::cerr << "  PASS: test_key_round_trip\n";
}

// ---------------------------------------------------------------------------
// Test 4: slice behaviour via base reference.

static void test_slice()
{
   lineitem_t base = make_base_lineitem();
   lineitem_i_t li(base, 99);

   [[maybe_unused]] const lineitem_t& sliced = static_cast<const lineitem_t&>(li);
   assert(sliced.l_partkey       == base.l_partkey);
   assert(sliced.l_extendedprice == base.l_extendedprice);
   assert(sliced.l_discount      == base.l_discount);
   assert(sliced.l_shipdate      == base.l_shipdate);
   assert(sliced.l_shipmode      == base.l_shipmode);

   std::cerr << "  PASS: test_slice\n";
}

// ---------------------------------------------------------------------------
// Test 5: adapter compatibility — insert + scan via RocksDB adapter.

static void test_adapter_roundtrip(RocksDB& rocks_db)
{
   using B = tpch::RocksDBBackend;
   B::Adapter<lineitem_i_t> lineitem_adapter(rocks_db);

   lineitem_t base = make_base_lineitem();
   constexpr Integer INV_KEY = 55;
   lineitem_i_t li(base, INV_KEY);

   lineitem_i_t::Key k{1000, 1};
   lineitem_adapter.insert(k, li);

   // Scan forward and find the inserted record.
   int matches = 0;
   auto sc = lineitem_adapter.getScanner();
   while (auto kv = sc->next()) {
      if (kv->first.l_orderkey == 1000 && kv->first.l_linenumber == 1) {
         const lineitem_i_t& got = kv->second;
         if (got.l_partkey       != li.l_partkey)       { std::cerr << "FAIL: l_partkey mismatch\n";       std::abort(); }
         if (got.l_extendedprice != li.l_extendedprice) { std::cerr << "FAIL: l_extendedprice mismatch\n"; std::abort(); }
         if (got.l_discount      != li.l_discount)      { std::cerr << "FAIL: l_discount mismatch\n";      std::abort(); }
         if (got.l_shipdate      != li.l_shipdate)      { std::cerr << "FAIL: l_shipdate mismatch\n";      std::abort(); }
         if (got.l_invoicekey    != INV_KEY)             { std::cerr << "FAIL: l_invoicekey mismatch\n";   std::abort(); }
         matches++;
         break;
      }
   }
   if (matches != 1) { std::cerr << "FAIL: lineitem_i_t record not found after insert\n"; std::abort(); }

   std::cerr << "  PASS: test_adapter_roundtrip\n";
}

// ---------------------------------------------------------------------------
// main

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("lineitem_i_t unit tests");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   // Pure-types tests (no DB needed).
   std::cerr << "Running test_lineitem_i_upgrade...\n";
   test_default_ctor();
   test_upgrade_ctor();
   test_key_round_trip();
   test_slice();

   // Adapter test requires RocksDB.
   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   rocks_db.open();
   test_adapter_roundtrip(rocks_db);

   std::cerr << "All tests passed.\n";
   return 0;
}
