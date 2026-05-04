// Unit tests for aCOLI tagged-key encoding (views_coli.hpp).
//
// Verifies the three-type aCOLI family: customer_acoli_t (idx=49),
// orders_coli_t reused (idx=1), and lineitem_acoli_t (idx=53).
// All pure-types — no DB required.
//
// CMake target: test_views_acoli
// Build:  cmake --build build --target test_views_acoli -j$(sysctl -n hw.ncpu)
// Run:    ./build/frontend/test_views_acoli

#include "../views_coli.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <variant>

using namespace tpch;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Test 1: customer_acoli_t key round-trip + trailing idx byte
//
// Key layout: [t=1(cust)][custkey:4][t=0(idx)][idx=49]  =  7 bytes

static void test_customer_acoli_round_trip()
{
   constexpr Integer CUST = 42;

   customer_acoli_t::Key orig{CUST};
   u8 buf[customer_acoli_t::maxFoldLength()];
   [[maybe_unused]] unsigned len = customer_acoli_t::foldKey(buf, orig);

   // Length check: same layout as customer_coli_t (7 bytes), distinguished
   // only by the trailing idx byte (49 vs 0).
   assert(len == 7);

   // Trailing byte must equal idx=customer_acoli (49).
   assert(buf[len - 1] == static_cast<u8>(coli_idx_id::customer_acoli));

   // Round-trip via typed unfoldKey.
   customer_acoli_t::Key rt{};
   customer_acoli_t::unfoldKey(buf, rt);
   assert(rt.custkey == orig.custkey);

   // accepts_key must be true for this key.
   assert(customer_acoli_t::Key::accepts_key(buf, len));

   // accepts_key must reject a foreign key (e.g. customer_coli_t's idx byte).
   u8 foreign[customer_acoli_t::maxFoldLength()];
   std::memcpy(foreign, buf, len);
   foreign[len - 1] = static_cast<u8>(coli_idx_id::customer);  // idx=0
   assert(!customer_acoli_t::Key::accepts_key(foreign, len));

   std::cerr << "  PASS: test_customer_acoli_round_trip\n";
}

// ---------------------------------------------------------------------------
// Test 2: lineitem_acoli_t key round-trip + trailing idx byte
//
// Key layout: [t=1][custkey:4][t=3][orderkey:4][t=4][linenumber:4][t=0][idx=53]  = 17 bytes
// Note: no invoicekey segment — this is what distinguishes lineitem_acoli_t
// (17 bytes, idx=53) from lineitem_coli_t (21 bytes, idx=2).

static void test_lineitem_acoli_round_trip()
{
   constexpr Integer CUST = 42, ORDER = 100, LINE = 1;

   lineitem_acoli_t::Key orig{CUST, ORDER, LINE};
   u8 buf[lineitem_acoli_t::maxFoldLength()];
   [[maybe_unused]] unsigned len = lineitem_acoli_t::foldKey(buf, orig);

   assert(len == 17);
   assert(buf[len - 1] == static_cast<u8>(coli_idx_id::lineitem_acoli));  // idx=53

   lineitem_acoli_t::Key rt{};
   lineitem_acoli_t::unfoldKey(buf, rt);
   assert(rt.custkey    == orig.custkey);
   assert(rt.orderkey   == orig.orderkey);
   assert(rt.linenumber == orig.linenumber);

   assert(lineitem_acoli_t::Key::accepts_key(buf, len));

   // Must not accept a lineitem_coli_t key (different length AND idx byte).
   lineitem_coli_t::Key coli_key{CUST, ORDER, 7 /*invoicekey*/, LINE};
   u8 coli_buf[lineitem_coli_t::maxFoldLength()];
   [[maybe_unused]] unsigned coli_len = lineitem_coli_t::foldKey(coli_buf, coli_key);
   assert(coli_len == 21);
   assert(!lineitem_acoli_t::Key::accepts_key(coli_buf, coli_len));

   std::cerr << "  PASS: test_lineitem_acoli_round_trip\n";
}

// ---------------------------------------------------------------------------
// Test 3: orders_coli_t round-trips in the aCOLI context (idx=1)
//
// orders_acoli_t was retired (Step 4b) and collapsed into orders_coli_t.
// Verify that orders_coli_t::Key::accepts_key still works correctly and that
// the trailing byte equals 1.

static void test_orders_coli_in_acoli_context()
{
   constexpr Integer CUST = 42, ORDER = 100;

   orders_coli_t::Key orig{CUST, ORDER};
   u8 buf[orders_coli_t::maxFoldLength()];
   [[maybe_unused]] unsigned len = orders_coli_t::foldKey(buf, orig);

   assert(len == 12);
   assert(buf[len - 1] == static_cast<u8>(coli_idx_id::orders));  // idx=1

   orders_coli_t::Key rt{};
   orders_coli_t::unfoldKey(buf, rt);
   assert(rt.custkey  == orig.custkey);
   assert(rt.orderkey == orig.orderkey);

   assert(orders_coli_t::Key::accepts_key(buf, len));

   // The same key must NOT be accepted by customer_acoli_t (different length
   // and trailing byte) or lineitem_acoli_t (different trailing byte and length).
   assert(!customer_acoli_t::Key::accepts_key(buf, len));
   assert(!lineitem_acoli_t::Key::accepts_key(buf, len));

   std::cerr << "  PASS: test_orders_coli_in_acoli_context\n";
}

// ---------------------------------------------------------------------------
// Test 4: discrimination — each type accepts only its own key
//
// The three aCOLI-family trailing bytes (49, 1, 53) are all distinct.
// Verify the full cross-discrimination matrix for the three types.

static void test_acoli_discrimination_matrix()
{
   constexpr Integer CUST = 5, ORDER = 10, LINE = 2;

   u8 ca_buf[customer_acoli_t::maxFoldLength()];
   u8 oc_buf[orders_coli_t::maxFoldLength()];
   u8 la_buf[lineitem_acoli_t::maxFoldLength()];

   [[maybe_unused]] unsigned ca_len = customer_acoli_t::foldKey(ca_buf, customer_acoli_t::Key{CUST});
   [[maybe_unused]] unsigned oc_len = orders_coli_t::foldKey(oc_buf,   orders_coli_t::Key{CUST, ORDER});
   [[maybe_unused]] unsigned la_len = lineitem_acoli_t::foldKey(la_buf, lineitem_acoli_t::Key{CUST, ORDER, LINE});

   // customer_acoli_t accepts only its own key.
   assert( customer_acoli_t::Key::accepts_key(ca_buf, ca_len));
   assert(!customer_acoli_t::Key::accepts_key(oc_buf, oc_len));
   assert(!customer_acoli_t::Key::accepts_key(la_buf, la_len));

   // orders_coli_t (in aCOLI role) accepts only its own key.
   assert(!orders_coli_t::Key::accepts_key(ca_buf, ca_len));
   assert( orders_coli_t::Key::accepts_key(oc_buf, oc_len));
   assert(!orders_coli_t::Key::accepts_key(la_buf, la_len));

   // lineitem_acoli_t accepts only its own key.
   assert(!lineitem_acoli_t::Key::accepts_key(ca_buf, ca_len));
   assert(!lineitem_acoli_t::Key::accepts_key(oc_buf, oc_len));
   assert( lineitem_acoli_t::Key::accepts_key(la_buf, la_len));

   std::cerr << "  PASS: test_acoli_discrimination_matrix\n";
}

// ---------------------------------------------------------------------------
// Test 5: negative — lineitem_coli_t-shaped key rejected by all aCOLI types
//
// A key with invoicekey segment (lineitem_coli_t, 21 bytes, idx=2) must not
// be accepted by any of the three aCOLI types.

static void test_acoli_rejects_coli_lineitem_key()
{
   lineitem_coli_t::Key lk{1, 2, 3, 4};
   u8 buf[lineitem_coli_t::maxFoldLength()];
   [[maybe_unused]] unsigned len = lineitem_coli_t::foldKey(buf, lk);

   assert(!customer_acoli_t::Key::accepts_key(buf, len));
   assert(!orders_coli_t::Key::accepts_key(buf, len));
   assert(!lineitem_acoli_t::Key::accepts_key(buf, len));

   std::cerr << "  PASS: test_acoli_rejects_coli_lineitem_key\n";
}

// ---------------------------------------------------------------------------
// Test 6: byte-lex scan order for custkey group
//
// For a fixed custkey, folded keys must sort in the order:
//   customer_acoli_t (7 B) < orders_coli_t (12 B) < lineitem_acoli_t (17 B)
//
// The sentinel byte (0) appended by tagged_path sorts strictly before any
// domain tag (≥1), which is what causes parent rows to precede children.

static void test_acoli_scan_order()
{
   constexpr Integer CUST = 42, ORDER = 100, LINE = 1;

   u8 ca_buf[customer_acoli_t::maxFoldLength()];
   u8 oc_buf[orders_coli_t::maxFoldLength()];
   u8 la_buf[lineitem_acoli_t::maxFoldLength()];

   [[maybe_unused]] unsigned ca_len = customer_acoli_t::foldKey(ca_buf, customer_acoli_t::Key{CUST});
   [[maybe_unused]] unsigned oc_len = orders_coli_t::foldKey(oc_buf,   orders_coli_t::Key{CUST, ORDER});
   [[maybe_unused]] unsigned la_len = lineitem_acoli_t::foldKey(la_buf, lineitem_acoli_t::Key{CUST, ORDER, LINE});

   // Lexicographic comparison helper.
   [[maybe_unused]] auto lex_lt = [](const u8* a, unsigned alen, const u8* b, unsigned blen) -> bool {
      size_t min_len = std::min(alen, blen);
      int cmp = std::memcmp(a, b, min_len);
      if (cmp != 0) return cmp < 0;
      return alen < blen;
   };

   // customer < orders < lineitem in byte-lex order.
   assert(lex_lt(ca_buf, ca_len, oc_buf, oc_len));
   assert(lex_lt(oc_buf, oc_len, la_buf, la_len));
   assert(lex_lt(ca_buf, ca_len, la_buf, la_len));

   std::cerr << "  PASS: test_acoli_scan_order\n";
}

// ---------------------------------------------------------------------------
// main

int main()
{
   std::cerr << "Running test_views_acoli...\n";
   test_customer_acoli_round_trip();
   test_lineitem_acoli_round_trip();
   test_orders_coli_in_acoli_context();
   test_acoli_discrimination_matrix();
   test_acoli_rejects_coli_lineitem_key();
   test_acoli_scan_order();
   std::cerr << "All tests passed.\n";
   return 0;
}
