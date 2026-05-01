// Unit tests for views_coli.hpp: tagged record types, SKMatcher specializations,
// and the byte-driven coli_unfold_key_to_variant dispatcher.
//
// CMake target: test_views_coli
// Build:  cd build && make -C frontend test_views_coli -j$(sysctl -n hw.ncpu)
// Run:    ./frontend/test_views_coli

#include "../views_coli.hpp"
#include "../../../frontend/shared/variant_utils.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

using namespace tpch;

// ---------------------------------------------------------------------------
// Helpers

// Sign-normalize to -1 / 0 / +1.
[[maybe_unused]] static int sign(int x) { return (x > 0) - (x < 0); }

// ---------------------------------------------------------------------------
// Test 1: Tag-byte format pin
//
// For each record type, fold a sample key with known field values and assert
// each byte at each expected offset.
//
// Layout reference (Integer folds to 4 B big-endian XOR-flipped; u8 = 1 B):
//
//   customer : [t=0:1][custkey:4][t=4:1][idx=0:1]              = 7 B  offsets 0..6
//   orders   : [t=0:1][custkey:4][t=1:1][orderkey:4][t=4:1][idx=1:1] = 12 B  offsets 0..11
//   lineitem : [t=0:1][custkey:4][t=1:1][orderkey:4][t=2:1][invoicekey:4][linenumber:4][t=4:1][idx=2:1] = 21 B  offsets 0..20
//   invoice  : [t=0:1][custkey:4][t=3:1][invoicekey:4][t=4:1][idx=3:1]  = 12 B  offsets 0..11

// Fold a big-endian XOR-flipped integer to a 4-byte array (matches fold(u8*, Integer)).
static void fold_int(u8* out, Integer x)
{
   u32 v = __builtin_bswap32(static_cast<u32>(x) ^ (1u << 31));
   std::memcpy(out, &v, 4);
}

static void test_tag_byte_format_pin()
{
   constexpr Integer CUST = 42, ORDER = 100, INVOICE = 7, LINE = 3;

   // ---- customer_coli_t ----
   {
      customer_coli_t::Key k{CUST};
      u8 buf[customer_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = customer_coli_t::foldKey(buf, k);
      assert(len == 7);

      u8 expected[7];
      expected[0] = static_cast<u8>(coli_domain_tag::customer);  // t=0
      fold_int(expected + 1, CUST);                               // custkey:4
      expected[5] = static_cast<u8>(coli_domain_tag::index);      // sentinel t=4
      expected[6] = static_cast<u8>(coli_idx_id::customer);       // idx=0
      assert(std::memcmp(buf, expected, 7) == 0);
   }

   // ---- orders_coli_t ----
   {
      orders_coli_t::Key k{CUST, ORDER};
      u8 buf[orders_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = orders_coli_t::foldKey(buf, k);
      assert(len == 12);

      u8 expected[12];
      expected[0] = static_cast<u8>(coli_domain_tag::customer);
      fold_int(expected + 1, CUST);
      expected[5] = static_cast<u8>(coli_domain_tag::orders);
      fold_int(expected + 6, ORDER);
      expected[10] = static_cast<u8>(coli_domain_tag::index);
      expected[11] = static_cast<u8>(coli_idx_id::orders);
      assert(std::memcmp(buf, expected, 12) == 0);
   }

   // ---- lineitem_coli_t ----
   {
      lineitem_coli_t::Key k{CUST, ORDER, INVOICE, LINE};
      u8 buf[lineitem_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = lineitem_coli_t::foldKey(buf, k);
      assert(len == 21);

      u8 expected[21];
      expected[0] = static_cast<u8>(coli_domain_tag::customer);
      fold_int(expected + 1, CUST);
      expected[5] = static_cast<u8>(coli_domain_tag::orders);
      fold_int(expected + 6, ORDER);
      expected[10] = static_cast<u8>(coli_domain_tag::lineitem);
      fold_int(expected + 11, INVOICE);
      fold_int(expected + 15, LINE);
      expected[19] = static_cast<u8>(coli_domain_tag::index);
      expected[20] = static_cast<u8>(coli_idx_id::lineitem);
      assert(std::memcmp(buf, expected, 21) == 0);
   }

   // ---- invoice_coli_t ----
   {
      invoice_coli_t::Key k{CUST, INVOICE};
      u8 buf[invoice_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = invoice_coli_t::foldKey(buf, k);
      assert(len == 12);

      u8 expected[12];
      expected[0] = static_cast<u8>(coli_domain_tag::customer);
      fold_int(expected + 1, CUST);
      expected[5] = static_cast<u8>(coli_domain_tag::invoice);
      fold_int(expected + 6, INVOICE);
      expected[10] = static_cast<u8>(coli_domain_tag::index);
      expected[11] = static_cast<u8>(coli_idx_id::invoice);
      assert(std::memcmp(buf, expected, 12) == 0);
   }

   std::cerr << "  PASS: test_tag_byte_format_pin\n";
}

// ---------------------------------------------------------------------------
// Test 2: Round-trip parity
//
// Fold a lineitem_coli_t::Key, unfold it via unfoldKey<Key> (round-trip path)
// AND via coli_unfold_key_to_variant (byte-driven dispatch path), assert
// identical results.

static void test_round_trip_parity()
{
   lineitem_coli_t::Key orig{42, 100, 7, 3};
   u8 buf[lineitem_coli_t::maxFoldLength()];
   [[maybe_unused]] unsigned len = lineitem_coli_t::foldKey(buf, orig);

   // Round-trip via typed unfoldKey
   lineitem_coli_t::Key rt;
   lineitem_coli_t::unfoldKey(buf, rt);
   assert(rt.custkey    == orig.custkey);
   assert(rt.orderkey   == orig.orderkey);
   assert(rt.invoicekey == orig.invoicekey);
   assert(rt.linenumber == orig.linenumber);

   // Round-trip via byte-driven dispatch
   coli_key_variant v = coli_unfold_key_to_variant(buf, len);
   assert(std::holds_alternative<lineitem_coli_t::Key>(v));
   [[maybe_unused]] const auto& vk = std::get<lineitem_coli_t::Key>(v);
   assert(vk.custkey    == orig.custkey);
   assert(vk.orderkey   == orig.orderkey);
   assert(vk.invoicekey == orig.invoicekey);
   assert(vk.linenumber == orig.linenumber);

   std::cerr << "  PASS: test_round_trip_parity\n";
}

// ---------------------------------------------------------------------------
// Test 3: Discrimination matrix
//
// Construct one key of each record type, fold it, push through
// record_matches<R> (the same hook toType() uses) and assert:
//   - exactly the correct record type matches
//   - all others do not match
// Also verify corruption detection: flip the trailing idx_id byte and assert
// no record matches.

static void test_discrimination_matrix()
{
   constexpr Integer CUST = 1, ORDER = 2, INV = 3, LINE = 4;

   // customer
   {
      customer_coli_t::Key k{CUST};
      u8 buf[customer_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = customer_coli_t::foldKey(buf, k);

      assert( record_matches<customer_coli_t>(buf, len, sizeof(customer_coli_t)));
      assert(!record_matches<orders_coli_t>  (buf, len, sizeof(customer_coli_t)));
      assert(!record_matches<lineitem_coli_t>(buf, len, sizeof(customer_coli_t)));
      assert(!record_matches<invoice_coli_t> (buf, len, sizeof(customer_coli_t)));

      // corrupt trailing byte → no match
      buf[len - 1] = 0xFF;
      assert(!record_matches<customer_coli_t>(buf, len, sizeof(customer_coli_t)));
   }

   // orders
   {
      orders_coli_t::Key k{CUST, ORDER};
      u8 buf[orders_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = orders_coli_t::foldKey(buf, k);

      assert(!record_matches<customer_coli_t>(buf, len, sizeof(orders_coli_t)));
      assert( record_matches<orders_coli_t>  (buf, len, sizeof(orders_coli_t)));
      assert(!record_matches<lineitem_coli_t>(buf, len, sizeof(orders_coli_t)));
      assert(!record_matches<invoice_coli_t> (buf, len, sizeof(orders_coli_t)));

      buf[len - 1] = 0xFF;
      assert(!record_matches<orders_coli_t>(buf, len, sizeof(orders_coli_t)));
   }

   // lineitem
   {
      lineitem_coli_t::Key k{CUST, ORDER, INV, LINE};
      u8 buf[lineitem_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = lineitem_coli_t::foldKey(buf, k);

      assert(!record_matches<customer_coli_t>(buf, len, sizeof(lineitem_coli_t)));
      assert(!record_matches<orders_coli_t>  (buf, len, sizeof(lineitem_coli_t)));
      assert( record_matches<lineitem_coli_t>(buf, len, sizeof(lineitem_coli_t)));
      assert(!record_matches<invoice_coli_t> (buf, len, sizeof(lineitem_coli_t)));

      buf[len - 1] = 0xFF;
      assert(!record_matches<lineitem_coli_t>(buf, len, sizeof(lineitem_coli_t)));
   }

   // invoice
   {
      invoice_coli_t::Key k{CUST, INV};
      u8 buf[invoice_coli_t::maxFoldLength()];
      [[maybe_unused]] unsigned len = invoice_coli_t::foldKey(buf, k);

      assert(!record_matches<customer_coli_t>(buf, len, sizeof(invoice_coli_t)));
      assert(!record_matches<orders_coli_t>  (buf, len, sizeof(invoice_coli_t)));
      assert(!record_matches<lineitem_coli_t>(buf, len, sizeof(invoice_coli_t)));
      assert( record_matches<invoice_coli_t> (buf, len, sizeof(invoice_coli_t)));

      buf[len - 1] = 0xFF;
      assert(!record_matches<invoice_coli_t>(buf, len, sizeof(invoice_coli_t)));
   }

   std::cerr << "  PASS: test_discrimination_matrix\n";
}

// ---------------------------------------------------------------------------
// Test 4: Mixed-type scan order
//
// Build one key of each type for custkey=42, fold them, sort byte-lex, and
// assert the sequence: customer → orders → lineitem → invoice.
//
// The orders key (12 B) is a prefix of the lineitem key (21 B) through
// orderkey, so with orderkey=100 the orders key sorts *before* any lineitem
// under that orderkey (same prefix, shorter key loses in lexicographic order
// only if the extra bytes push lineitem past orders — but here the next byte
// after the shared prefix is the lineitem tag t=2 > t=4 sentinel in orders,
// so lineitem sorts *after* orders as expected).

static void test_mixed_type_scan_order()
{
   constexpr Integer CUST = 42, ORDER = 100, INV_KEY = 7, LINE = 3;

   // Fold all four keys into heap-allocated vectors for easy sorting.
   auto fold_to_vec = [](const u8* buf, unsigned len) {
      return std::vector<u8>(buf, buf + len);
   };

   static constexpr unsigned kCLen = customer_coli_t::maxFoldLength();
   static constexpr unsigned kOLen = orders_coli_t::maxFoldLength();
   static constexpr unsigned kLLen = lineitem_coli_t::maxFoldLength();
   static constexpr unsigned kILen = invoice_coli_t::maxFoldLength();
   u8 cbuf[kCLen];
   u8 obuf[kOLen];
   u8 lbuf[kLLen];
   u8 ibuf[kILen];

   unsigned clen = customer_coli_t::foldKey(cbuf, customer_coli_t::Key{CUST});
   unsigned olen = orders_coli_t::foldKey(obuf, orders_coli_t::Key{CUST, ORDER});
   unsigned llen = lineitem_coli_t::foldKey(lbuf, lineitem_coli_t::Key{CUST, ORDER, INV_KEY, LINE});
   unsigned ilen = invoice_coli_t::foldKey(ibuf, invoice_coli_t::Key{CUST, INV_KEY});

   // Tag each vector with its expected position.
   struct Entry {
      std::vector<u8> key;
      int expected_rank;
   };

   std::vector<Entry> entries = {
       {fold_to_vec(cbuf, clen), 0},  // customer
       {fold_to_vec(obuf, olen), 1},  // orders
       {fold_to_vec(lbuf, llen), 2},  // lineitem (after orders for same orderkey)
       {fold_to_vec(ibuf, ilen), 3},  // invoice
   };

   std::sort(entries.begin(), entries.end(), [](const Entry& a, const Entry& b) {
      size_t min_len = std::min(a.key.size(), b.key.size());
      int cmp = std::memcmp(a.key.data(), b.key.data(), min_len);
      if (cmp != 0) return cmp < 0;
      return a.key.size() < b.key.size();
   });

   for (int i = 0; i < 4; ++i) {
      assert(entries[i].expected_rank == i);
   }

   std::cerr << "  PASS: test_mixed_type_scan_order\n";
}

// ---------------------------------------------------------------------------
// Test 5: SKMatcher unit tests
//
// For every defined COLI pair, exercise three cases:
//   a) same group (0 expected)
//   b) different group via first differing field (negative expected for R1 < R2)
//   c) different group via second differing field (where applicable)
// Verify sign semantics.

// Matcher call helpers — invoke SKMatcher::match and normalize to sign.
// Using helper functions avoids unused-typedef warnings from `using M = ...`
// and unused-variable warnings from value-args that disappear under assert.
template <typename R1, typename R2>
static int sk_match(const typename R1::Key& k1, const R1& v1,
                    const typename R2::Key& k2, const R2& v2)
{
   return sign(SKMatcher<R1, R2>::match(k1, v1, k2, v2));
}

static void test_skmatcher_self_pairs()
{
   // Default-constructed value args — only their type is needed by SKMatcher.
   [[maybe_unused]] customer_coli_t c{};
   [[maybe_unused]] orders_coli_t   o{};
   [[maybe_unused]] lineitem_coli_t l{};
   [[maybe_unused]] invoice_coli_t  i{};

   // Extra parens around sk_match<...>(...) calls guard against the assert()
   // macro treating commas inside brace-initializers as argument separators.

   // customer × customer — match on custkey
   assert((sk_match<customer_coli_t, customer_coli_t>({10}, c, {10}, c) == 0));
   assert((sk_match<customer_coli_t, customer_coli_t>({10}, c, {20}, c) == -1));
   assert((sk_match<customer_coli_t, customer_coli_t>({20}, c, {10}, c) == 1));

   // orders × orders — match on (custkey, orderkey)
   assert((sk_match<orders_coli_t, orders_coli_t>({10, 5}, o, {10, 5}, o) == 0));
   assert((sk_match<orders_coli_t, orders_coli_t>({10, 5}, o, {10, 9}, o) == -1));
   assert((sk_match<orders_coli_t, orders_coli_t>({10, 5}, o, {20, 5}, o) == -1));
   assert((sk_match<orders_coli_t, orders_coli_t>({20, 5}, o, {10, 5}, o) == 1));

   // lineitem × lineitem — full key
   assert((sk_match<lineitem_coli_t, lineitem_coli_t>({1,2,3,4}, l, {1,2,3,4}, l) == 0));
   assert((sk_match<lineitem_coli_t, lineitem_coli_t>({1,2,3,4}, l, {1,2,3,5}, l) == -1));
   assert((sk_match<lineitem_coli_t, lineitem_coli_t>({1,2,3,4}, l, {1,2,4,4}, l) == -1));
   assert((sk_match<lineitem_coli_t, lineitem_coli_t>({1,2,3,4}, l, {1,3,3,4}, l) == -1));
   assert((sk_match<lineitem_coli_t, lineitem_coli_t>({1,2,3,4}, l, {2,2,3,4}, l) == -1));

   // invoice × invoice — (custkey, invoicekey)
   assert((sk_match<invoice_coli_t, invoice_coli_t>({5,10}, i, {5,10}, i) == 0));
   assert((sk_match<invoice_coli_t, invoice_coli_t>({5,10}, i, {5,20}, i) == -1));
   assert((sk_match<invoice_coli_t, invoice_coli_t>({5,10}, i, {6,10}, i) == -1));
   assert((sk_match<invoice_coli_t, invoice_coli_t>({6,10}, i, {5,10}, i) == 1));

   std::cerr << "  PASS: test_skmatcher_self_pairs\n";
}

static void test_skmatcher_cross_pairs()
{
   [[maybe_unused]] customer_coli_t c{};
   [[maybe_unused]] orders_coli_t   o{};
   [[maybe_unused]] lineitem_coli_t l{};
   [[maybe_unused]] invoice_coli_t  i{};

   // Extra parens guard against assert() macro treating commas as arg separators.

   // customer × orders — custkey only; verify symmetry via reversed spec
   assert((sk_match<customer_coli_t, orders_coli_t>({10}, c, {10, 5}, o) == 0));
   assert((sk_match<customer_coli_t, orders_coli_t>({10}, c, {20, 5}, o) == -1));
   assert((sk_match<customer_coli_t, orders_coli_t>({20}, c, {10, 5}, o) == 1));
   assert((sk_match<orders_coli_t, customer_coli_t>({10, 5}, o, {10}, c) == 0));
   assert((sk_match<orders_coli_t, customer_coli_t>({10, 5}, o, {20}, c) == 1));  // symmetry

   // customer × lineitem — custkey only
   assert((sk_match<customer_coli_t, lineitem_coli_t>({5}, c, {5,1,2,3}, l) == 0));
   assert((sk_match<customer_coli_t, lineitem_coli_t>({5}, c, {6,1,2,3}, l) == -1));
   assert((sk_match<lineitem_coli_t, customer_coli_t>({5,1,2,3}, l, {5}, c) == 0));
   assert((sk_match<lineitem_coli_t, customer_coli_t>({5,1,2,3}, l, {6}, c) == 1));

   // customer × invoice — custkey only
   assert((sk_match<customer_coli_t, invoice_coli_t>({3}, c, {3,7}, i) == 0));
   assert((sk_match<customer_coli_t, invoice_coli_t>({3}, c, {4,7}, i) == -1));
   assert((sk_match<invoice_coli_t, customer_coli_t>({3,7}, i, {3}, c) == 0));
   assert((sk_match<invoice_coli_t, customer_coli_t>({3,7}, i, {4}, c) == 1));

   // orders × lineitem — (custkey, orderkey)
   assert((sk_match<orders_coli_t, lineitem_coli_t>({5,10}, o, {5,10,2,1}, l) == 0));
   assert((sk_match<orders_coli_t, lineitem_coli_t>({5,10}, o, {5,11,2,1}, l) == -1));  // orderkey diff
   assert((sk_match<orders_coli_t, lineitem_coli_t>({5,10}, o, {6,10,2,1}, l) == -1));  // custkey diff
   assert((sk_match<lineitem_coli_t, orders_coli_t>({5,10,2,1}, l, {5,10}, o) == 0));
   assert((sk_match<lineitem_coli_t, orders_coli_t>({5,10,2,1}, l, {5,11}, o) == 1));

   // orders × invoice — custkey only (siblings)
   assert((sk_match<orders_coli_t, invoice_coli_t>({7,3}, o, {7,9}, i) == 0));
   assert((sk_match<orders_coli_t, invoice_coli_t>({7,3}, o, {8,9}, i) == -1));
   assert((sk_match<invoice_coli_t, orders_coli_t>({7,9}, i, {7,3}, o) == 0));
   assert((sk_match<invoice_coli_t, orders_coli_t>({8,9}, i, {7,3}, o) == 1));

   // lineitem × invoice — (custkey, invoicekey)
   assert((sk_match<lineitem_coli_t, invoice_coli_t>({2,5,9,1}, l, {2,9},  i) == 0));
   assert((sk_match<lineitem_coli_t, invoice_coli_t>({2,5,9,1}, l, {2,10}, i) == -1));  // invoicekey diff
   assert((sk_match<lineitem_coli_t, invoice_coli_t>({2,5,9,1}, l, {3,9},  i) == -1));  // custkey diff
   assert((sk_match<invoice_coli_t, lineitem_coli_t>({2,9},  i, {2,5,9,1}, l) == 0));
   assert((sk_match<invoice_coli_t, lineitem_coli_t>({2,10}, i, {2,5,9,1}, l) == 1));

   std::cerr << "  PASS: test_skmatcher_cross_pairs\n";
}

// ---------------------------------------------------------------------------
// main

int main()
{
   std::cerr << "Running test_views_coli...\n";
   test_tag_byte_format_pin();
   test_round_trip_parity();
   test_discrimination_matrix();
   test_mixed_type_scan_order();
   test_skmatcher_self_pairs();
   test_skmatcher_cross_pairs();
   std::cerr << "All tests passed.\n";
   return 0;
}
