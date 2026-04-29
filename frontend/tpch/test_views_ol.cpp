// Unit tests for views_ol.hpp: ol_sort_key_t, joined_ol_t, and SKBuilder.
//
// CMake target: test_views_ol (macOS/RocksDB-only build).
// Build: cd build2/frontend && make test_views_ol
// Run:   ./test_views_ol

#include "views_ol.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>

using namespace tpch;

// ---------------------------------------------------------------------------
// test_sort_key_ordering: spaceship-derived < respects (orderkey, linenumber)

static void test_sort_key_ordering()
{
   [[maybe_unused]] ol_sort_key_t a{1, 0}, b{1, 1}, c{1, 2}, d{2, 0};
   assert(a < b);  // order-level key before its lineitems
   assert(b < c);
   assert(c < d);  // different orderkey
   assert(a < d);
   std::cerr << "  PASS: test_sort_key_ordering\n";
}

// ---------------------------------------------------------------------------
// test_sort_key_max: max() must be >= every ordinary key

static void test_sort_key_max()
{
   [[maybe_unused]] auto m = ol_sort_key_t::max();
   [[maybe_unused]] ol_sort_key_t any{999999, 999999};
   assert(any < m || any == m);
   std::cerr << "  PASS: test_sort_key_max\n";
}

// ---------------------------------------------------------------------------
// test_match_semantics: linenumber==0 is a prefix wildcard

static void test_match_semantics()
{
   // Prefix match: linenumber==0 is wildcard.
   // Extra parens work around the macOS assert() macro treating the comma in
   // brace-initializers as argument separators.
   assert((ol_sort_key_t{5, 0}.match(ol_sort_key_t{5, 3}) == 0));
   assert((ol_sort_key_t{5, 3}.match(ol_sort_key_t{5, 0}) == 0));
   assert((ol_sort_key_t{5, 0}.match(ol_sort_key_t{5, 0}) == 0));

   // Exact match
   assert((ol_sort_key_t{5, 3}.match(ol_sort_key_t{5, 3}) == 0));

   // Different orderkey
   assert((ol_sort_key_t{5, 3}.match(ol_sort_key_t{6, 0}) < 0));
   assert((ol_sort_key_t{6, 0}.match(ol_sort_key_t{5, 3}) > 0));

   // Different linenumber (neither zero)
   assert((ol_sort_key_t{5, 2}.match(ol_sort_key_t{5, 3}) < 0));
   assert((ol_sort_key_t{5, 3}.match(ol_sort_key_t{5, 2}) > 0));

   std::cerr << "  PASS: test_match_semantics\n";
}

// ---------------------------------------------------------------------------
// test_matching_keys: LINEITEM key yields two buckets; ORDERS key yields one

static void test_matching_keys()
{
   auto keys1 = ol_sort_key_t{5, 3}.matching_keys();
   assert(keys1.size() == 2);
   assert((std::find(keys1.begin(), keys1.end(), ol_sort_key_t{5, 0}) != keys1.end()));
   assert((std::find(keys1.begin(), keys1.end(), ol_sort_key_t{5, 3}) != keys1.end()));

   auto keys2 = ol_sort_key_t{5, 0}.matching_keys();
   assert(keys2.size() == 1);
   assert((keys2[0] == ol_sort_key_t{5, 0}));

   std::cerr << "  PASS: test_matching_keys\n";
}

// ---------------------------------------------------------------------------
// test_first_diff: returns (field index, other's value, signed distance)

static void test_first_diff()
{
   [[maybe_unused]] auto [idx1, base1, dist1] = ol_sort_key_t{5, 3}.first_diff(ol_sort_key_t{5, 1});
   assert(idx1 == 1);
   assert(base1 == 1);
   assert(dist1 == 2);

   [[maybe_unused]] auto [idx2, base2, dist2] = ol_sort_key_t{7, 0}.first_diff(ol_sort_key_t{5, 0});
   assert(idx2 == 0);
   assert(base2 == 5);
   assert(dist2 == 2);

   // Identical keys: distance must be zero
   [[maybe_unused]] auto [idx3, base3, dist3] = ol_sort_key_t{5, 3}.first_diff(ol_sort_key_t{5, 3});
   assert(dist3 == 0);

   std::cerr << "  PASS: test_first_diff\n";
}

// ---------------------------------------------------------------------------
// test_hash: hashes on orderkey only, so order-level and lineitem keys collide

static void test_hash()
{
   std::hash<tpch::ol_sort_key_t> hasher;

   // Same orderkey → same hash regardless of linenumber
   assert(hasher(ol_sort_key_t{5, 0}) == hasher(ol_sort_key_t{5, 3}));

   // Different orderkey → exercise the hasher (not guaranteed to differ, but
   // must not crash)
   auto h = hasher(ol_sort_key_t{1, 1});
   (void)h;

   std::cerr << "  PASS: test_hash\n";
}

// ---------------------------------------------------------------------------
// test_joined_ol_construction: payloads round-trip through accessors

static void test_joined_ol_construction()
{
   orders_t o{};
   o.o_custkey = 42;
   o.o_shippriority = 7;

   lineitem_t l{};
   l.l_partkey = 100;
   l.l_suppkey = 200;

   joined_ol_t j(o, l);
   assert(j.order().o_custkey == 42);
   assert(j.order().o_shippriority == 7);
   assert(j.line().l_partkey == 100);
   assert(j.line().l_suppkey == 200);

   std::cerr << "  PASS: test_joined_ol_construction\n";
}

// ---------------------------------------------------------------------------
// test_joined_ol_key: Key constructors propagate (orderkey, linenumber)

static void test_joined_ol_key()
{
   orders_t::Key ok{Integer(5)};
   lineitem_t::Key lk{Integer(5), Integer(3)};
   joined_ol_t::Key k(ok, lk);
   assert(k.jk.orderkey == 5);
   assert(k.jk.linenumber == 3);

   // Reconstruct from sort key
   joined_ol_t::Key k2(ol_sort_key_t{5, 3});
   assert(k2.jk.orderkey == 5);
   assert(k2.jk.linenumber == 3);

   std::cerr << "  PASS: test_joined_ol_key\n";
}

// ---------------------------------------------------------------------------
// test_skbuilder_create: derives sort key from each record's primary key

static void test_skbuilder_create()
{
   using SKB = SKBuilder<tpch::ol_sort_key_t>;

   orders_t::Key ok{Integer(5)};
   orders_t ov{};
   [[maybe_unused]] auto sk1 = SKB::create(ok, ov);
   assert(sk1.orderkey == 5);
   assert(sk1.linenumber == 0);  // ORDERS row → order-level granularity

   lineitem_t::Key lk{Integer(5), Integer(3)};
   lineitem_t lv{};
   [[maybe_unused]] auto sk2 = SKB::create(lk, lv);
   assert(sk2.orderkey == 5);
   assert(sk2.linenumber == 3);

   std::cerr << "  PASS: test_skbuilder_create\n";
}

// ---------------------------------------------------------------------------
// test_skbuilder_project: project drops linenumber for ORDERS, keeps it for LINEITEM

static void test_skbuilder_project()
{
   using SKB = SKBuilder<tpch::ol_sort_key_t>;
   ol_sort_key_t full{5, 3};

   [[maybe_unused]] auto proj_o = SKB::project<orders_t>(full);
   assert(proj_o.orderkey == 5);
   assert(proj_o.linenumber == 0);

   [[maybe_unused]] auto proj_l = SKB::project<lineitem_t>(full);
   assert(proj_l == full);

   std::cerr << "  PASS: test_skbuilder_project\n";
}

// ---------------------------------------------------------------------------
// test_skbuilder_to_key: produces the native primary key for each record type

static void test_skbuilder_to_key()
{
   using SKB = SKBuilder<tpch::ol_sort_key_t>;
   ol_sort_key_t sk{5, 3};

   [[maybe_unused]] auto ok = SKB::to_key<orders_t>(sk);
   assert(ok.o_orderkey == 5);

   [[maybe_unused]] auto lk = SKB::to_key<lineitem_t>(sk);
   assert(lk.l_orderkey == 5);
   assert(lk.l_linenumber == 3);

   [[maybe_unused]] auto jk = SKB::to_key<tpch::joined_ol_t>(sk);
   assert(jk.jk.orderkey == 5);
   assert(jk.jk.linenumber == 3);

   std::cerr << "  PASS: test_skbuilder_to_key\n";
}

// ---------------------------------------------------------------------------
// test_skbuilder_roundtrip: create → to_key must recover the original key fields

static void test_skbuilder_roundtrip()
{
   using SKB = SKBuilder<tpch::ol_sort_key_t>;

   // ORDERS round-trip
   orders_t::Key ok{Integer(5)};
   orders_t ov{};
   auto sk = SKB::create(ok, ov);
   [[maybe_unused]] auto ok2 = SKB::to_key<orders_t>(sk);
   assert(ok2.o_orderkey == ok.o_orderkey);

   // LINEITEM round-trip
   lineitem_t::Key lk{Integer(5), Integer(3)};
   lineitem_t lv{};
   auto sk2 = SKB::create(lk, lv);
   [[maybe_unused]] auto lk2 = SKB::to_key<lineitem_t>(sk2);
   assert(lk2.l_orderkey == lk.l_orderkey);
   assert(lk2.l_linenumber == lk.l_linenumber);

   std::cerr << "  PASS: test_skbuilder_roundtrip\n";
}

// ---------------------------------------------------------------------------

int main()
{
   test_sort_key_ordering();
   test_sort_key_max();
   test_match_semantics();
   test_matching_keys();
   test_first_diff();
   test_hash();
   test_joined_ol_construction();
   test_joined_ol_key();
   test_skbuilder_create();
   test_skbuilder_project();
   test_skbuilder_to_key();
   test_skbuilder_roundtrip();

   std::cout << "All tests passed.\n";
   return 0;
}
