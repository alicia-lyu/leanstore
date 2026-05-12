// Unit tests for frontend/tpch/q3_family/{accumulators,predicates,params}.hpp.
//
// All cases are pure-types — no DB required. Stub records are constructed
// directly and fed to the family functions to verify the shared scaffolding
// consumed by Q3 and Q3I.
//
// CMake target: test_q3_family
// Build:  cmake --build build --target test_q3_family -j$(sysctl -n hw.ncpu)
// Run:    ./build/frontend/test_q3_family

#include "../q3_family/accumulators.hpp"
#include "../q3_family/params.hpp"
#include "../q3_family/predicates.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <set>
#include <string>
#include <tuple>

using namespace tpch;
using namespace tpch::q3_family;

// ---------------------------------------------------------------------------
// Minimal Params-like struct satisfying accumulator + predicate requirements.
// Mirrors the Params structs in per-query workload.hpp files.

struct TestParams {
   Varchar<10> mktsegment;
   Timestamp   orderdate;
   Timestamp   shipdate;
};

// ---------------------------------------------------------------------------
// Helper: build a stub lineitem_t with just the revenue-relevant fields set.

static lineitem_t make_lineitem(Numeric extprice, Numeric discount, Timestamp shipdate)
{
   lineitem_t l{};
   l.l_extendedprice = extprice;
   l.l_discount      = discount;
   l.l_shipdate      = shipdate;
   return l;
}

// ---------------------------------------------------------------------------
// Test 1: LineitemRevenueAccumulator — 3-row stub, reset between groups
//
// Revenue = Σ extendedprice × (1 - discount) for rows passing shipdate filter.
// The accumulator uses strict > comparison: l_shipdate > params.shipdate.

static void test_accumulator_revenue()
{
   const TestParams p{Varchar<10>("BUILDING"), DATE_1995_03_15, DATE_1995_03_15};
   LineitemRevenueAccumulator<TestParams> acc;

   // Row 1: shipdate = DATE+1 (passes), extprice=100, discount=0.1 → +90.0
   acc.consume(make_lineitem(100.0, 0.1, DATE_1995_03_15 + 1), p);
   // Row 2: shipdate = DATE (fails — not strictly greater), extprice=200, discount=0.2
   acc.consume(make_lineitem(200.0, 0.2, DATE_1995_03_15), p);
   // Row 3: shipdate = DATE+100 (passes), extprice=50, discount=0.0 → +50.0
   acc.consume(make_lineitem(50.0, 0.0, DATE_1995_03_15 + 100), p);

   // Expected revenue: 100*(1-0.1) + 50*(1-0.0) = 90 + 50 = 140.
   assert(acc.revenue == 140.0);

   // reset() clears revenue for the next group.
   acc.reset();
   assert(acc.revenue == 0.0);

   // Verify reset is clean by re-accumulating one row.
   acc.consume(make_lineitem(10.0, 0.5, DATE_1995_03_15 + 1), p);
   assert(acc.revenue == 5.0);

   std::cerr << "  PASS: test_accumulator_revenue\n";
}

// ---------------------------------------------------------------------------
// Test 2: LineitemRevenueAccumulator — coli and acoli overloads
//
// Same arithmetic must hold for lineitem_coli_t and lineitem_acoli_t overloads.

static void test_accumulator_coli_overloads()
{
   const TestParams p{Varchar<10>("BUILDING"), DATE_1995_03_15, DATE_1995_03_15};

   // lineitem_coli_t overload.
   {
      LineitemRevenueAccumulator<TestParams> acc;
      lineitem_coli_t lc{};
      lc.l_extendedprice = 200.0;
      lc.l_discount      = 0.25;
      lc.l_shipdate      = DATE_1995_03_15 + 1;  // passes
      acc.consume(lc, p);
      assert(acc.revenue == 200.0 * (1.0 - 0.25));
   }

   // lineitem_acoli_t overload.
   {
      LineitemRevenueAccumulator<TestParams> acc;
      lineitem_acoli_t la{};
      la.l_extendedprice = 300.0;
      la.l_discount      = 0.0;
      la.l_shipdate      = DATE_1995_03_15 + 1;  // passes
      acc.consume(la, p);
      assert(acc.revenue == 300.0);
   }

   std::cerr << "  PASS: test_accumulator_coli_overloads\n";
}

// ---------------------------------------------------------------------------
// Test 3: q3_predicate_customer — true iff c_mktsegment == params.mktsegment

static void test_predicate_customer()
{
   const char* segments[] = {
       "AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};

   for (const char* seg : segments) {
      customerh_t c{};
      c.c_mktsegment = Varchar<10>(seg);

      // Matching segment → true.
      [[maybe_unused]] TestParams p_match{Varchar<10>(seg), DATE_1995_03_15, DATE_1995_03_15};
      assert(q3_predicate_customer(c, p_match));

      // Non-matching segment → false (pick a different one).
      const char* other = (std::strcmp(seg, "BUILDING") == 0) ? "FURNITURE" : "BUILDING";
      [[maybe_unused]] TestParams p_miss{Varchar<10>(other), DATE_1995_03_15, DATE_1995_03_15};
      assert(!q3_predicate_customer(c, p_miss));
   }

   std::cerr << "  PASS: test_predicate_customer\n";
}

// ---------------------------------------------------------------------------
// Test 4: q3_predicate_orders — strict < on o_orderdate < params.orderdate
//
// Boundary: equal-to → false, one-day-less → true.

static void test_predicate_orders()
{
   [[maybe_unused]] const TestParams p{Varchar<10>("BUILDING"), DATE_1995_03_15, DATE_1995_03_15};

   orders_t o{};

   // Strictly less → true.
   o.o_orderdate = DATE_1995_03_15 - 1;
   assert(q3_predicate_orders(o, p));

   // Equal → false (strict <).
   o.o_orderdate = DATE_1995_03_15;
   assert(!q3_predicate_orders(o, p));

   // Greater → false.
   o.o_orderdate = DATE_1995_03_15 + 1;
   assert(!q3_predicate_orders(o, p));

   std::cerr << "  PASS: test_predicate_orders\n";
}

// ---------------------------------------------------------------------------
// Test 5: q3_predicate_lineitem — strict > on l_shipdate > params.shipdate
//
// Boundary: equal-to → false, one-day-greater → true.

static void test_predicate_lineitem()
{
   [[maybe_unused]] const TestParams p{Varchar<10>("BUILDING"), DATE_1995_03_15, DATE_1995_03_15};

   lineitem_t l{};

   // Strictly greater → true.
   l.l_shipdate = DATE_1995_03_15 + 1;
   assert(q3_predicate_lineitem(l, p));

   // Equal → false (strict >).
   l.l_shipdate = DATE_1995_03_15;
   assert(!q3_predicate_lineitem(l, p));

   // Less → false.
   l.l_shipdate = DATE_1995_03_15 - 1;
   assert(!q3_predicate_lineitem(l, p));

   std::cerr << "  PASS: test_predicate_lineitem\n";
}

// ---------------------------------------------------------------------------
// Test 6: PARAM_TABLE — 10 entries covering all 5 segments and 5 dates,
// no duplicates, correct rotation period via set_params_for_iter.

static void test_param_table()
{
   // Exactly 10 entries.
   assert(PARAM_TABLE_SIZE == 10);

   // All 5 TPC-H market segments appear at least once.
   const char* expected_segs[] = {
       "AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
   for (const char* seg : expected_segs) {
      bool found = false;
      for (long i = 0; i < PARAM_TABLE_SIZE; ++i) {
         if (std::strcmp(PARAM_TABLE[i].segment, seg) == 0) {
            found = true;
            break;
         }
      }
      assert(found);
      (void)found;
   }

   // No duplicate (segment, date) tuples.
   std::set<std::pair<std::string, Timestamp>> seen;
   for (long i = 0; i < PARAM_TABLE_SIZE; ++i) {
      auto key = std::make_pair(std::string(PARAM_TABLE[i].segment), PARAM_TABLE[i].date);
      assert(seen.find(key) == seen.end() && "duplicate entry in PARAM_TABLE");
      seen.insert(key);
   }

   // set_params_for_iter cycles with period 10: entry at iter and iter+10
   // must be identical.
   for (long iter = 0; iter < 20; ++iter) {
      [[maybe_unused]] const auto& a = PARAM_TABLE[iter % PARAM_TABLE_SIZE];
      [[maybe_unused]] const auto& b = PARAM_TABLE[(iter + 10) % PARAM_TABLE_SIZE];
      assert(std::strcmp(a.segment, b.segment) == 0);
      assert(a.date == b.date);
   }

   // Validation defaults (BUILDING / 1995-03-15) must be first entry.
   assert(std::strcmp(PARAM_TABLE[0].segment, "BUILDING") == 0);
   assert(PARAM_TABLE[0].date == DATE_1995_03_15);

   std::cerr << "  PASS: test_param_table\n";
}

// ---------------------------------------------------------------------------
// main

int main()
{
   std::cerr << "Running test_q3_family...\n";
   test_accumulator_revenue();
   test_accumulator_coli_overloads();
   test_predicate_customer();
   test_predicate_orders();
   test_predicate_lineitem();
   test_param_table();
   std::cerr << "All tests passed.\n";
   return 0;
}
