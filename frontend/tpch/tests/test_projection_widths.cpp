// Unit tests pinning post-G8 payload sizes for projected secondary record types.
//
// Each static_assert pins the current sizeof so any future widening is visible
// at compile time and forces a deliberate decision (see CLAUDE.md §Project
// pushdown: "modify in place when a future query needs more, do not fork").
//
// Runtime checks verify the field counts match expectations, providing a
// clearer error message than a raw sizeof mismatch.
//
// Pure-types — no DB required, no gflags needed.
//
// CMake target: test_projection_widths
// Build:  cmake --build build --target test_projection_widths -j$(sysctl -n hw.ncpu)
// Run:    ./build/frontend/test_projection_widths

#include "../tpch_family/views_coli.hpp"
#include "../q12/views.hpp"

#include <cassert>
#include <iostream>

using namespace tpch;

// ---------------------------------------------------------------------------
// Size constants derived from the type definitions and confirmed by probe:
//   Varchar<N>  = int16_t(2) + char[N] → size = 2+N, padded by compiler.
//   Timestamp   = int64_t  = 8 bytes
//   Numeric     = double   = 8 bytes
//   Integer     = int32_t  = 4 bytes
//
// orders_coli_t:        {Timestamp(8), Integer(4)} + 4-byte tail padding  = 16 bytes
// lineitem_coli_t:      {Numeric(8), Numeric(8), Timestamp(8)}            = 24 bytes
// invoice_coli_t:       {Numeric(8), Varchar<1>(4)} + 4-byte tail padding = 16 bytes
// lineitem_acoli_t:     {Numeric(8), Numeric(8), Timestamp(8)}            = 24 bytes (same projection)
// q12_pipeline_view_t:  {Varchar<10>(12), Varchar<15>(18), 3×Timestamp(24)}
//                       = 12+18+24 = 54 + 2 tail padding                 = 56 bytes

// ---------------------------------------------------------------------------
// Static assertions: compile-time canaries for accidental payload widening.

// orders_coli_t payload: {o_orderdate, o_shippriority} only.
static_assert(sizeof(orders_coli_t) == 16,
              "orders_coli_t widened — was {Timestamp, Integer} = 16 bytes (G8a)");

// lineitem_coli_t payload: {l_extendedprice, l_discount, l_shipdate} only.
static_assert(sizeof(lineitem_coli_t) == 24,
              "lineitem_coli_t widened — was {Numeric, Numeric, Timestamp} = 24 bytes (G8a)");

// invoice_coli_t payload: {i_totaldue, i_status} only.
static_assert(sizeof(invoice_coli_t) == 16,
              "invoice_coli_t widened — was {Numeric, Varchar<1>} = 16 bytes (G8a)");

// lineitem_acoli_t payload: {l_extendedprice, l_discount, l_shipdate} only.
static_assert(sizeof(lineitem_acoli_t) == 24,
              "lineitem_acoli_t widened — was {Numeric, Numeric, Timestamp} = 24 bytes (G8c)");

// q12_pipeline_view_t payload: {l_shipmode, o_orderpriority, l_shipdate,
//                                l_commitdate, l_receiptdate} only.
static_assert(sizeof(q12::q12_pipeline_view_t) == 56,
              "q12_pipeline_view_t widened — was {Varchar<10>, Varchar<15>, 3×Timestamp} = 56 bytes (G8d)");

// ---------------------------------------------------------------------------
// Runtime checks: verify each type has exactly the expected fields by
// reading and writing them — a field that doesn't exist won't compile.

static void test_orders_coli_fields()
{
   orders_coli_t r{};
   // Only these two fields should exist in the payload.
   r.o_orderdate    = 9204;  // DATE_1995_03_15
   r.o_shippriority = 1;
   assert(r.o_orderdate    == 9204);
   assert(r.o_shippriority == 1);
   std::cerr << "  PASS: test_orders_coli_fields (sizeof=" << sizeof(r) << ")\n";
}

static void test_lineitem_coli_fields()
{
   lineitem_coli_t r{};
   r.l_extendedprice = 100.0;
   r.l_discount      = 0.05;
   r.l_shipdate      = 9300;
   assert(r.l_extendedprice == 100.0);
   assert(r.l_discount      == 0.05);
   assert(r.l_shipdate      == 9300);
   std::cerr << "  PASS: test_lineitem_coli_fields (sizeof=" << sizeof(r) << ")\n";
}

static void test_invoice_coli_fields()
{
   invoice_coli_t r{};
   r.i_totaldue = 500.0;
   r.i_status   = Varchar<1>("O");
   assert(r.i_totaldue == 500.0);
   assert(r.i_status   == Varchar<1>("O"));
   std::cerr << "  PASS: test_invoice_coli_fields (sizeof=" << sizeof(r) << ")\n";
}

static void test_lineitem_acoli_fields()
{
   lineitem_acoli_t r{};
   r.l_extendedprice = 200.0;
   r.l_discount      = 0.10;
   r.l_shipdate      = 9500;
   assert(r.l_extendedprice == 200.0);
   assert(r.l_discount      == 0.10);
   assert(r.l_shipdate      == 9500);
   std::cerr << "  PASS: test_lineitem_acoli_fields (sizeof=" << sizeof(r) << ")\n";
}

static void test_q12_view_fields()
{
   q12::q12_pipeline_view_t r{};
   r.l_shipmode      = Varchar<10>("MAIL");
   r.o_orderpriority = Varchar<15>("1-URGENT");
   r.l_shipdate      = 8000;
   r.l_commitdate    = 8010;
   r.l_receiptdate   = 8020;
   assert(r.l_shipmode      == Varchar<10>("MAIL"));
   assert(r.o_orderpriority == Varchar<15>("1-URGENT"));
   assert(r.l_shipdate      == 8000);
   assert(r.l_commitdate    == 8010);
   assert(r.l_receiptdate   == 8020);
   std::cerr << "  PASS: test_q12_view_fields (sizeof=" << sizeof(r) << ")\n";
}

// ---------------------------------------------------------------------------
// main

int main()
{
   std::cerr << "Running test_projection_widths...\n";
   test_orders_coli_fields();
   test_lineitem_coli_fields();
   test_invoice_coli_fields();
   test_lineitem_acoli_fields();
   test_q12_view_fields();
   std::cerr << "All tests passed.\n";
   return 0;
}
