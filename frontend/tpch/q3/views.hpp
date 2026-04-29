#pragma once

// Q3-specific record types layered on top of the shared ORDERS x LINEITEM
// pipeline types in ../views_ol.hpp.
//
// Three types are defined here:
//   - Params              : TPC-H §2.4.3 substitution parameters.
//   - q3_pipeline_view_t  : structure 2 intermediate view (aliases joined_ol_t).
//   - q3_agg_row_t        : final aggregate output row (one row per orderkey group).
//
// Predicate function declarations live here; bodies are in query.tpp.

#include "../views_ol.hpp"

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.3).
// Defaults: SEGMENT=BUILDING, DATE=1995-03-15 (days since epoch: 9205).

struct Params {
   Varchar<10> mktsegment;    // default "BUILDING"
   Timestamp   orderdate_hi;  // default 1995-03-15; orders with o_orderdate < this pass
   Timestamp   shipdate_lo;   // default 1995-03-15; lineitems with l_shipdate > this pass

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row — PLACEHOLDER.
// OPERATORS.md §4: Q3's view stores post-SortedAggregate rows (one per
// orderkey with summed revenue + FD-attached o_orderdate, o_shippriority,
// o_custkey). This alias MUST be replaced with a proper q3_pipeline_view_t
// record type during implementation. See OPERATORS.md §4 for the schema.
using q3_pipeline_view_t = ::tpch::joined_ol_t;  // TODO: replace with aggregated type

// ---------------------------------------------------------------------------
// Final aggregate output row: one per (l_orderkey, o_orderdate, o_shippriority).

struct q3_agg_row_t {
   static constexpr int id = 32;

   struct Key {
      static constexpr int id = 32;
      Integer   l_orderkey;
      Timestamp o_orderdate;
      Integer   o_shippriority;
      ADD_KEY_TRAITS(&Key::l_orderkey, &Key::o_orderdate, &Key::o_shippriority)
   };

   Integer   l_orderkey;
   Timestamp o_orderdate;
   Integer   o_shippriority;
   Numeric   revenue;  // SUM(l_extendedprice * (1 - l_discount))

   ADD_RECORD_TRAITS(q3_agg_row_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to a raw orders_t before joining (structures 1 and 4).
// Encodes: o_orderdate < p.orderdate_hi
// See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 1.
bool q3_predicate_orders(const orders_t& o, const Params& p);

// Applied to a raw lineitem_t before joining (structures 1 and 4).
// Encodes: l_shipdate > p.shipdate_lo
// See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 1.
bool q3_predicate_lineitem(const lineitem_t& l, const Params& p);

// Applied to a fully-assembled joined_ol_t (structures 2 and 3).
// Encodes: o_orderdate < p.orderdate_hi AND l_shipdate > p.shipdate_lo
// See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 3.
bool q3_predicate_joined(const joined_ol_t& j, const Params& p);

}  // namespace tpch::q3
