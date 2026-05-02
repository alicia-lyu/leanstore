#pragma once

// Q3I-specific record types: Q3 extended with a per-customer open-invoice-due
// aggregate joined from the INVOICE table via the COLI 4-table merged index.
//
// Row-shape types only.  Query-time parameters and predicate declarations live
// in workload.hpp alongside Q3IWorkload.

#include "../views_coli.hpp"
#include "../views_ol.hpp"

namespace tpch::q3i
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: per-(custkey, orderkey) post-aggregate row.
//
// Rationale for widening from joined_ol_t alias:
//   The family logical plan's inside-pipeline SortedAggregate collapses all
//   lineitems per orderkey into one revenue sum.  The view therefore has one
//   row per (custkey, orderkey) — roughly |orders| rows — which is far smaller
//   than joined_ol_t × N_lineitems_per_order.  Carrying cust_open_due,
//   c_mktsegment, o_orderdate, and o_shippriority in the view row means S2's
//   query time is a single sequential scan with per-row mktsegment + threshold
//   filters, with no secondary invoice lookup needed.  This makes S2 a fair
//   comparison point against S1/S3 (both of which build cust_open_due in a
//   single streaming pass) while keeping the view UNFILTERED on parametrised
//   predicates so it stays reusable across param sets (predicate hoisting).

struct q3i_pipeline_view_t {
   static constexpr int id = 43;

   struct Key {
      static constexpr int id = 43;
      Integer custkey;   // primary sort — groups custkey partitions
      Integer orderkey;  // secondary sort — unique within a custkey group
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey)
   };

   Numeric     revenue;         // SUM(l_extendedprice * (1 - l_discount)) per orderkey
   Numeric     cust_open_due;   // SUM(i_totaldue WHERE i_status='O') per custkey
   Varchar<10> c_mktsegment;    // customer market segment (filter at query time)
   Timestamp   o_orderdate;     // order date (filter at query time)
   Integer     o_shippriority;  // output column

   ADD_RECORD_TRAITS(q3i_pipeline_view_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Intermediate row types used by S1/S2 query drivers (Phase 2).

// Per-customer open-invoice-due aggregate: one row per custkey in the
// custkey-sorted COLI secondary invoice index.
struct cust_open_due_t {
   static constexpr int id = 44;

   struct Key {
      static constexpr int id = 44;
      Integer custkey;
      ADD_KEY_TRAITS(&Key::custkey)
   };

   Numeric cust_open_due;  // SUM(i_totaldue WHERE i_status='O')

   ADD_RECORD_TRAITS(cust_open_due_t)

   void print(std::ostream& os) const;
};

// Per-(custkey, orderkey) lineitem revenue aggregate: output of the
// LineitemRevenueAggregator scanner-wrapper (OPERATORS.md §3 op 6).
// One row per orderkey after SUM(l_extendedprice * (1 - l_discount)).
struct lineitem_agg_t {
   static constexpr int id = 45;

   struct Key {
      static constexpr int id = 45;
      Integer custkey;
      Integer orderkey;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey)
   };

   Numeric revenue;  // SUM(l_extendedprice * (1 - l_discount))

   ADD_RECORD_TRAITS(lineitem_agg_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Final aggregate output row: one per (orderkey, orderdate, shippriority,
// cust_open_due) group, ordered by revenue DESC LIMIT 10.

struct q3i_agg_row_t {
   static constexpr int id = 42;

   struct Key {
      static constexpr int id = 42;
      Integer o_orderkey;
      ADD_KEY_TRAITS(&Key::o_orderkey)
   };

   Integer   o_orderkey;
   Numeric   revenue;        // SUM(l_extendedprice * (1 - l_discount))
   Timestamp o_orderdate;
   Integer   o_shippriority;
   Numeric   cust_open_due;  // SUM(i_totaldue) WHERE i_status='O' per custkey

   ADD_RECORD_TRAITS(q3i_agg_row_t)

   void print(std::ostream& os) const;
};

}  // namespace tpch::q3i
