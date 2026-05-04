#pragma once

// Q3-specific record types for the COL pipeline (CUSTOMER × ORDERS × LINEITEM).
//
// Three types are defined here:
//   - q3_pipeline_view_t  : Structure 2 intermediate view — one row per
//                           (custkey, orderkey, linenumber), unaggregated
//                           lineitem fields + FD-attached order/customer cols.
//   - q3_agg_row_t        : aliases q3_family::q3_agg_row_base_t directly
//                           (Q3 has no invoice extension; the four base fields
//                           are everything Q3 needs).
//
// Params struct and predicate declarations live in workload.hpp.
// SKBuilder specialisation for q3_pipeline_view_t::Key lives below.

#include "../q3_family/agg_row.hpp"
#include "../views_col.hpp"

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: per-(custkey, orderkey, linenumber) row.
//
// Schema: one row per lineitem, carrying unaggregated lineitem fields so
// query_by_view can apply the shipdate filter live and accumulate revenue
// per orderkey at query time.  No filters are baked in — predicate hoisting
// keeps the view reusable across all SEGMENT / DATE param sets.
//
// FD-attached customer / order columns (c_mktsegment, o_orderdate,
// o_shippriority) are copied into every lineitem row — the same
// "schema-faithful secondary" policy as the COL MI.

struct q3_pipeline_view_t {
   static constexpr int id = 35;

   struct Key {
      static constexpr int id = 35;
      Integer custkey;     // primary sort — groups custkey partitions
      Integer orderkey;    // secondary sort — unique within a custkey group
      Integer linenumber;  // tertiary sort — unique within an order
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey, &Key::linenumber)
   };

   // Unaggregated lineitem fields — revenue computed at query time.
   Numeric   l_extendedprice;
   Numeric   l_discount;
   Timestamp l_shipdate;

   // FD-attached customer / order columns (parameter-independent at load time).
   Varchar<10> c_mktsegment;    // customer market segment (filter at query time)
   Timestamp   o_orderdate;     // order date (filter at query time)
   Integer     o_shippriority;  // output column

   ADD_RECORD_TRAITS(q3_pipeline_view_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Final aggregate output row: Q3 uses the base type directly (no extension).

using q3_agg_row_t = q3_family::q3_agg_row_base_t;

}  // namespace tpch::q3

// ---------------------------------------------------------------------------
// SKBuilder specialisation for q3_pipeline_view_t::Key.
//
// Required by BinaryMergeJoin / PremergedJoin callers that derive join keys
// from this record type.  Q3's view is not used in a join directly — the
// Phase-0.5 query_by_view is a stub — but the specialisation must exist for
// the template to instantiate cleanly when Q3Workload is compiled.

template <>
struct SKBuilder<tpch::q3::q3_pipeline_view_t::Key> {
   using JK = tpch::q3::q3_pipeline_view_t::Key;

   static JK create(const JK& k, const tpch::q3::q3_pipeline_view_t&)
   {
      return k;
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};
