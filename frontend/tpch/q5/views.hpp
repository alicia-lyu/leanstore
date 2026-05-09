#pragma once

// Q5-specific record types for the COL pipeline (CUSTOMER × ORDERS × LINEITEM).
//
// Two types are defined here:
//   - q5_pipeline_view_t  : Structure 2 intermediate view — one row per
//                           (custkey, orderkey, linenumber), unaggregated
//                           lineitem fields + FD-attached order/customer cols.
//   - q5_agg_row_t        : final aggregate output row, one per in-region
//                           nation (~5 rows).
//
// ID allocation (verified 2026-05-08 against all existing record type IDs
// in the tpch directory — last used was 55 in tpchi_tables.hpp):
//   q5_pipeline_view_t : id = 56
//   q5_agg_row_t       : id = 57
//
// Params struct and predicate declarations live in workload.hpp.
// SKBuilder specialisation for q5_pipeline_view_t::Key lives below.

#include <functional>
#include <limits>

#include "../tpch_family/views_col.hpp"
#include "../tpch_tables.hpp"

namespace tpch::q5
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: per-(custkey, orderkey, linenumber) row.
//
// Schema: one row per lineitem, carrying unaggregated lineitem fields so
// query_by_view can apply the orderdate filter live and accumulate revenue
// per n_name at query time.  No filters are baked in — predicate hoisting
// keeps the view reusable across all REGION / DATE param sets.
//
// FD-attached customer / order columns (c_nationkey, o_orderdate) are
// copied into every lineitem row — the same "schema-faithful secondary"
// policy as the COL MI.
//
// l_suppkey is included so query_by_view can perform the SUPPLIER hashmap
// probe and cross-equality check (c_nationkey = s_nationkey) without
// re-joining base tables.

struct q5_pipeline_view_t {
   static constexpr int id = 56;

   struct Key {
      static constexpr int id = 56;
      Integer custkey;     // primary sort — groups custkey partitions
      Integer orderkey;    // secondary sort — unique within a custkey group
      Integer linenumber;  // tertiary sort — unique within an order
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey, &Key::linenumber)
   };

   // Unaggregated lineitem fields — revenue computed at query time.
   Numeric   l_extendedprice;
   Numeric   l_discount;
   Integer   l_suppkey;      // supplier join key for SUPPLIER probe

   // FD-attached customer / order columns (parameter-independent at load time).
   Integer   c_nationkey;    // customer's nation (filter + GROUP BY driver)
   Timestamp o_orderdate;    // order date (filter at query time)

   ADD_RECORD_TRAITS(q5_pipeline_view_t)

   // TODO Phase 1: implement real unfoldKey body.
   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Final aggregate output row: one row per in-region nation (~5 rows).
//
// This is an in-memory result type — never stored in a B-tree or RocksDB
// column family — so it has no Key sub-struct, no SKBuilder, and no
// ADD_RECORD_TRAITS.  Fields are stored flat, analogous to q3_agg_row_base_t.
//
// Design note: the SQL groups by n_name, but n_nationkey is 1:1 with n_name
// so we track accumulation by nationkey internally and carry n_name for output.
// The result is sorted by revenue DESC (no LIMIT; bounded by |nation_set| ≈ 5).
//
// id = 57 is reserved here for future use (e.g. if this type is ever stored
// in a secondary structure).  It is not currently active.

struct q5_agg_row_t {
   Integer     n_nationkey;  // grouping key (1:1 with n_name)
   Varchar<25> n_name;       // output column (GROUP BY n_name per spec)
   Numeric     revenue;      // SUM(l_extendedprice * (1 - l_discount))

   void print(std::ostream& os) const;

   // Comparator for sorting: revenue DESC (no tiebreaker needed — ~5 rows).
   static bool cmp(const q5_agg_row_t& a, const q5_agg_row_t& b)
   {
      return a.revenue > b.revenue;
   }
};

}  // namespace tpch::q5

// ---------------------------------------------------------------------------
// SKBuilder specialisation for q5_pipeline_view_t::Key.
//
// Required so Backend::template Adapter<q5_pipeline_view_t> instantiates
// cleanly when Q5Workload is compiled.  Q5's view is not used in a join
// directly at Phase 0.5 — query_by_view is a stub — but the specialisation
// must exist for the template to compile.

template <>
struct SKBuilder<tpch::q5::q5_pipeline_view_t::Key> {
   using JK = tpch::q5::q5_pipeline_view_t::Key;

   static JK create(const JK& k, const tpch::q5::q5_pipeline_view_t&)
   {
      return k;
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};
