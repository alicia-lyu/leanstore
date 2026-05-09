#pragma once

// Q12-specific record types layered on top of the shared ORDERS x LINEITEM
// pipeline types in ../views_ol.hpp.
//
// Only row-shape types live here: the pipeline view alias and the aggregate
// output row. Query-time substitution parameters (Params) and predicate
// declarations live in workload.hpp alongside Q12Workload.

#include "../tpch_family/views_ol.hpp"

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: ORDERS x LINEITEM join output, unfiltered.
// Predicate is hoisted to query time so the view is reusable across param sets.
//
// G8d (2026-05-03): q12_pipeline_view_t was previously a `using` alias for
// joined_ol_t (full O × L payloads). Per CLAUDE.md §Project pushdown, this
// is a pre-aggregated secondary (S2's per-query view) and should carry only
// the columns query_by_view + q12_predicate_joined read. Promoted to a real
// struct keyed by (o_orderkey, l_linenumber) carrying the five Q12-relevant
// fields. Widen in place if a future Q12 variant needs more columns.

struct q12_pipeline_view_t {
   static constexpr int id = 34;

   struct Key {
      static constexpr int id = 34;
      Integer o_orderkey;
      Integer l_linenumber;
      ADD_KEY_TRAITS(&Key::o_orderkey, &Key::l_linenumber)
   };

   Varchar<10> l_shipmode;
   Varchar<15> o_orderpriority;
   Timestamp   l_shipdate;
   Timestamp   l_commitdate;
   Timestamp   l_receiptdate;

   ADD_RECORD_TRAITS(q12_pipeline_view_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Final aggregate output row: one per shipmode group.

struct q12_agg_row_t {
   static constexpr int id = 31;

   struct Key {
      static constexpr int id = 31;
      Varchar<10> l_shipmode;
      ADD_KEY_TRAITS(&Key::l_shipmode)
   };

   Varchar<10> l_shipmode;
   Integer high_line_count;
   Integer low_line_count;

   ADD_RECORD_TRAITS(q12_agg_row_t)

   void print(std::ostream& os) const;
};

}  // namespace tpch::q12
