#pragma once

// Q12-specific record types layered on top of the shared ORDERS x LINEITEM
// pipeline types in ../views_ol.hpp.
//
// Only row-shape types live here: the pipeline view alias and the aggregate
// output row. Query-time substitution parameters (Params) and predicate
// declarations live in workload.hpp alongside Q12Workload.

#include "../views_ol.hpp"

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: ORDERS x LINEITEM join output, unfiltered.
// Predicate is hoisted to query time so the view is reusable across param sets.
// Aliases joined_ol_t — no narrower projection needed for Q12.

using q12_pipeline_view_t = ::tpch::joined_ol_t;

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
