#pragma once

// Q12I-specific record types: Q12 extended with an invoice late-status aggregate.
//
// Q12I adds a third aggregate column (late_invoiced) to Q12 by joining with
// the INVOICE table via the COLI 4-table merged index.  The pipeline view
// alias and aggregate output row are defined here; query-time parameters and
// predicate declarations live in workload.hpp.

#include "../views_coli.hpp"
#include "../views_ol.hpp"

namespace tpch::q12i
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: ORDERS x LINEITEM join output, unfiltered.
// TODO: define a wider join-result type that includes invoice fields once
// the COLI MI driver is implemented. For the skeleton, alias to joined_ol_t.

using q12i_pipeline_view_t = ::tpch::joined_ol_t;

// ---------------------------------------------------------------------------
// Final aggregate output row: one per shipmode group.

struct q12i_agg_row_t {
   static constexpr int id = 41;

   struct Key {
      static constexpr int id = 41;
      Varchar<10> l_shipmode;
      ADD_KEY_TRAITS(&Key::l_shipmode)
   };

   Varchar<10> l_shipmode;
   Integer hi;             // orders with '1-URGENT' or '2-HIGH' priority
   Integer lo;             // orders without high priority
   Integer late_invoiced;  // lineitems where i_status = 'L'

   ADD_RECORD_TRAITS(q12i_agg_row_t)

   void print(std::ostream& os) const;
};

}  // namespace tpch::q12i
