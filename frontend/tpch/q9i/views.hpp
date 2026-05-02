#pragma once

// Q9I-specific record types: Q9 extended with a paid-profit aggregate joining
// the INVOICE table via the COLI 4-table merged index.
//
// Row-shape types only.  Query-time parameters and predicate declarations live
// in workload.hpp alongside Q9IWorkload.

#include "../views_coli.hpp"
#include "../views_ol.hpp"

namespace tpch::q9i
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: ORDERS x LINEITEM join output, unfiltered.
// TODO: define a wider join-result type that includes nation and invoice fields
// once the COLI MI driver is implemented.  For the skeleton, alias to joined_ol_t.

using q9i_pipeline_view_t = ::tpch::joined_ol_t;

// ---------------------------------------------------------------------------
// Final aggregate output row: one per (nation, o_year) group.

struct q9i_agg_row_t {
   static constexpr int id = 43;

   struct Key {
      static constexpr int id = 43;
      Varchar<25> nation;
      Integer     o_year;
      ADD_KEY_TRAITS(&Key::nation, &Key::o_year)
   };

   Varchar<25> nation;
   Integer     o_year;
   Numeric     profit;       // SUM(amount) — Q9 definition of amount
   Numeric     paid_profit;  // SUM(amount) WHERE i_status='P'

   ADD_RECORD_TRAITS(q9i_agg_row_t)

   void print(std::ostream& os) const;
};

}  // namespace tpch::q9i
