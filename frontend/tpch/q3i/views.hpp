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
// Structure 2 pipeline view row: ORDERS x LINEITEM join output, unfiltered.
// TODO: define a wider join-result type that includes the per-customer
// open-invoice-due sub-aggregate once the COLI MI driver is implemented.
// For the skeleton, alias to joined_ol_t.

using q3i_pipeline_view_t = ::tpch::joined_ol_t;

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
