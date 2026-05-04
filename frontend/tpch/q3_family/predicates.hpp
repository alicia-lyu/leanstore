#pragma once

// Shared predicate functions for the Q3 / Q3I query family.
//
// These three filters are driven by the same SUBSTITUTION-PARAMETER set
// (mktsegment, orderdate, shipdate) that both Q3 and Q3I share per the
// TPC-H §2.4.3 spec.  They are templated on the Params type so each
// per-query workload.hpp can define its own Params struct without coupling
// to the other query's definition.  The only requirement is that P carries:
//   - a `mktsegment` Varchar field
//   - an `orderdate` Timestamp field
//   - a `shipdate`   Timestamp field
//
// Q3I-specific predicates (invoice status filter, joined_ol_t combined
// predicate) remain in q3i/workload.hpp because they have no Q3 analogue.

#include <string_view>

#include "../tpch_tables.hpp"

namespace tpch::q3_family
{

// q3_predicate_customer — c_mktsegment = params.mktsegment.
// Applied before joining to skip entire custkey groups that cannot qualify.
template <typename P>
inline bool q3_predicate_customer(const customerh_t& c, const P& p)
{
   auto sm  = std::string_view(c.c_mktsegment.data, c.c_mktsegment.length);
   auto psm = std::string_view(p.mktsegment.data,   p.mktsegment.length);
   return sm == psm;
}

// q3_predicate_orders — o_orderdate < params.orderdate.
// Applied to an orders row before (or during) the join.
template <typename P>
inline bool q3_predicate_orders(const orders_t& o, const P& p)
{
   return o.o_orderdate < p.orderdate;
}

// q3_predicate_lineitem — l_shipdate > params.shipdate.
// Applied to a lineitem row before (or during) the join.
template <typename P>
inline bool q3_predicate_lineitem(const lineitem_t& l, const P& p)
{
   return l.l_shipdate > p.shipdate;
}

}  // namespace tpch::q3_family
