#pragma once

// tpch_family/revenue.hpp — shared revenue arithmetic for all TPC-H queries.
//
// `tpch::lineitem_revenue(l)` computes the per-lineitem revenue contribution
// SUM(l_extendedprice * (1 - l_discount)) without any date gate.  It is
// generic over any record type that exposes `l_extendedprice` and
// `l_discount` fields — lineitem_t, lineitem_col_t, lineitem_coli_t,
// lineitem_acoli_t, and future Q5 / Q10 view rows all qualify.
//
// The shipdate gate (l_shipdate > params.shipdate) is Q3-family-specific and
// lives in q3_family/accumulators.hpp::LineitemRevenueAccumulator.  Q5 calls
// this function directly inside its own per-n_name accumulator.

#include "../../shared/Types.hpp"

namespace tpch
{

// lineitem_revenue — pure arithmetic, no predicate.
//
// L must expose:
//   Numeric l_extendedprice
//   Numeric l_discount
template <typename L>
inline Numeric lineitem_revenue(const L& l)
{
   return l.l_extendedprice * (Numeric(1) - l.l_discount);
}

}  // namespace tpch
