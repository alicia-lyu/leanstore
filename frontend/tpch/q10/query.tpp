// Template method bodies for Q10Workload<Backend> query methods,
// Params::defaults(), q10_pipeline_view_t::print(), q10_agg_row_t::print(),
// and predicate implementations.
// Operator-translation reference: see ../OPERATORS.md §3.
//
// Phase 1 commit 1: all query_by_* return empty results (XOR digest = 0x0).
// Real bodies land in Phase 4 §7.1 (S3), §7.2 (S1), §7.3 (S2), §7.5 (S4).

#pragma once

#include <gflags/gflags.h>
#include <ostream>
#include <vector>

#include "../tpch_tables.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q10
{

// ---------------------------------------------------------------------------
// Date constants for the 3-month orderdate window.
//
// TPC-H Q10 substitution-parameter domain: :d ∈ first day of a month between
// 1993-02-01 and 1995-01-01 (24 valid month starts inclusive). DATE constants
// are days since 1970-01-01. DATE_1994_01_01 = 8766 and DATE_1995_01_01 = 9131
// ship in tpch_tables.hpp. Validation default = 1993-10-01.
//
// Phase 1 commit 1 ships only the validation default; commit 3 populates the
// 24-entry PARAM_TABLE (Decision E4).

// Days since 1970-01-01.  1993-01-01 = DATE_1994_01_01 − 365.
static constexpr Timestamp Q10_DATE_1993_01_01 = DATE_1994_01_01 - 365;
//   1993-10-01 = 1993-01-01 + (31+28+31+30+31+30+31+31+30) = 1993-01-01 + 273
static constexpr Timestamp Q10_DATE_1993_10_01 = Q10_DATE_1993_01_01 + 273;

// ---------------------------------------------------------------------------

inline Params Params::defaults()
{
   return Params{Q10_DATE_1993_10_01};
}

// Phase 1 commit 1: pin to defaults (no rotation yet).
// Commit 3 will populate PARAM_TABLE[24] and rotate
// `params.date_lo = PARAM_TABLE[iter % 24]`.
template <typename Backend>
void Q10Workload<Backend>::set_params_for_iter(long /*iter*/)
{
   params = Params::defaults();
}

// ---------------------------------------------------------------------------
// Predicate implementations.

inline bool q10_predicate_orders(const orders_t& o, const Params& p)
{
   // o_orderdate ∈ [date_lo, date_lo + 3 months).
   // 3 months ≈ 90 days for the spec's monthly grid; consistent with how
   // Q3 / Q5 lower their date-window predicates.
   return o.o_orderdate >= p.date_lo
       && o.o_orderdate <  p.date_lo + 90;
}

inline bool q10_predicate_lineitem(const lineitem_t& l, const Params& /*p*/)
{
   // l_returnflag = 'R' (spec-hardcoded; kept live for view reusability).
   return l.l_returnflag.data[0] == 'R';
}

// ---------------------------------------------------------------------------
// Output formatting.

inline void q10_pipeline_view_t::print(std::ostream& os) const
{
   os << l_extendedprice << '\t' << l_discount << '\t'
      << l_returnflag << '\t' << o_orderdate << '\n';
}

inline void q10_agg_row_t::print(std::ostream& os) const
{
   os << c_custkey << '\t' << revenue << '\n';
}

// ---------------------------------------------------------------------------
// Stub query bodies — Phase 1 commit 1.
// Real bodies land in Phase 4 §7.1/§7.2/§7.3/§7.5.

template <typename Backend>
long Q10Workload<Backend>::query_by_base(std::vector<q10_agg_row_t>& out)
{
   (void)out;
   return 0;
}

template <typename Backend>
long Q10Workload<Backend>::query_by_view(std::vector<q10_agg_row_t>& out)
{
   (void)out;
   return 0;
}

template <typename Backend>
long Q10Workload<Backend>::query_by_merged(std::vector<q10_agg_row_t>& out)
{
   (void)out;
   return 0;
}

template <typename Backend>
long Q10Workload<Backend>::query_by_hash(std::vector<q10_agg_row_t>& out)
{
   (void)out;
   return 0;
}

}  // namespace tpch::q10
