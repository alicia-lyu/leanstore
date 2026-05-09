// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §6 (comparison-integrity rules)    — read before changing join strategy
//   §7 (HashJoin downstream)           — dimension-table builds at query time
//
// Template method bodies for Q5Workload<Backend> query methods,
// predicate implementations, Params::defaults(), and agg_row_t::print().
//
// Phase 0.5 status:
//   - Params::defaults()     — real body (ASIA / 1994-01-01).
//   - query_by_*             — all four stubbed: out.clear(); return 0.
//   - q5_predicate_*         — stubbed to return true (real bodies Phase 4).
//   - q5_agg_row_t::print()  — real body.
//   - q5_pipeline_view_t::print() — real body.

#pragma once

#include <ostream>

namespace tpch::q5
{

// ---------------------------------------------------------------------------
// q5_pipeline_view_t out-of-line method bodies.

inline void q5_pipeline_view_t::print(std::ostream& os) const
{
   // TODO Phase 1: implement real print body.
   os << "q5_view(extprice=" << l_extendedprice
      << ",disc=" << l_discount
      << ",suppkey=" << l_suppkey
      << ",nationkey=" << c_nationkey
      << ",orderdate=" << o_orderdate << ")";
}

// ---------------------------------------------------------------------------
// q5_agg_row_t out-of-line method bodies.
//
// Output format mirrors TPC-H spec §2.4.5 result: n_name | revenue.

inline void q5_agg_row_t::print(std::ostream& os) const
{
   os << n_name << " | " << revenue;
}

// ---------------------------------------------------------------------------
// Params

inline Params Params::defaults()
{
   return Params{
       Varchar<25>("ASIA"),
       DATE_1994_01_01,  // o_orderdate >= 1994-01-01
   };
}

// ---------------------------------------------------------------------------
// Predicate bodies — stubbed to return true for Phase 0.5.
// Real filter logic lands in Phase 4 when query_by_* bodies are implemented.

inline bool q5_predicate_customer(const customerh_t& /*c*/, const Params& /*p*/)
{
   // Phase 4: return nation_set.count(c.c_nationkey) > 0;
   return true;
}

inline bool q5_predicate_orders(const orders_t& /*o*/, const Params& /*p*/)
{
   // Phase 4: return o.o_orderdate >= p.orderdate_lo
   //                && o.o_orderdate <  p.orderdate_lo + ONE_YEAR_DAYS;
   return true;
}

inline bool q5_predicate_lineitem(const lineitem_t& /*l*/, const Params& /*p*/)
{
   // Q5 has no per-lineitem single-table filter before the SUPPLIER probe.
   // The cross-equality (c_nationkey = s_nationkey) fires after the probe.
   // This predicate exists for symmetry with Q3's shape.
   return true;
}

// ---------------------------------------------------------------------------
// Query bodies — all four stubbed for Phase 0.5.
//
// Each returns 0 admitted rows (empty result vector).  The XOR digest of an
// empty vector is 0x0; the test harness reports [OK] parity vacuously since
// all four paths agree on 0.

template <typename Backend>
long Q5Workload<Backend>::query_by_base(std::vector<q5_agg_row_t>& out)
{
   // Phase 4 §7.2: 2-way BMJ chain over COL split indexes
   // (customer ⋈ orders_sec ⋈ lineitem_sec), same visitor as S3.
   out.clear();
   return 0;
}

template <typename Backend>
long Q5Workload<Backend>::query_by_view(std::vector<q5_agg_row_t>& out)
{
   // Phase 4 §7.3: sequential scan of q5_pipeline_view_t; per-nation
   // HashAggregate over post-filter rows.
   out.clear();
   return 0;
}

template <typename Backend>
long Q5Workload<Backend>::query_by_merged(std::vector<q5_agg_row_t>& out)
{
   // Phase 4 §7.1: col_group_walk over MI[COL] with a Q5-specific Visitor;
   // SUPPLIER + NATION + REGION side hash builds pre-walk.
   out.clear();
   return 0;
}

template <typename Backend>
long Q5Workload<Backend>::query_by_hash(std::vector<q5_agg_row_t>& out)
{
   // Phase 4 §7.5: HashJoin chain over base tables (S4 baseline).
   out.clear();
   return 0;
}

}  // namespace tpch::q5
