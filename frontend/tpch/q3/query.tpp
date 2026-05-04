// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §3 op 4 (load vs query)            — keep query-time joins on shared JoinState
//   §3 op 6 (SortedAggregate)          — Q3's LineitemRevenueAccumulator wrapper
//   §5 (Q3 worked example)             — inline sketch for query_by_merged
//   §6 (comparison-integrity rules)    — read before changing join strategy
//   §7 (HashJoin downstream)           — CUSTOMER join uses HashJoin, not MergeJoin
//
// Template method bodies for Q3Workload<Backend> query methods,
// predicate implementations, Params::defaults(), and set_params_for_iter().
// query_by_* bodies are Phase-0.5 stubs — they return empty vectors.

#pragma once

#include "../q3_family/params.hpp"
#include "../q3_family/predicates.hpp"

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// Params

inline Params Params::defaults()
{
   return Params{
       Varchar<10>("BUILDING"),
       DATE_1995_03_15,  // orderdate upper bound
       DATE_1995_03_15,  // shipdate lower bound
   };
}

template <typename Backend>
void Q3Workload<Backend>::set_params_for_iter(long iter)
{
   const auto& e =
       q3_family::PARAM_TABLE[iter % q3_family::PARAM_TABLE_SIZE];
   params.mktsegment = Varchar<10>(e.segment);
   params.orderdate  = e.date;
   params.shipdate   = e.date;
}

// ---------------------------------------------------------------------------
// Predicate implementations — delegate to q3_family shared filters.

// c_mktsegment == params.mktsegment
inline bool q3_predicate_customer(const customerh_t& c, const Params& p)
{
   return q3_family::q3_predicate_customer(c, p);
}

// o_orderdate < params.orderdate
inline bool q3_predicate_orders(const orders_t& o, const Params& p)
{
   return q3_family::q3_predicate_orders(o, p);
}

// l_shipdate > params.shipdate
inline bool q3_predicate_lineitem(const lineitem_t& l, const Params& p)
{
   return q3_family::q3_predicate_lineitem(l, p);
}

// Applied to a pipeline view row: checks mktsegment + orderdate + shipdate.
// All three are parameterised and were NOT baked at load time (predicate
// hoisting), so all three must be applied live here.
inline bool q3_predicate_view(const q3_pipeline_view_t& v, const Params& p)
{
   return v.c_mktsegment == p.mktsegment
       && v.o_orderdate  <  p.orderdate
       && v.l_shipdate   >  p.shipdate;
}

// ---------------------------------------------------------------------------
// q3_pipeline_view_t printer (Phase 0.5 — informational only)

inline void q3_pipeline_view_t::print(std::ostream& os) const
{
   os << l_extendedprice << "\t" << l_discount << "\t"
      << l_shipdate      << "\t" << c_mktsegment << "\t"
      << o_orderdate     << "\t" << o_shippriority << "\n";
}

// ---------------------------------------------------------------------------
// query_by_* — Phase-0.5 STUBS.
//
// Each stub clears the output vector and returns 0.  Real bodies arrive in
// Phase 1 (query_by_merged) and Phase 2 (S1, S2, S4 baselines).  The test
// harness asserts all four digests agree; stubs trivially agree at 0.

template <typename Backend>
long Q3Workload<Backend>::query_by_base(std::vector<q3_agg_row_t>& out)
{
   // TODO Phase 1: drive col.split_orders / split_lineitem BMJ chain with
   // the shared Q3FamilyVisitor; apply q3_predicate_{customer,orders,lineitem}.
   out.clear();
   return 0;
}

template <typename Backend>
long Q3Workload<Backend>::query_by_view(std::vector<q3_agg_row_t>& out)
{
   // TODO Phase 1: scan pipeline_view; apply q3_predicate_view live;
   // accumulate revenue per (orderkey, orderdate, shippriority); apply_topN.
   out.clear();
   return 0;
}

template <typename Backend>
long Q3Workload<Backend>::query_by_merged(std::vector<q3_agg_row_t>& out)
{
   // TODO Phase 1: drive col_group_walk over col.merged_adapter() with a
   // Q3FamilyVisitor; accumulate revenue per orderkey; apply_topN.
   out.clear();
   return 0;
}

template <typename Backend>
long Q3Workload<Backend>::query_by_hash(std::vector<q3_agg_row_t>& out)
{
   // TODO Phase 2: HashJoin chain over base tables; build customer / orders
   // hashmaps filtered by mktsegment / orderdate; probe lineitems by shipdate.
   out.clear();
   return 0;
}

}  // namespace tpch::q3
