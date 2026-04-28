// Template method bodies for Q3Workload<Backend> query methods,
// the four per-structure wrapper query() methods, and predicate functions.
// All bodies are TODO stubs. Each comment cites the CLAUDE.md section
// or infrastructure header the implementer should consult.

#pragma once

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to raw orders_t before joining (structures 1 and 4).
// See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 1 —
//      filter pushed down immediately after ORDERS scan.
inline bool q3_predicate_orders(const orders_t& /* o */, const Params& /* p */)
{
   // TODO(skeleton): Implement: o.o_orderdate < p.orderdate_hi
   return false;
}

// Applied to raw lineitem_t before joining (structures 1 and 4).
// See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 1 —
//      filter pushed down immediately after LINEITEM scan.
inline bool q3_predicate_lineitem(const lineitem_t& /* l */, const Params& /* p */)
{
   // TODO(skeleton): Implement: l.l_shipdate > p.shipdate_lo
   return false;
}

// Applied to a fully-assembled joined_ol_t (structures 2 and 3).
// See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 3 —
//      filter applied after PremergedJoin using ORDERS||LINEITEM column indices.
inline bool q3_predicate_joined(const joined_ol_t& /* j */, const Params& /* p */)
{
   // TODO(skeleton): Implement using joined_ol_t accessors:
   //   j.order().o_orderdate < p.orderdate_hi
   //   AND j.line().l_shipdate > p.shipdate_lo
   // See: frontend/tpch/views_ol.hpp joined_ol_t::order() / line().
   return false;
}

// ---------------------------------------------------------------------------
// Q3Workload query methods

template <typename Backend>
long Q3Workload<Backend>::query_by_base(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.merge_join_base(lambda) with q3_predicate_orders
   // and q3_predicate_lineitem applied inline. After the OL aggregate, sort by
   // o_custkey and merge-join with the CUSTOMER scan filtered by c_mktsegment.
   // Sort result by (revenue DESC, o_orderdate ASC) and return top 10.
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 1;
   //      §Execution Style: Monolithic vs Cascade.
   out.clear();
   return 0;
}

template <typename Backend>
long Q3Workload<Backend>::query_by_view(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Scan pipeline_view adapter; for each joined_ol_t row,
   // apply q3_predicate_joined, aggregate revenue per (l_orderkey, o_custkey,
   // o_orderdate, o_shippriority). Sort by o_custkey, merge-join with CUSTOMER
   // filtered by c_mktsegment. Sort by (revenue DESC, o_orderdate ASC), top 10.
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 2 & 4.
   out.clear();
   return 0;
}

template <typename Backend>
long Q3Workload<Backend>::query_by_merged(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.scan_merged(lambda) with q3_predicate_joined.
   // Aggregate revenue per (l_orderkey, o_custkey, o_orderdate, o_shippriority)
   // into a local map. Sort by o_custkey, merge-join with CUSTOMER filtered by
   // c_mktsegment. Sort by (revenue DESC, o_orderdate ASC), take top 10.
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 3;
   //      §Execution Style: Monolithic vs Cascade (monolithic post-join sketch).
   out.clear();
   return 0;
}

template <typename Backend>
long Q3Workload<Backend>::query_by_hash(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.hash_join_base(lambda) applying q3_predicate_joined.
   // Build CUSTOMER filtered by c_mktsegment into a hash map (keyed by c_custkey),
   // probe during or after OL join. Aggregate, sort top 10.
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 2 & 4
   //      (hash join variant); §Execution Style: Monolithic vs Cascade.
   out.clear();
   return 0;
}

// ---------------------------------------------------------------------------
// Per-structure wrapper query() and get_size() methods

template <typename Backend>
long BaseQ3<Backend>::query(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_base(out).
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 1.
   out.clear();
   return 0;
}

template <typename Backend>
double BaseQ3<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long ViewQ3<Backend>::query(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_view(out).
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 2 & 4.
   out.clear();
   return 0;
}

template <typename Backend>
double ViewQ3<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long MergedQ3<Backend>::query(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_merged(out).
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 3.
   out.clear();
   return 0;
}

template <typename Backend>
double MergedQ3<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long HashQ3<Backend>::query(std::vector<q3_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_hash(out).
   // See: frontend/tpch/q3/CLAUDE.md §Plan Descriptions, Structure 2 & 4
   //      (hash join variant).
   out.clear();
   return 0;
}

template <typename Backend>
double HashQ3<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

}  // namespace tpch::q3
