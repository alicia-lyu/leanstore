// Template method bodies for Q12Workload<Backend> query methods,
// the four per-structure wrapper query() methods, and the two predicate
// functions. All bodies are TODO stubs. Each comment cites the CLAUDE.md
// section or infrastructure header the implementer should consult.

#pragma once

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to a raw lineitem before joining (structures 1 and 4).
// See: OPERATORS.md §5 Q12 worked example; q12/CLAUDE.md §Substitution Parameters
inline bool q12_predicate_lineitem(const lineitem_t& /* l */, const Params& /* p */)
{
   // TODO(skeleton): Implement:
   //   l_shipmode IN (p.shipmode1, p.shipmode2)
   //   AND l_shipdate < l_commitdate
   //   AND l_commitdate < l_receiptdate
   //   AND l_receiptdate >= p.receiptdate_lo
   //   AND l_receiptdate <  p.receiptdate_hi
   return false;
}

// Applied to a fully-assembled joined_ol_t (structures 2 and 3).
// See: OPERATORS.md §5 Q12 worked example; q12/CLAUDE.md §Substitution Parameters
inline bool q12_predicate_joined(const joined_ol_t& /* j */, const Params& /* p */)
{
   // TODO(skeleton): Same predicate as above, but read fields from
   //   j.line().l_shipmode, j.line().l_shipdate, etc.
   // See: frontend/tpch/views_ol.hpp joined_ol_t::line() / order().
   return false;
}

// ---------------------------------------------------------------------------
// Q12Workload query methods

template <typename Backend>
long Q12Workload<Backend>::query_by_base(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.merge_join_base(lambda) where the lambda
   // applies q12_predicate_joined, accumulates high/low counts per shipmode
   // into a local map, then writes q12_agg_row_t rows into `out`.
   // See: OPERATORS.md §3 op 4 (S1), §5 Q12.
   out.clear();
   return 0;
}

template <typename Backend>
long Q12Workload<Backend>::query_by_view(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Scan pipeline_view adapter; for each joined_ol_t row,
   // apply q12_predicate_joined, accumulate counts, write to `out`.
   // See: OPERATORS.md §3 op 4 (S2).
   out.clear();
   return 0;
}

template <typename Backend>
long Q12Workload<Backend>::query_by_merged(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.scan_merged(lambda) where the lambda applies
   // q12_predicate_joined, accumulates counts per shipmode, writes to `out`.
   // See: OPERATORS.md §3 op 4 (S3), §5 Q12.
   out.clear();
   return 0;
}

template <typename Backend>
long Q12Workload<Backend>::query_by_hash(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.hash_join_base(lambda) where the lambda applies
   // q12_predicate_joined, accumulates counts per shipmode, writes to `out`.
   // See: OPERATORS.md §3 op 4 (S4 baseline), §5 Q12.
   out.clear();
   return 0;
}

// ---------------------------------------------------------------------------
// Per-structure wrapper query() and get_size() methods

template <typename Backend>
long BaseQ12<Backend>::query(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_base(out).
   // See: OPERATORS.md §3 op 4 (S1).
   out.clear();
   return 0;
}

template <typename Backend>
double BaseQ12<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long ViewQ12<Backend>::query(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_view(out).
   // See: OPERATORS.md §3 op 4 (S2).
   out.clear();
   return 0;
}

template <typename Backend>
double ViewQ12<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long MergedQ12<Backend>::query(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_merged(out).
   // See: OPERATORS.md §3 op 4 (S3).
   out.clear();
   return 0;
}

template <typename Backend>
double MergedQ12<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long HashQ12<Backend>::query(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_hash(out).
   // See: OPERATORS.md §3 op 4 (S4).
   out.clear();
   return 0;
}

template <typename Backend>
double HashQ12<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

}  // namespace tpch::q12
