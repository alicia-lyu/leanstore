// Template method bodies for Q12Workload<Backend> query methods,
// Params::defaults(), q12_agg_row_t::print(), and predicate stubs.
//
// Per-structure wrappers (BaseQ12 / ViewQ12 / ...) are aliases to the shared
// tpch::BaseStructure / ViewStructure / ... templates defined in
// frontend/tpch/per_structure_workload.hpp, whose method bodies forward
// directly to w.query_by_*() and w.get_size() — no extra definitions needed.
//
// query_by_* bodies remain TODO stubs (separate plan).

#pragma once

#include <ostream>

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// Params::defaults — TPC-H §2.4.12 validation values.

inline Params Params::defaults()
{
   return {
       Varchar<10>("MAIL"),
       Varchar<10>("SHIP"),
       DATE_1994_01_01,
       DATE_1995_01_01,
   };
}

// ---------------------------------------------------------------------------
// q12_agg_row_t::print — tab-separated: shipmode, high_line_count, low_line_count.

inline void q12_agg_row_t::print(std::ostream& os) const
{
   os << std::string_view(l_shipmode.data, l_shipmode.length) << "\t"
      << high_line_count << "\t"
      << low_line_count << "\n";
}

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to a raw lineitem before joining (structures 1 and 4).
// See: OPERATORS.md §5 Q12; q12/CLAUDE.md §Q12-Specific Operator Configurations.
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
// Same predicate as above expressed over joined_ol_t fields.
// See: frontend/tpch/views_ol.hpp joined_ol_t::line() / order().
inline bool q12_predicate_joined(const joined_ol_t& /* j */, const Params& /* p */)
{
   // TODO(skeleton): read fields from j.line().l_shipmode, j.line().l_shipdate, etc.
   return false;
}

// ---------------------------------------------------------------------------
// Q12Workload query methods

template <typename Backend>
long Q12Workload<Backend>::query_by_base(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Drive BinaryMergeJoin over base tables; apply
   // q12_predicate_lineitem per lineitem before joining, accumulate
   // high/low counts per shipmode, write q12_agg_row_t rows into `out`.
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
   // TODO(skeleton): Drive PremergedJoin over MI[0]; apply q12_predicate_joined
   // per row, accumulate counts per shipmode, write to `out`.
   // See: OPERATORS.md §3 op 4 (S3), §5 Q12.
   out.clear();
   return 0;
}

template <typename Backend>
long Q12Workload<Backend>::query_by_hash(std::vector<q12_agg_row_t>& out)
{
   // TODO(skeleton): Drive HashJoin over base tables; apply q12_predicate_joined,
   // accumulate counts per shipmode, write to `out`.
   // See: OPERATORS.md §3 op 4 (S4 baseline), §5 Q12.
   out.clear();
   return 0;
}

}  // namespace tpch::q12
