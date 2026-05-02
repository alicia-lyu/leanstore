// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §5 (Q12 worked example)       — inline sketches applicable to Q12I
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Template method bodies for Q12IWorkload<Backend> query methods,
// Params::defaults(), q12i_agg_row_t::print(), and predicate stubs.

#pragma once

#include <ostream>

namespace tpch::q12i
{

// ---------------------------------------------------------------------------
// Params::defaults — Q12I SQL validation values.

inline Params Params::defaults()
{
   // TODO: populate real values from the Q12I SQL:
   //   shipmode1     = "MAIL"         (same as Q12)
   //   shipmode2     = "SHIP"         (same as Q12)
   //   receiptdate_lo = DATE_1994_01_01
   //   receiptdate_hi = DATE_1995_01_01
   return Params{};
}

// ---------------------------------------------------------------------------
// q12i_agg_row_t::print — tab-separated output.

inline void q12i_agg_row_t::print(std::ostream& os) const
{
   // TODO: write l_shipmode \t hi \t lo \t late_invoiced \n
   os << "TODO\n";
}

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to a raw lineitem before joining (structures 1 and 4).
// Encodes the same five Q12 date/shipmode conditions; the i_status='L'
// aggregate is computed inside the join callback, not here.
inline bool q12i_predicate_lineitem(const lineitem_t&, const Params&)
{
   return false;  // TODO: same logic as q12_predicate_lineitem + params
}

// Applied to a fully-assembled joined_ol_t (structures 2 and 3).
inline bool q12i_predicate_joined(const joined_ol_t& j, const Params& p)
{
   return q12i_predicate_lineitem(j.line(), p);
}

// ---------------------------------------------------------------------------
// Q12IWorkload query methods

template <typename Backend>
long Q12IWorkload<Backend>::query_by_base(std::vector<q12i_agg_row_t>& out)
{
   // S1: BinaryMergeJoin over base ORDERS and LINEITEM scanners.
   // After join, probe INVOICE by l_invoicekey for i_status='L' aggregate.
   // OPERATORS.md §3 op 4 (S1), §5 Q12.
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q12IWorkload<Backend>::query_by_view(std::vector<q12i_agg_row_t>& out)
{
   // S2: scan materialized pipeline view (unfiltered joined_ol_t rows).
   // Apply q12i_predicate_joined post-scan; probe INVOICE for i_status='L'.
   // OPERATORS.md §3 op 4 (S2).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q12IWorkload<Backend>::query_by_merged(std::vector<q12i_agg_row_t>& out)
{
   // S3: PremergedJoin over the 4-table COLI MI.
   //
   // Key design (§3.1.2 sibling + §3.1.3 hierarchical hybrid):
   //   - One COLI scan visits all four record variants per custkey group:
   //     customer_coli_t  → filter context (optional; custkey already in key)
   //     orders_coli_t    → o_orderpriority for hi/lo accumulation
   //     lineitem_coli_t  → shipmode/date predicates
   //     invoice_coli_t   → i_status='L' for late_invoiced accumulation
   //   - Orders and lineitems form a §3.1.3 hierarchy (orderkey nesting).
   //   - Invoices are §3.1.2 siblings of orders within the same custkey.
   //
   // OPERATORS.md §3 op 4 (S3).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q12IWorkload<Backend>::query_by_hash(std::vector<q12i_agg_row_t>& out)
{
   // S4: HashJoin baseline. Build side = ORDERS; probe = filtered LINEITEM.
   // After join, probe INVOICE by l_invoicekey for i_status='L'.
   // OPERATORS.md §3 op 4 (S4 baseline), §5 Q12.
   out.clear();
   return 0;  // TODO
}

}  // namespace tpch::q12i
