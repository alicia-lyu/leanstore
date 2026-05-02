// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Template method bodies for Q3IWorkload<Backend> query methods,
// Params::defaults(), q3i_agg_row_t::print(), and predicate stubs.

#pragma once

#include <ostream>

namespace tpch::q3i
{

// ---------------------------------------------------------------------------
// Params::defaults — Q3I SQL validation values.

inline Params Params::defaults()
{
   // TODO: populate real values from the Q3I SQL:
   //   mktsegment = "BUILDING"
   //   orderdate  = DATE '1995-03-15' (days since epoch)
   //   shipdate   = DATE '1995-03-15' (same threshold, different comparison)
   //   threshold  = 0  (minimum cust_open_due)
   return Params{};
}

// ---------------------------------------------------------------------------
// q3i_agg_row_t::print — tab-separated output.

inline void q3i_agg_row_t::print(std::ostream& os) const
{
   // TODO: write o_orderkey \t revenue \t o_orderdate \t o_shippriority \t cust_open_due \n
   os << "TODO\n";
}

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to an orders row before joining.
inline bool q3i_predicate_orders(const orders_t&, const Params&)
{
   return false;  // TODO: o.o_orderdate < p.orderdate
}

// Applied to a raw lineitem before joining.
inline bool q3i_predicate_lineitem(const lineitem_t&, const Params&)
{
   return false;  // TODO: l.l_shipdate > p.shipdate
}

// Applied to a fully-assembled joined_ol_t.
inline bool q3i_predicate_joined(const joined_ol_t& j, const Params& p)
{
   return q3i_predicate_orders(j.order(), p) && q3i_predicate_lineitem(j.line(), p);
}

// Applied to an invoice row for the open-due sub-aggregate.
inline bool q3i_predicate_invoice(const invoice_t&)
{
   return false;  // TODO: i.i_status == 'O'
}

// ---------------------------------------------------------------------------
// Q3IWorkload query methods

template <typename Backend>
long Q3IWorkload<Backend>::query_by_base(std::vector<q3i_agg_row_t>& out)
{
   // S1: BinaryMergeJoin over base ORDERS and LINEITEM scanners.
   // Pre-join: hash-scan CUSTOMER filtering by c_mktsegment.
   // Pre-join: scan INVOICE grouping by i_custkey with i_status='O' to build
   //           cust_open_due map; apply threshold filter.
   // Post-join: accumulate revenue = SUM(l_extendedprice * (1-l_discount)).
   // Post-aggregate: ORDER BY revenue DESC LIMIT 10.
   // OPERATORS.md §3 op 4 (S1).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_view(std::vector<q3i_agg_row_t>& out)
{
   // S2: scan materialized pipeline view (unfiltered joined_ol_t rows).
   // Apply q3i_predicate_joined post-scan; look up cust_open_due from a
   // pre-built invoice map.
   // Post-aggregate: ORDER BY revenue DESC LIMIT 10.
   // OPERATORS.md §3 op 4 (S2).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_merged(std::vector<q3i_agg_row_t>& out)
{
   // S3: PremergedJoin over the 4-table COLI MI.
   //
   // Key design (§3.1.2 sibling + §3.1.3 hierarchical hybrid):
   //   One COLI scan per custkey group visits:
   //     customer_coli_t — c_mktsegment filter gate
   //     orders_coli_t   — o_orderdate predicate; contributes orderkey/shippriority
   //     lineitem_coli_t — l_shipdate predicate; revenue accumulation
   //     invoice_coli_t  — i_status='O'; cust_open_due accumulation (§3.1.2 sibling)
   //   The cust_open_due is computed within the same COLI pass — no separate
   //   invoice scan needed (unlike S1/S2).
   // Post-aggregate: ORDER BY revenue DESC LIMIT 10.
   // OPERATORS.md §3 op 4 (S3).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_hash(std::vector<q3i_agg_row_t>& out)
{
   // S4: HashJoin baseline. Build side = CUSTOMER (filtered by mktsegment);
   // probe side = filtered LINEITEM joined with ORDERS.
   // Pre-join: build cust_open_due map from INVOICE.
   // Post-aggregate: ORDER BY revenue DESC LIMIT 10.
   // OPERATORS.md §3 op 4 (S4 baseline).
   out.clear();
   return 0;  // TODO
}

}  // namespace tpch::q3i
