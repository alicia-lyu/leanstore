// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Template method bodies for Q9IWorkload<Backend> query methods,
// Params::defaults(), q9i_agg_row_t::print(), and predicate stubs.

#pragma once

#include <ostream>

namespace tpch::q9i
{

// ---------------------------------------------------------------------------
// Params::defaults — Q9I SQL validation values.

inline Params Params::defaults()
{
   // TODO: populate real values from the Q9I SQL:
   //   color = "green"  (used as p_name LIKE '%green%')
   return Params{};
}

// ---------------------------------------------------------------------------
// q9i_agg_row_t::print — tab-separated output.

inline void q9i_agg_row_t::print(std::ostream& os) const
{
   // TODO: write nation \t o_year \t profit \t paid_profit \n
   os << "TODO\n";
}

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to a part row: p_name LIKE '%color%'.
inline bool q9i_predicate_part(const part_t&, const Params&)
{
   return false;  // TODO: substring search on p.p_name for params.color
}

// Applied to an invoice row: i_status = 'P' (for paid-profit case expression).
inline bool q9i_predicate_invoice(const invoice_t&)
{
   return false;  // TODO: i.i_status == 'P'
}

// ---------------------------------------------------------------------------
// Q9IWorkload query methods

template <typename Backend>
long Q9IWorkload<Backend>::query_by_base(std::vector<q9i_agg_row_t>& out)
{
   // S1: BinaryMergeJoin over base ORDERS and LINEITEM scanners.
   // Pre-join: build in-memory hashmaps for nation (nationkey→name),
   //           supplier (suppkey→nationkey), part (partkey — LIKE filter),
   //           partsupp (partkey+suppkey→ps_supplycost).
   // Pre-join: scan INVOICE, filter i_status='P', accumulate paid amounts
   //           keyed by l_invoicekey (or orderkey — to be determined).
   // Post-join callback: compute amount = l_extendedprice*(1-l_discount) -
   //           ps_supplycost*l_quantity; accumulate into (nation, year) groups.
   // Post-aggregate: ORDER BY nation ASC, o_year DESC.
   // OPERATORS.md §3 op 4 (S1).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q9IWorkload<Backend>::query_by_view(std::vector<q9i_agg_row_t>& out)
{
   // S2: scan materialized pipeline view (unfiltered joined_ol_t rows).
   // Pre-scan: build same hashmaps as S1.
   // Apply nation/part/supplier/partsupp lookups post-scan; accumulate
   // profit and paid_profit.
   // Post-aggregate: ORDER BY nation ASC, o_year DESC.
   // OPERATORS.md §3 op 4 (S2).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q9IWorkload<Backend>::query_by_merged(std::vector<q9i_agg_row_t>& out)
{
   // S3: PremergedJoin over the 4-table COLI MI.
   //
   // Key design (§3.1.2 sibling + §3.1.3 hierarchical hybrid):
   //   One COLI scan per custkey group visits:
   //     customer_coli_t — custkey context (no direct Q9I filter, but gating)
   //     orders_coli_t   — o_orderdate → extract o_year
   //     lineitem_coli_t — revenue component; partkey/suppkey for lookup
   //     invoice_coli_t  — i_status='P' → paid_profit case (§3.1.2 sibling)
   //   Nation/supplier/part/partsupp lookups still require pre-built hashmaps
   //   (these tables are not in the COLI MI; they are small and fit in memory).
   //   The COLI MI eliminates the separate invoice scan needed in S1/S4.
   // Post-aggregate: ORDER BY nation ASC, o_year DESC.
   // OPERATORS.md §3 op 4 (S3).
   out.clear();
   return 0;  // TODO
}

template <typename Backend>
long Q9IWorkload<Backend>::query_by_hash(std::vector<q9i_agg_row_t>& out)
{
   // S4: HashJoin baseline. Build side = ORDERS; probe side = LINEITEM.
   // Pre-join: build in-memory hashmaps for nation, supplier, part,
   //           partsupp (same as S1).
   // Pre-join: build invoice paid-amount map.
   // Post-aggregate: ORDER BY nation ASC, o_year DESC.
   // OPERATORS.md §3 op 4 (S4 baseline).
   out.clear();
   return 0;  // TODO
}

}  // namespace tpch::q9i
