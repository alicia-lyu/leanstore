#pragma once

// Operator-translation reference: see ../OPERATORS.md
//
// Q10Workload<Backend>: Returned Item Reporting — per-customer top-20 over
// returned lineitems within a 3-month order window. Composes the COL
// pipeline (CUSTOMER × ORDERS × LINEITEM merged index) verbatim from Q3/Q5;
// NATION attaches as a per-customer INL on PK at emit time (Decision D6),
// not as a stored secondary.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + binary merge join  -> query_by_base
//   2 = intermediate pipeline view               -> query_by_view
//   3 = MI[COL] only (col_group_walk)            -> query_by_merged
//   4 = traditional indexes + hash join          -> query_by_hash
//
// S5 is omitted (Decision D8) — Q10 has no parameter-independent aggregate
// to bake; orderdate window is parameterised.

#include <vector>

#include "../backend.hpp"
#include "../tpch_family/col_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q10
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.10).
// Domain: :d ∈ first day of a month between 1993-02-01 and 1995-01-01.
// Validation default: 1993-10-01.
//
// The query selects orders where:
//   o_orderdate >= date_lo  AND  o_orderdate < date_lo + INTERVAL '3' MONTH

struct Params {
   Timestamp date_lo;  // inclusive lower bound on o_orderdate

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to an orders row: orderdate in [date_lo, date_lo + 3mo).
bool q10_predicate_orders(const orders_t& o, const Params& p);

// Applied to a lineitem row: l_returnflag = 'R'.
// (Spec-hardcoded; kept live — not baked into any secondary — so the view
// remains reusable across a hypothetical no-returnflag variant.)
bool q10_predicate_lineitem(const lineitem_t& l, const Params& p);

// ---------------------------------------------------------------------------
// Cardinality and path counters. Populated by Phase 4 query bodies; Phase 1
// leaves them at zero. Declared at namespace scope so test harnesses and
// executables can attach a Stats instance before calling query_by_*.

struct Q10Stats {
   long customers_scanned             = 0;
   long orders_scanned                = 0;
   long orders_passing_date           = 0;
   long lineitems_scanned             = 0;
   long lineitems_passing_returnflag  = 0;
   long aggregator_rows_out           = 0;  // surviving customer aggregates
   long topn_offers                   = 0;  // TopNSink::offer calls (Phase 4 §7.1)
   long topn_evictions                = 0;  // bounded-heap evictions
   long mi_records_visited            = 0;
   long mi_groups_skipped             = 0;
   long view_rows_scanned             = 0;
   long nation_inl_lookups            = 0;  // per-customer NATION PK probes (D6)
   long customer_inl_lookups          = 0;  // S4 record-assembly recoveries
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q10Workload
{
   // The full TPC-H base-table workload (owns all 8 base tables).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Direct references to the base adapters needed at query time.
   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_t>&   lineitem;
   typename Backend::template Adapter<nation_t>&     nation;

   // COL 3-table pipeline: owns MI(customer_coli_t, orders_coli_t,
   // lineitem_col_t). Reused verbatim from Q3 / Q5 — no Q10-specific
   // modifications.
   CustomerOrdersLineitemPipeline<Backend> col;

   // Structure 2: intermediate pipeline view (per-lineitem rows, unfiltered).
   typename Backend::template Adapter<q10_pipeline_view_t>& pipeline_view;

  public:
   Params    params;
   Q10Stats* stats = nullptr;

   // Expose the COL pipeline so test harnesses can call populate_split()
   // and populate_merged() directly without routing through load()'s switch.
   CustomerOrdersLineitemPipeline<Backend>& col_pipeline() { return col; }

   Q10Workload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_t>&   lineitem,
       typename Backend::template Adapter<nation_t>&     nation,
       typename Backend::template Adapter<q10_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_col_t>& merged_col,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_col_t>& split_lineitem);

   // Param cycling: Phase 1 commit 1 always uses defaults.
   // Commit 3 populates the 24-entry PARAM_TABLE (every valid month start in
   // [1993-02-01, 1995-01-01]) so the executable exercises domain-full
   // rotation per TX iteration (PLAYBOOK §3.6 rationale: avoids the
   // fixed-param trap that hid Q3I's pre_revenue bug for weeks).
   void set_params_for_iter(long iter);

   // Queries — one per storage structure.
   // Returns the number of result rows (≤ 20 — TPC-H Q10 has LIMIT 20).
   long query_by_base  (std::vector<q10_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q10_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q10_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q10_agg_row_t>& out);  // structure 4

   void   load();
   double get_size() const;
};

}  // namespace tpch::q10

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
