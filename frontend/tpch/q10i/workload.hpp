#pragma once

// Q10IWorkload<Backend>: Q10 extended with invoice return-payment split.
// Operator-translation reference: see ../OPERATORS.md
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join    -> query_by_base
//   2 = intermediate pipeline view          -> query_by_view
//   3 = MI[COLI] (group-walk)               -> query_by_merged
//   4 = traditional indexes + hash join     -> query_by_hash

#include <vector>

#include "../backend.hpp"
#include "../tpchi_family/coli_pipeline.hpp"
#include "../tpchi_family/tpchi_workload.hpp"
#include "views.hpp"

namespace tpch::q10i
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.10, shared with Q10).
// Domain: :d ∈ first day of a month between 1993-02-01 and 1995-01-01.
// Validation default: 1993-10-01. Identical to q10::Params (single param,
// same domain) — copied rather than aliased to keep namespaces clean.
struct Params {
   Timestamp date_lo;  // inclusive lower bound on o_orderdate

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to an orders row: orderdate in [date_lo, date_lo + 3mo).
bool q10i_predicate_orders(const orders_t& o, const Params& p);

// Applied to a lineitem row: l_returnflag = 'R'.
// (Spec-hardcoded; kept live — not baked into any secondary — so the view
// remains reusable across a hypothetical no-returnflag Q10I variant.)
bool q10i_predicate_lineitem(const lineitem_coli_t& l, const Params& p);

// ---------------------------------------------------------------------------
// Cardinality and path counters. Populated by Phase 4 query bodies; Phase 1
// leaves them at zero. Declared at namespace scope so test harnesses and
// executables can attach a Stats instance before calling query_by_*.

struct Q10IStats {
   long customers_scanned              = 0;
   long orders_scanned                 = 0;
   long orders_passing_date            = 0;
   long lineitems_scanned              = 0;
   long lineitems_passing_returnflag   = 0;
   long invoices_scanned               = 0;
   long aggregator_rows_out            = 0;  // surviving customer aggregates
   long topn_offers                    = 0;  // TopNSink::offer calls
   long topn_evictions                 = 0;  // bounded-heap evictions
   long mi_records_visited             = 0;
   long mi_groups_skipped              = 0;
   long view_rows_scanned              = 0;
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q10IWorkload
{
   TPCHIWorkload<Backend::template Adapter>& tpch;

   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_i_t>& lineitem;
   typename Backend::template Adapter<invoice_t>&    invoice;
   typename Backend::template Adapter<nation_t>&     nation;

   // Track 2 COLI 4-table pipeline (NOT the COL 3-table pipeline).
   CustomerOrdersLineitemInvoicePipeline<Backend> coli;

   typename Backend::template Adapter<q10i_pipeline_view_t>& pipeline_view;

   // S2 variant B: per-order pre-aggregated view (paid/open/late baked, date
   // live). Selected at query time via --q10i_view_variant=preagg.
   typename Backend::template Adapter<q10i_pipeline_view_preagg_t>& pipeline_view_preagg;

   // S5: Q10I aCOLI MI — pre-aggregated 2-type merged index
   // (customer_coli_t + orders_acoli_q10i_t, NO lineitems/invoices; per-order
   // paid/open/late returned revenue baked). The "fair MI" answer to the S2
   // preagg view — co-locates the customer payload once per customer. Walked by
   // the hand-rolled acoli_group_walk.
   typename Backend::template MergedAdapter<customer_coli_t, orders_acoli_q10i_t>& acoli_q10i;

  public:
   Params      params;
   Q10IStats*  stats = nullptr;

   // Expose the COLI pipeline so test harnesses can call populate_split()
   // and populate_merged() directly without routing through load().
   CustomerOrdersLineitemInvoicePipeline<Backend>& coli_pipeline() { return coli; }

   // Pattern B view loaders — exposed so test harnesses can drive them after
   // populate_merged() without going through load().
   void populate_q10i_view();         // S2 variant A (per-lineitem)
   void populate_q10i_view_preagg();  // S2 variant B (per-order, paid/open/late)
   void populate_q10i_acoli();        // S5 aCOLI MI (customer scan + per-order aggs)

   Q10IWorkload(
       TPCHIWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_i_t>& lineitem,
       typename Backend::template Adapter<invoice_t>&    invoice,
       typename Backend::template Adapter<nation_t>&     nation,
       typename Backend::template Adapter<q10i_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_coli_t>& split_lineitem,
       typename Backend::template Adapter<invoice_coli_t>&  split_invoice,
       typename Backend::template MergedAdapter<customer_acoli_t, orders_coli_t,
                                                lineitem_acoli_t>& acoli,
       typename Backend::template Adapter<q10i_pipeline_view_preagg_t>& pipeline_view_preagg,
       typename Backend::template MergedAdapter<customer_coli_t, orders_acoli_q10i_t>& acoli_q10i);

   // Param cycling: rotate through the 24-entry PARAM_TABLE so each TX
   // iteration exercises a different month start (PLAYBOOK §3.6 rationale).
   void set_params_for_iter(long iter);

   // Queries — one per storage structure.
   // Returns the number of result rows (≤ 20 — Q10I has LIMIT 20).
   long query_by_base      (std::vector<q10i_agg_row_t>& out);  // structure 1
   long query_by_view      (std::vector<q10i_agg_row_t>& out);  // structure 2 (A/B dispatch)
   long query_by_view_preagg(std::vector<q10i_agg_row_t>& out); // structure 7 (per-order preagg view)
   long query_by_merged    (std::vector<q10i_agg_row_t>& out);  // structure 3
   long query_by_hash      (std::vector<q10i_agg_row_t>& out);  // structure 4
   long query_by_aggregated(std::vector<q10i_agg_row_t>& out);  // structure 5 (aCOLI)

   void   load();
   double get_size() const;
};

}  // namespace tpch::q10i

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
