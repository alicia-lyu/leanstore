#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Q9IWorkload<Backend>: Q9 extended with a paid-profit aggregate from INVOICE.
// Q9 joins 6 tables (part, supplier, lineitem, partsupp, orders, nation) and
// groups by (nation, year).  Q9I adds INVOICE via the COLI MI so that
// paid_profit = SUM(amount) WHERE i_status='P' can be computed in the same pass.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join    -> query_by_base
//   2 = intermediate pipeline view          -> query_by_view
//   3 = MI[COLI] only (PremergedJoin)       -> query_by_merged
//   4 = traditional indexes + hash join     -> query_by_hash

#include <vector>

#include "../backend.hpp"
#include "../coli_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q9i
{

// ---------------------------------------------------------------------------
// Substitution parameters (Q9I SQL).
// Default: COLOR=%green% (p_name LIKE '%green%').

struct Params {
   Varchar<10> color;  // default "green" — used as LIKE '%color%' on p_name

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to a part row: p_name LIKE '%color%'.
bool q9i_predicate_part(const part_t& p, const Params& params);

// Applied to an invoice row: i_status = 'P' (for paid-profit case expression).
bool q9i_predicate_invoice(const invoice_t& i);

// ---------------------------------------------------------------------------
// Optional per-query intermediate cardinality counters.

struct Q9IStats {
   long parts_scanned       = 0;
   long lineitems_scanned   = 0;
   long invoices_scanned    = 0;
   long join_callbacks      = 0;
   long aggregator_rows_out = 0;

   void reset() { *this = Q9IStats{}; }
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q9IWorkload
{
   // The full TPC-H base-table workload (owns part, supplier, etc.).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Direct references to the base adapters needed at query time.
   // Q9 requires all six non-region tables; Q9I adds invoice.
   typename Backend::template Adapter<nation_t>&    nation;
   typename Backend::template Adapter<supplier_t>&  supplier;
   typename Backend::template Adapter<part_t>&      part;
   typename Backend::template Adapter<partsupp_t>&  partsupp;
   typename Backend::template Adapter<orders_t>&    orders;
   typename Backend::template Adapter<lineitem_t>&  lineitem;
   typename Backend::template Adapter<invoice_t>&   invoice;

   // COLI 4-table pipeline: owns MI(customer_coli_t, orders_coli_t,
   // lineitem_coli_t, invoice_coli_t).
   CustomerOrdersLineitemInvoicePipeline<Backend> coli;

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   typename Backend::template Adapter<q9i_pipeline_view_t>& pipeline_view;

  public:
   Params params;
   Q9IStats* stats = nullptr;

   Q9IWorkload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>& customer,
       typename Backend::template Adapter<nation_t>& nation,
       typename Backend::template Adapter<supplier_t>& supplier,
       typename Backend::template Adapter<part_t>& part,
       typename Backend::template Adapter<partsupp_t>& partsupp,
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template Adapter<invoice_t>& invoice,
       typename Backend::template Adapter<q9i_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q9i_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q9i_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q9i_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q9i_agg_row_t>& out);  // structure 4

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   void   load();
   double get_size() const;
};

}  // namespace tpch::q9i

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
