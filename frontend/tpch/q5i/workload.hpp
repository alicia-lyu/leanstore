#pragma once

// Q5IWorkload<Backend>: Q5 extended with invoice payment-status split.
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

namespace tpch::q5i
{

// Substitution parameters (Q5I SQL).
// Defaults: REGION=ASIA, DATE=1994-01-01.
struct Params {
   Varchar<25> region;       // region name filter (e.g. "ASIA")
   Timestamp   orderdate_lo; // inclusive lower bound on o_orderdate

   static Params defaults();
};

// Predicate declarations. Bodies live in query.tpp.
bool q5i_predicate_orders(const orders_t& o, const Params& p);

// ---------------------------------------------------------------------------

template <typename Backend>
class Q5IWorkload
{
   TPCHIWorkload<Backend::template Adapter>& tpch;

   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_i_t>& lineitem;
   typename Backend::template Adapter<invoice_t>&    invoice;
   typename Backend::template Adapter<supplier_t>&   supplier;
   typename Backend::template Adapter<nation_t>&     nation;
   typename Backend::template Adapter<region_t>&     region_table;

   CustomerOrdersLineitemInvoicePipeline<Backend> coli;

   typename Backend::template Adapter<q5i_pipeline_view_t>& pipeline_view;

  public:
   Params params;

   CustomerOrdersLineitemInvoicePipeline<Backend>& coli_pipeline() { return coli; }

   Q5IWorkload(
       TPCHIWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_i_t>& lineitem,
       typename Backend::template Adapter<invoice_t>&    invoice,
       typename Backend::template Adapter<supplier_t>&   supplier,
       typename Backend::template Adapter<nation_t>&     nation,
       typename Backend::template Adapter<region_t>&     region_table,
       typename Backend::template Adapter<q5i_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_coli_t>& split_lineitem,
       typename Backend::template Adapter<invoice_coli_t>&  split_invoice,
       typename Backend::template MergedAdapter<customer_acoli_t, orders_coli_t,
                                                lineitem_acoli_t>& acoli);

   void set_params_for_iter(long iter);

   long query_by_base  (std::vector<q5i_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q5i_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q5i_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q5i_agg_row_t>& out);  // structure 4

   void   load();
   double get_size() const;
};

}  // namespace tpch::q5i

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
