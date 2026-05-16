// Template method bodies for Q5IWorkload<Backend> ctor / load / get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>

// q5::Params must be complete before q5/side_tables.hpp parses its
// template bodies (see note in visitor.hpp). q5/workload.hpp owns the
// Params definition and pulls in the rest of Q5's heavyweight workload —
// included here only at the Q5I .tpp boundary, not from the visitor.
#include "../q5/workload.hpp"
#include "../q5/side_tables.hpp"
#include "visitor.hpp"

DECLARE_int32(storage_structure);
DECLARE_int32(load_only_structure);

namespace tpch::q5i
{

// Sink wrapper used in ViewLoad mode: forwards the visitor's assembled
// (Key, payload) pair into the q5i_pipeline_view_t adapter.
template <typename ViewAdapter>
struct Q5IViewLoadSink {
   ViewAdapter& adapter;
   long         rows_inserted = 0;

   void emit_view(const q5i_pipeline_view_t::Key& k,
                  const q5i_pipeline_view_t&      v)
   {
      adapter.insert(k, v);
      ++rows_inserted;
   }
};

template <typename Backend>
Q5IWorkload<Backend>::Q5IWorkload(
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
                                             lineitem_acoli_t>& acoli)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      supplier(supplier),
      nation(nation),
      region_table(region_table),
      coli(customer, orders, lineitem, invoice, merged_coli,
           split_orders, split_lineitem, split_invoice, acoli),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q5IWorkload<Backend>::load()
{
   tpch.load();
   const int only     = FLAGS_load_only_structure;
   const bool load_all = (only < 0);
   if (load_all || only == 1) coli.populate_split();
   if (load_all || only == 3) coli.populate_merged();
   // S2 view loader runs after S3 because it consumes the COLI MI via
   // Q5IGroupWalkVisitor in ViewLoad mode (Pattern B, PLAYBOOK §3.6).
   if (load_all || only == 2) populate_q5i_view();
   // S4 needs no secondary.
}

template <typename Backend>
void Q5IWorkload<Backend>::populate_q5i_view()
{
   // Pattern B: reuse the S3 group walker with all parameterised
   // filters dropped (no region, no orderdate window, no supplier
   // semi-join, no nation membership). FD-attached fields kept
   // (c_nationkey, o_orderdate, i_status). One row per base lineitem;
   // reusable across all (region, date) param combinations.
   q5::Q5SideTables empty_sides{};  // no nation_set, no supplier_nation_set
   using ViewAdapterT =
       typename Backend::template Adapter<q5i_pipeline_view_t>;
   Q5IViewLoadSink<ViewAdapterT> sink{pipeline_view};
   Q5IGroupWalkVisitor<q5::Q5SideTables,
                        Q5IViewLoadSink<ViewAdapterT>,
                        Q5IFilterMode::ViewLoad>
       visitor{params, empty_sides, sink, stats};
   coli_group_walk<Backend>(coli.merged_adapter(), visitor);
}

template <typename Backend>
double Q5IWorkload<Backend>::get_size() const
{
   double base = customer.size() + orders.size()
               + lineitem.size() + invoice.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + coli.get_split_size();
      case 2: return base + pipeline_view.size();
      case 3: return base + coli.get_merged_size();
      case 4: return base;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q5i
