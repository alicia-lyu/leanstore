// Template method bodies for Q5IWorkload<Backend> ctor / load / get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>

DECLARE_int32(storage_structure);
DECLARE_int32(load_only_structure);

namespace tpch::q5i
{

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
   if (load_all || only == 2) {
      // Pattern B (PLAYBOOK §3.6): view loader reuses S3 group-walk
      // (Phase 4a) with parameterised filters dropped. Hand-rolling
      // a 4-way join here would duplicate query_by_merged.
   }
   if (load_all || only == 3) coli.populate_merged();
   // S4 needs no secondary.
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
