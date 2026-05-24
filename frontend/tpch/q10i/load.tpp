// Template method bodies for Q10IWorkload<Backend> ctor / load / get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>

DECLARE_int32(storage_structure);

namespace tpch::q10i
{

template <typename Backend>
Q10IWorkload<Backend>::Q10IWorkload(
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
                                             lineitem_acoli_t>& acoli)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      nation(nation),
      coli(customer, orders, lineitem, invoice, merged_coli,
           split_orders, split_lineitem, split_invoice, acoli),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

// Populate every secondary unconditionally so a single DB image can serve
// all four --storage_structure query-time variants (PLAYBOOK §3.6 PITFALL —
// `load()` populating only one secondary).
//
// S2 view loader is deferred to Phase 4a (Pattern B, PLAYBOOK §3.6): the
// view will reuse the S3 coli_group_walk visitor with parameterised filters
// dropped and a view-insert sink. Hand-rolling the merge here would duplicate
// the eventual query_by_merged body.
template <typename Backend>
void Q10IWorkload<Backend>::load()
{
   tpch.load();
   coli.populate_split();    // S1: custkey-sorted split indexes
   // S2: populate_q10i_view — Phase 4a (Pattern B, PLAYBOOK §3.6).
   //   View loader will reuse Q10IGroupWalkVisitor (Phase 4 §7.1) with
   //   parameterised filters dropped and a view-insert sink.
   coli.populate_merged();   // S3: COLI merged index
   // S4: base tables only — nothing to populate.
}

template <typename Backend>
double Q10IWorkload<Backend>::get_size() const
{
   double base = customer.size() + orders.size() + lineitem.size()
               + invoice.size() + nation.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + coli.get_split_size();
      case 2: return base + pipeline_view.size();
      case 3: return base + coli.get_merged_size();
      case 4: return base;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q10i
