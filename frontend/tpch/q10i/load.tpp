// Template method bodies for Q10IWorkload<Backend> ctor / load / get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>
#include <unordered_map>

#include "../tpch_family/walk_action.hpp"
#include "../tpchi_family/coli_pipeline.hpp"

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
template <typename Backend>
void Q10IWorkload<Backend>::load()
{
   tpch.load();
   coli.populate_split();    // S1: custkey-sorted split indexes
   coli.populate_merged();   // S3: COLI merged index
   populate_q10i_view();     // S2: Pattern B view loader
   // S4: base tables only — nothing to populate.
}

// Pattern B view loader (PLAYBOOK §3.6): walks the COLI MI exactly like
// query_by_merged but with all parameterised filters dropped. Emits one
// view row per (custkey, orderkey, invoicekey, linenumber) tuple with
// FD-attached customer payload, o_orderdate, and i_status pre-resolved
// from the invoice_buf. The view is then reusable across every (date_lo)
// param combination.
template <typename Backend>
void Q10IWorkload<Backend>::populate_q10i_view()
{
   struct ViewLoadVisitor {
      typename Backend::template Adapter<q10i_pipeline_view_t>& view;

      // Per-group customer cache (FD-attached on every view row).
      Integer      c_custkey      = 0;
      Varchar<25>  c_name         = {};
      Varchar<40>  c_address      = {};
      Integer      c_nationkey    = 0;
      Varchar<15>  c_phone        = {};
      Numeric      c_acctbal      = 0;
      Varchar<117> c_comment      = {};

      // Per-order o_orderdate cache (FD-attached on every lineitem row).
      Timestamp    o_orderdate    = 0;

      // Pattern B Rule 10: full i_status buffer keyed by invoicekey,
      // populated during on_invoice (visits before any lineitem within
      // the custkey group), consumed during on_lineitem.
      std::unordered_map<Integer, Varchar<1>> invoice_buf = {};

      WalkAction on_customer(Integer ck, const customer_coli_t& c)
      {
         c_custkey   = ck;
         c_name      = c.c_name;
         c_address   = c.c_address;
         c_nationkey = c.c_nationkey;
         c_phone     = c.c_phone;
         c_acctbal   = c.c_acctbal;
         c_comment   = c.c_comment;
         return WalkAction::Continue;
      }

      WalkAction on_invoice(const invoice_coli_t::Key& k,
                            const invoice_coli_t&      i)
      {
         invoice_buf.emplace(k.invoicekey, i.i_status);
         return WalkAction::Continue;
      }

      WalkAction on_order(const orders_coli_t::Key& /*k*/,
                          const orders_coli_t&      o)
      {
         o_orderdate = o.o_orderdate;
         return WalkAction::Continue;
      }

      WalkAction on_lineitem(const lineitem_coli_t::Key& k,
                             const lineitem_coli_t&      l)
      {
         auto it = invoice_buf.find(k.invoicekey);
         if (it == invoice_buf.end()) {
            throw std::runtime_error(
                "populate_q10i_view: invoice missing from buffer "
                "(FK violation); custkey-group invariant broken");
         }
         typename q10i_pipeline_view_t::Key vk{
             k.custkey, k.orderkey, k.invoicekey, k.linenumber};
         q10i_pipeline_view_t vv{
             /*l_extendedprice*/ l.l_extendedprice,
             /*l_discount     */ l.l_discount,
             /*l_returnflag   */ l.l_returnflag,
             /*o_orderdate    */ o_orderdate,
             /*i_status       */ it->second,
             /*c_name         */ c_name,
             /*c_address      */ c_address,
             /*c_nationkey    */ c_nationkey,
             /*c_phone        */ c_phone,
             /*c_acctbal      */ c_acctbal,
             /*c_comment      */ c_comment,
         };
         view.insert(vk, vv);
         return WalkAction::Continue;
      }

      void on_group_end(Integer /*ck*/)
      {
         invoice_buf.clear();
         o_orderdate = 0;
      }
   };

   ViewLoadVisitor v{pipeline_view};
   coli_group_walk<Backend>(coli.merged_adapter(), v);
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
