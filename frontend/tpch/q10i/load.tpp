// Template method bodies for Q10IWorkload<Backend> ctor / load / get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>
#include <unordered_map>

#include "../tpch_family/walk_action.hpp"
#include "../tpchi_family/coli_pipeline.hpp"

DECLARE_int32(storage_structure);
DECLARE_string(q10i_view_variant);

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
                                             lineitem_acoli_t>& acoli,
    typename Backend::template Adapter<q10i_pipeline_view_preagg_t>& pipeline_view_preagg,
    typename Backend::template MergedAdapter<customer_coli_t, orders_acoli_q10i_t>& acoli_q10i)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      nation(nation),
      coli(customer, orders, lineitem, invoice, merged_coli,
           split_orders, split_lineitem, split_invoice, acoli),
      pipeline_view(pipeline_view),
      pipeline_view_preagg(pipeline_view_preagg),
      acoli_q10i(acoli_q10i),
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
   coli.populate_split();        // S1: custkey-sorted split indexes
   coli.populate_merged();       // S3: COLI merged index
   populate_q10i_view();         // S2 variant A: per-lineitem Pattern B view
   populate_q10i_view_preagg();  // S2 variant B: per-order pre-aggregated view
   populate_q10i_acoli();        // S5: aCOLI MI (must run after populate_merged)
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

// ---------------------------------------------------------------------------
// S2-B / S5 shared per-ORDER pre-aggregation loader.
//
// One COLI-MI walk buffers each custkey's invoice statuses (Pattern B Rule 10),
// accumulates every order's returned revenue split paid/open/late
// (l_returnflag='R' + the i_status partition baked — both TPC-H spec
// constants), and flushes one record per order (returned revenue > 0) to a
// sink. NO orderdate filter — baked unfiltered; the parameterised window is
// applied at query time. Two sinks consume the same emit: the S2-B view and the
// S5 aCOLI MI (mirrors Q10's ViewLoadPreagg + Q10AcolLoadSink pattern).
template <typename Sink>
struct Q10IPreaggLoadVisitor {
   Sink& sink;

   // Per-group customer FD snapshot (the view sink rides these per order; the
   // aCOLI sink ignores them — customer records come from a separate scan).
   Integer      c_custkey   = 0;
   Varchar<25>  c_name      = {};
   Varchar<40>  c_address   = {};
   Integer      c_nationkey = 0;
   Varchar<15>  c_phone     = {};
   Numeric      c_acctbal   = 0;
   Varchar<117> c_comment   = {};

   // invoice i_status buffer keyed by invoicekey (invoices precede lineitems
   // within a custkey group — Pattern B Rule 10).
   std::unordered_map<Integer, Varchar<1>> invoice_buf = {};

   // Open-order accumulator.
   Integer   cur_orderkey  = 0;
   Timestamp cur_orderdate = 0;
   Numeric   paid = 0, open = 0, late = 0;
   bool      order_open = false;

   void flush_order()
   {
      if (order_open
          && (paid != Numeric{0} || open != Numeric{0} || late != Numeric{0})) {
         typename q10i_pipeline_view_preagg_t::Key k{c_custkey, cur_orderkey};
         q10i_pipeline_view_preagg_t r{
             /*paid_returns*/ paid, /*open_returns*/ open, /*late_returns*/ late,
             /*o_orderdate */ cur_orderdate,
             /*c_name      */ c_name,      /*c_address */ c_address,
             /*c_nationkey */ c_nationkey, /*c_phone   */ c_phone,
             /*c_acctbal   */ c_acctbal,   /*c_comment */ c_comment,
         };
         sink.emit_preagg(k, r);
      }
      order_open = false;
      paid = open = late = Numeric{0};
   }

   WalkAction on_customer(Integer ck, const customer_coli_t& c)
   {
      c_custkey   = ck;
      c_name      = c.c_name;      c_address = c.c_address;
      c_nationkey = c.c_nationkey; c_phone   = c.c_phone;
      c_acctbal   = c.c_acctbal;   c_comment = c.c_comment;
      return WalkAction::Continue;
   }
   WalkAction on_invoice(const invoice_coli_t::Key& k, const invoice_coli_t& i)
   {
      invoice_buf.emplace(k.invoicekey, i.i_status);
      return WalkAction::Continue;
   }
   WalkAction on_order(const orders_coli_t::Key& k, const orders_coli_t& o)
   {
      flush_order();
      cur_orderkey  = k.orderkey;
      cur_orderdate = o.o_orderdate;
      order_open    = true;
      return WalkAction::Continue;
   }
   WalkAction on_lineitem(const lineitem_coli_t::Key& k, const lineitem_coli_t& l)
   {
      if (l.l_returnflag.data[0] != 'R') return WalkAction::Continue;
      auto it = invoice_buf.find(k.invoicekey);
      if (it == invoice_buf.end()) {
         throw std::runtime_error(
             "Q10IPreaggLoadVisitor: invoice missing from buffer (FK violation)");
      }
      const Numeric rev = l.l_extendedprice * (Numeric{1} - l.l_discount);
      switch (it->second.data[0]) {
         case 'P': paid += rev; break;
         case 'O': open += rev; break;
         case 'L': late += rev; break;
         default: break;
      }
      return WalkAction::Continue;
   }
   void on_group_end(Integer /*ck*/)
   {
      flush_order();
      invoice_buf.clear();
   }
};

// Sink → S2-B per-order view.
template <typename ViewAdapter>
struct Q10IViewPreaggSink {
   ViewAdapter& view;
   void emit_preagg(const q10i_pipeline_view_preagg_t::Key& k,
                    const q10i_pipeline_view_preagg_t&      v)
   {
      view.insert(k, v);
   }
};

// Sink → S5 aCOLI MI (FD customer cols dropped; customer records inserted by
// the separate base scan in populate_q10i_acoli).
template <typename AcoliAdapter>
struct Q10IAcoliSink {
   AcoliAdapter& acoli;
   void emit_preagg(const q10i_pipeline_view_preagg_t::Key& k,
                    const q10i_pipeline_view_preagg_t&      v)
   {
      acoli.template insert<orders_acoli_q10i_t>(
          orders_acoli_q10i_t::Key{k.custkey, k.orderkey},
          orders_acoli_q10i_t{v.o_orderdate, v.paid_returns,
                              v.open_returns, v.late_returns});
   }
};

template <typename Backend>
void Q10IWorkload<Backend>::populate_q10i_view_preagg()
{
   using ViewAdapterT = typename Backend::template Adapter<q10i_pipeline_view_preagg_t>;
   Q10IViewPreaggSink<ViewAdapterT> sink{pipeline_view_preagg};
   Q10IPreaggLoadVisitor<Q10IViewPreaggSink<ViewAdapterT>> v{sink};
   coli_group_walk<Backend>(coli.merged_adapter(), v);
}

template <typename Backend>
void Q10IWorkload<Backend>::populate_q10i_acoli()
{
   // Sub-pass 1: full customer records (mirrors populate_q10_acol).
   {
      auto sc = customer.getScanner();
      while (auto kv = sc->next()) {
         acoli_q10i.template insert<customer_coli_t>(
             customer_coli_t::key_from_base(kv->first),
             customer_coli_t::from_base(kv->second));
      }
   }
   // Sub-pass 2: per-order paid/open/late aggregates from the COLI MI.
   using AcoliAdapterT = typename Backend::template MergedAdapter<customer_coli_t,
                                                                  orders_acoli_q10i_t>;
   Q10IAcoliSink<AcoliAdapterT> sink{acoli_q10i};
   Q10IPreaggLoadVisitor<Q10IAcoliSink<AcoliAdapterT>> v{sink};
   coli_group_walk<Backend>(coli.merged_adapter(), v);
}

template <typename Backend>
double Q10IWorkload<Backend>::get_size() const
{
   double base = customer.size() + orders.size() + lineitem.size()
               + invoice.size() + nation.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + coli.get_split_size();
      // S2 reports the active view variant (A/B).
      case 2: return base + (FLAGS_q10i_view_variant == "preagg"
                                 ? pipeline_view_preagg.size()
                                 : pipeline_view.size());
      case 3: return base + coli.get_merged_size();
      case 4: return base;
      case 5: return base + acoli_q10i.size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q10i
