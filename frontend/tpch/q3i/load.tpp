// Operator-translation reference: see ../OPERATORS.md
//   §3 op 4 (load vs query)  — view loading may use a manual two-pointer merge;
//                              query-time joins must use shared join primitives.
//   §4 (per-query view rationale) — unfiltered, predicate hoisted
//
// Template method bodies for Q3IWorkload<Backend> ctor / load / get_size,
// plus the free function populate_q3i_view.

#pragma once

#include <gflags/gflags.h>

#include <stdexcept>
#include <string_view>
#include <unordered_map>

DECLARE_int32(storage_structure);
DECLARE_int32(load_only_structure);
DECLARE_string(coli_walker_variant);

namespace tpch::q3i
{

// ---------------------------------------------------------------------------
// View loading: materialise per-(custkey, orderkey, linenumber) rows into
// the view.
//
// The view is UNFILTERED on parameterised predicates (mktsegment, threshold,
// orderdate, shipdate) — predicate hoisting per OPERATORS.md §4 so it stays
// reusable across param sets.  Per-table filters (i_status='O') ARE applied
// because they are constant, not parameterised.
//
// Schema redesign (2026-05-03): one row per lineitem (not per orderkey).
// The shipdate filter is NOT applied here; query_by_view applies it live at
// query time.  This makes S2 reusable across all DATE param sets.
//
// Algorithm:
//   1. Scan invoice base table; accumulate cust_open_due per custkey in a
//      hash map (i_status='O' fused — constant, not parameterised).
//   2. Scan customer base table; populate c_mktsegment per custkey map.
//   3. Two-pointer merge over orders + lineitem base scanners (both sorted by
//      orderkey); emit one q3i_pipeline_view_t row per (custkey, orderkey,
//      linenumber) carrying unaggregated lineitem fields for query-time revenue.

template <typename Backend>
static void populate_q3i_view(
    typename Backend::template Adapter<customerh_t>&  customer,
    typename Backend::template Adapter<orders_t>&     orders,
    typename Backend::template Adapter<lineitem_i_t>& lineitem,
    typename Backend::template Adapter<invoice_t>&    invoice,
    typename Backend::template Adapter<q3i_pipeline_view_t>& pipeline_view)
{
   // Step 1: build per-custkey open-due map from invoice (i_status='O' filter).
   std::unordered_map<Integer, Numeric> open_due_map;
   {
      auto inv_scan = invoice.getScanner();
      while (auto kv = inv_scan->next()) {
         const invoice_t& inv = kv->second;
         auto s = std::string_view(inv.i_status.data, inv.i_status.length);
         if (s == "O") open_due_map[inv.i_custkey] += inv.i_totaldue;
      }
   }

   // Step 2: build per-custkey mktsegment map from customer.
   std::unordered_map<Integer, Varchar<10>> mktseg_map;
   {
      auto cust_scan = customer.getScanner();
      while (auto kv = cust_scan->next()) {
         mktseg_map[kv->first.c_custkey] = kv->second.c_mktsegment;
      }
   }

   // Step 3: two-pointer merge over orders (sorted by orderkey) and lineitem
   // (sorted by (orderkey, linenumber)).  Emit one view row per lineitem —
   // no shipdate filter here; query_by_view applies it live at query time.
   auto ord_scan = orders.getScanner();
   auto lin_scan = lineitem.getScanner();

   std::optional<std::pair<orders_t::Key,     orders_t>>     cur_ord = ord_scan->next();
   std::optional<std::pair<lineitem_i_t::Key, lineitem_i_t>> cur_lin = lin_scan->next();

   while (cur_ord) {
      const orders_t& o        = cur_ord->second;
      Integer         orderkey = cur_ord->first.o_orderkey;
      Integer         custkey  = o.o_custkey;

      Numeric     open_due = open_due_map.count(custkey) ? open_due_map.at(custkey) : Numeric(0);
      Varchar<10> mktseg   = mktseg_map.count(custkey)   ? mktseg_map.at(custkey)   : Varchar<10>{};

      // Emit one view row per lineitem under this order.
      while (cur_lin && cur_lin->first.l_orderkey == orderkey) {
         const lineitem_i_t& l = cur_lin->second;

         q3i_pipeline_view_t::Key vk{custkey, orderkey, cur_lin->first.l_linenumber};
         q3i_pipeline_view_t      vv;
         vv.l_extendedprice = l.l_extendedprice;
         vv.l_discount      = l.l_discount;
         vv.l_shipdate      = l.l_shipdate;
         vv.cust_open_due   = open_due;
         vv.c_mktsegment    = mktseg;
         vv.o_orderdate     = o.o_orderdate;
         vv.o_shippriority  = o.o_shippriority;
         pipeline_view.insert(vk, vv);

         cur_lin = lin_scan->next();
      }

      cur_ord = ord_scan->next();
   }
}

// ---------------------------------------------------------------------------

template <typename Backend>
Q3IWorkload<Backend>::Q3IWorkload(
    TPCHIWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<customerh_t>&  customer,
    typename Backend::template Adapter<orders_t>&     orders,
    typename Backend::template Adapter<lineitem_i_t>& lineitem,
    typename Backend::template Adapter<invoice_t>&    invoice,
    typename Backend::template Adapter<q3i_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>& merged_coli,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_coli_t>& split_lineitem,
    typename Backend::template Adapter<invoice_coli_t>&  split_invoice,
    typename Backend::template MergedAdapter<customer_acoli_t, orders_acoli_t,
                                             lineitem_acoli_t>& acoli)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      coli(customer, orders, lineitem, invoice, merged_coli,
           split_orders, split_lineitem, split_invoice, acoli),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q3IWorkload<Backend>::load()
{
   tpch.load();
   // Default (FLAGS_load_only_structure < 0): populate ALL secondaries so a
   // single shared DB image can serve every --storage_structure query-time
   // variant. This is the production Makefile flow — load once with no
   // gating, then run multiple --recover invocations selecting different
   // S1/S2/S3/S4/S5 values.
   //
   // Isolated-DB mode (FLAGS_load_only_structure >= 1, A5 experiment): only
   // populate the single secondary needed for that storage structure. Used
   // to remove cross-structure cache pollution (H8) so each path reads
   // *only* the data it actually queries. S4 has no secondary; in that case
   // we just load base tables.
   const int only = FLAGS_load_only_structure;
   const bool load_all = (only < 0);
   if (load_all || only == 1) coli.populate_split();
   if (load_all || only == 2)
      populate_q3i_view<Backend>(customer, orders, lineitem, invoice, pipeline_view);
   if (load_all || only == 3) coli.populate_merged();
   // S4 needs no secondary; nothing extra to populate.
   if (load_all || only == 5) coli.populate_aggregated();
}

template <typename Backend>
double Q3IWorkload<Backend>::get_size() const
{
   // Always include base + active secondary so all four structures are
   // comparable in the CSV size column. Earlier dispatch returned only
   // the secondary for S2/S3, only base for S4, and base+secondary for S1
   // — the inconsistent definition made S2 look unrealistically small
   // (1.25 MiB at SF=40) and made S1/S4 vs S2/S3 unfit for direct
   // comparison.
   double base = customer.size() + orders.size()
               + lineitem.size() + invoice.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + coli.get_split_size();
      case 2: return base + pipeline_view.size();
      case 3: return base + coli.get_merged_size();
      case 4: return base;
      case 5: return base + coli.get_aggregated_size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q3i
