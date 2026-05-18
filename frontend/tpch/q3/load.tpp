// Operator-translation reference: see ../OPERATORS.md
//   §3 op 4 (load vs query)  — view loading uses a manual two-pointer merge;
//                              query-time joins must use shared join primitives.
//   §4 (per-query view rationale) — unfiltered, predicate hoisted
//
// Template method bodies for Q3Workload<Backend> ctor / load / get_size,
// plus the free function populate_q3_view.

#pragma once

#include <gflags/gflags.h>

#include <stdexcept>
#include <unordered_map>

#include "../q3_family/view_loaders.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// View loading: materialise per-(custkey, orderkey, linenumber) rows into
// the view.
//
// The view is UNFILTERED on parameterised predicates (mktsegment, orderdate,
// shipdate) — predicate hoisting per OPERATORS.md §4 so it stays reusable
// across param sets.
//
// Algorithm:
//   1. Scan customer base table; build per-custkey mktsegment map.
//   2. Two-pointer merge over orders + lineitem base scanners (both sorted by
//      orderkey); emit one q3_pipeline_view_t row per (custkey, orderkey,
//      linenumber) carrying unaggregated lineitem fields for query-time revenue.

template <typename Backend>
static void populate_q3_view(
    typename Backend::template Adapter<customerh_t>&       customer,
    typename Backend::template Adapter<orders_t>&          orders,
    typename Backend::template Adapter<lineitem_t>&         lineitem,
    typename Backend::template Adapter<q3_pipeline_view_t>& pipeline_view)
{
   // Step 1: build per-custkey mktsegment map from customer.
   std::unordered_map<Integer, Varchar<10>> mktseg_map;
   {
      auto cust_scan = customer.getScanner();
      while (auto kv = cust_scan->next()) {
         mktseg_map[kv->first.c_custkey] = kv->second.c_mktsegment;
      }
   }

   // Step 2: delegate the O×L two-pointer merge to the shared family core.
   // The emit callback closes over the mktseg_map and attaches c_mktsegment.
   q3_family::populate_q3_view_core(
       orders, lineitem,
       [&](Integer custkey, Integer orderkey, Integer linenumber,
           const orders_t& o, const lineitem_t& l) {
          Varchar<10> mktseg = mktseg_map.count(custkey)
                                   ? mktseg_map.at(custkey) : Varchar<10>{};

          q3_pipeline_view_t::Key vk{custkey, orderkey, linenumber};
          q3_pipeline_view_t      vv;
          vv.l_extendedprice = l.l_extendedprice;
          vv.l_discount      = l.l_discount;
          vv.l_shipdate      = l.l_shipdate;
          vv.c_mktsegment    = mktseg;
          vv.o_orderdate     = o.o_orderdate;
          vv.o_shippriority  = o.o_shippriority;
          pipeline_view.insert(vk, vv);
       });
}

// ---------------------------------------------------------------------------

template <typename Backend>
Q3Workload<Backend>::Q3Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<customerh_t>&  customer,
    typename Backend::template Adapter<orders_t>&     orders,
    typename Backend::template Adapter<lineitem_t>&   lineitem,
    typename Backend::template Adapter<q3_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_col_t>& split_lineitem)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      col(customer, orders, lineitem, merged_col, split_orders, split_lineitem),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q3Workload<Backend>::populate_secondaries()
{
   // Populate every secondary so a single DB image can serve all
   // --storage_structure query-time variants (production Makefile flow).
   // S4 has no secondary; nothing extra to populate for it.
   col.populate_split();    // S1: custkey-sorted split indexes
   populate_q3_view<Backend>(customer, orders, lineitem, pipeline_view);  // S2
   col.populate_merged();   // S3: COL merged index
   // S4: base tables only — nothing to populate.
}

template <typename Backend>
void Q3Workload<Backend>::load()
{
   tpch.load();
   populate_secondaries();
}

template <typename Backend>
double Q3Workload<Backend>::get_size() const
{
   // Include base + active secondary so all four structures are comparable.
   double base = customer.size() + orders.size() + lineitem.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + col.get_split_size();
      case 2: return base + pipeline_view.size();
      case 3: return base + col.get_merged_size();
      case 4: return base;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q3
