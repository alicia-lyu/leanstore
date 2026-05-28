// Operator-translation reference: see ../OPERATORS.md
//   §3 op 4 (load vs query)  — view loading uses a manual merge;
//                              query-time joins must use shared join primitives.
//   §4 (per-query view rationale) — unfiltered, predicate hoisted
//
// Template method bodies for Q5Workload<Backend> ctor / load / get_size,
// plus the free function populate_q5_view.

#pragma once

#include <gflags/gflags.h>

#include <stdexcept>
#include <unordered_map>

#include "../q3_family/view_loaders.hpp"
#include "../tpch_family/shared_view_loader.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q5
{

// Build one Q5 pipeline-view row. Caller owns FD-source lookups
// (c_nationkey from customer, n_name from nation). Shared between load-time
// `populate_q5_view` and runtime `Q5Workload::maintain_rf1` so the per-row
// view shape is identical across bulk-load and incremental paths.
inline std::pair<q5_pipeline_view_t::Key, q5_pipeline_view_t>
build_q5_view_row(Integer custkey, Integer orderkey, Integer linenumber,
                  const orders_t& o, const lineitem_t& l,
                  Integer c_nationkey, const Varchar<25>& n_name)
{
   q5_pipeline_view_t::Key vk{custkey, orderkey, linenumber};
   q5_pipeline_view_t      vv;
   vv.l_extendedprice = l.l_extendedprice;
   vv.l_discount      = l.l_discount;
   vv.l_suppkey       = l.l_suppkey;
   vv.c_nationkey     = c_nationkey;
   vv.n_name          = n_name;
   vv.o_orderdate     = o.o_orderdate;
   return {vk, vv};
}

// ---------------------------------------------------------------------------
// View loading: materialise per-(custkey, orderkey, linenumber) rows into
// the Q5 pipeline view.
//
// The view is UNFILTERED on parameterised predicates (region, orderdate) —
// predicate hoisting per OPERATORS.md §4 so it stays reusable across all
// REGION / DATE param sets.
//
// Algorithm:
//   1. Scan customer base table; build per-custkey c_nationkey map.
//   2. Scan NATION base table to build nationkey → n_name map (~25 entries).
//      n_name is FD-determined by c_nationkey, so it can be attached at load
//      time without violating predicate hoisting — it is not parameterised.
//   3. Two-pointer merge over orders + lineitem base scanners (both sorted by
//      orderkey); emit one q5_pipeline_view_t row per (custkey, orderkey,
//      linenumber) carrying unaggregated lineitem fields plus FD-attached
//      c_nationkey and n_name for query-time revenue accumulation.

template <typename Backend>
static void populate_q5_view(
    typename Backend::template Adapter<customerh_t>&        customer,
    typename Backend::template Adapter<nation_t>&           nation,
    typename Backend::template Adapter<orders_t>&           orders,
    typename Backend::template Adapter<lineitem_t>&          lineitem,
    typename Backend::template Adapter<q5_pipeline_view_t>& pipeline_view)
{
   // Step 1: build per-custkey nationkey map from customer base table.
   std::unordered_map<Integer, Integer> nationkey_map;
   {
      auto cust_scan = customer.getScanner();
      while (auto kv = cust_scan->next()) {
         nationkey_map[kv->first.c_custkey] = kv->second.c_nationkey;
      }
   }

   // Step 2: build nationkey → n_name from the NATION base table (~25 rows).
   // Bounds the total NATION PK lookups during load to at most 25, regardless
   // of the customer count (~150K at SF=1).  n_name is FD-determined by
   // nationkey — attaching it at load time is sound (not parameterised).
   std::unordered_map<Integer, std::string> nation_name_map;
   {
      auto nat_scan = nation.getScanner();
      while (auto kv = nat_scan->next()) {
         nation_name_map[kv->first.n_nationkey] =
             std::string(kv->second.n_name.data,
                         strnlen(kv->second.n_name.data,
                                 sizeof(kv->second.n_name.data)));
      }
   }

   // Step 3: delegate the O×L two-pointer merge to the shared family core.
   // The emit callback closes over nationkey_map / nation_name_map and
   // attaches c_nationkey, n_name, and o_orderdate per row.
   q3_family::populate_q3_view_core(
       orders, lineitem,
       [&](Integer custkey, Integer orderkey, Integer linenumber,
           const orders_t& o, const lineitem_t& l) {
          Integer c_nationkey = nationkey_map.count(custkey)
                                    ? nationkey_map.at(custkey) : Integer(0);
          std::string n_name;
          auto nit = nation_name_map.find(c_nationkey);
          if (nit != nation_name_map.end()) n_name = nit->second;

          // Convert std::string → Varchar<25> (TPC-H spec: n_name ≤ 25 chars;
          // missing → zero-init Varchar is fine for digest parity).
          auto [vk, vv] = build_q5_view_row(custkey, orderkey, linenumber,
                                             o, l, c_nationkey,
                                             Varchar<25>(n_name.c_str()));
          pipeline_view.insert(vk, vv);
       });
}

// ---------------------------------------------------------------------------

template <typename Backend>
Q5Workload<Backend>::Q5Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<customerh_t>&  customer,
    typename Backend::template Adapter<orders_t>&     orders,
    typename Backend::template Adapter<lineitem_t>&   lineitem,
    typename Backend::template Adapter<supplier_t>&   supplier,
    typename Backend::template Adapter<nation_t>&     nation,
    typename Backend::template Adapter<region_t>&     region,
    typename Backend::template Adapter<q5_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_col_t>& split_lineitem,
    typename Backend::template Adapter<col_shared_view_t>& shared_view)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      supplier(supplier),
      nation(nation),
      region(region),
      col(customer, orders, lineitem, merged_col, split_orders, split_lineitem),
      pipeline_view(pipeline_view),
      shared_view(shared_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q5Workload<Backend>::populate_secondaries()
{
   // Populate every secondary so a single DB image can serve all
   // --storage_structure query-time variants (production Makefile flow).
   // S4 has no secondary; nothing extra to populate for it.
   col.populate_split();    // S1: custkey-sorted split indexes
   populate_q5_view<Backend>(customer, nation, orders, lineitem, pipeline_view);  // S2
   col.populate_merged();   // S3: COL merged index
   // S4: base tables only — nothing to populate.
   // S6: COL-family shared union view (standalone load only; in the vanilla
   // family q3 is the canonical loader and writes it — q5 uses
   // populate_view_only() there, which skips this so it's written once).
   populate_col_shared_view<Backend>(col.merged_adapter(), shared_view);
}

template <typename Backend>
void Q5Workload<Backend>::populate_view_only()
{
   populate_q5_view<Backend>(customer, nation, orders, lineitem, pipeline_view);  // S2
}

template <typename Backend>
void Q5Workload<Backend>::load()
{
   tpch.load();
   populate_secondaries();
}

template <typename Backend>
double Q5Workload<Backend>::get_size() const
{
   // Include base + active secondary so all four structures are comparable.
   double base = customer.size() + orders.size() + lineitem.size()
               + supplier.size() + nation.size() + region.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + col.get_split_size();
      case 2: return base + pipeline_view.size();
      case 3: return base + col.get_merged_size();
      case 4: return base;
      case 6: return base + shared_view.size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q5
