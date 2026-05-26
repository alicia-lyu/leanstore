#pragma once

// populate_col_shared_view — Pattern-B loader for the S6 shared COL view.
//
// Walks the shared COL MI (the SAME structure S3 uses, via col_group_walk) and
// emits one col_shared_view_t per lineitem, FD-attaching the co-located
// customer + order columns from a LOCAL current-row snapshot — no map. In COL
// byte-lex order (customer -> (orders -> lineitem*)+) a customer's payload is
// live only until the next customer record, so a single local struct suffices;
// modelled on the existing local-snapshot visitors (Q3FamilyVisitor,
// Q10GroupWalkVisitor).
//
// The view carries ONLY COL columns — NATION (n_name) is resolved post-pipeline
// by the query (q5), not stored here. No parameterised filter is baked
// (predicate hoisting): the view is reusable across all param sets, exactly
// like the per-query S2 views.
//
// Run AFTER col.populate_merged() — the loader consumes the COL MI.

#include "../tpch_tables.hpp"
#include "col_pipeline.hpp"  // col_group_walk (incl. its .tpp definition)
#include "views_col.hpp"     // col_shared_view_t, customer_coli_t, orders_coli_t, lineitem_col_t
#include "walk_action.hpp"

namespace tpch
{

// Group-walk visitor that emits one col_shared_view_t per lineitem. No gates,
// no filters (unfiltered view); a local current-customer / current-order
// snapshot carries the FD-attached columns forward through each custkey group.
template <typename SharedViewAdapter>
struct ColSharedViewLoadVisitor {
   SharedViewAdapter& adapter;
   long               rows_inserted = 0;

   // Local current-customer snapshot (overwritten at each on_customer).
   Varchar<10>  cur_c_mktsegment;
   Integer      cur_c_nationkey  = 0;
   Varchar<25>  cur_c_name;
   Varchar<40>  cur_c_address;
   Varchar<15>  cur_c_phone;
   Numeric      cur_c_acctbal    = 0;
   Varchar<117> cur_c_comment;

   // Local current-order snapshot (overwritten at each on_order).
   Timestamp    cur_o_orderdate    = 0;
   Integer      cur_o_shippriority = 0;

   WalkAction on_customer(Integer /*ck*/, const customer_coli_t& c)
   {
      cur_c_mktsegment = c.c_mktsegment;
      cur_c_nationkey  = c.c_nationkey;
      cur_c_name       = c.c_name;
      cur_c_address    = c.c_address;
      cur_c_phone      = c.c_phone;
      cur_c_acctbal    = c.c_acctbal;
      cur_c_comment    = c.c_comment;
      return WalkAction::Continue;  // no per-customer gate — view is unfiltered
   }

   WalkAction on_order(const orders_coli_t::Key& /*k*/, const orders_coli_t& o)
   {
      cur_o_orderdate    = o.o_orderdate;
      cur_o_shippriority = o.o_shippriority;
      return WalkAction::Continue;  // no date filter baked (predicate hoisting)
   }

   WalkAction on_lineitem(const lineitem_col_t::Key& k, const lineitem_col_t& l)
   {
      col_shared_view_t::Key vk{k.custkey, k.orderkey, k.linenumber};
      col_shared_view_t v{
          l.l_extendedprice, l.l_discount, l.l_shipdate, l.l_suppkey, l.l_returnflag,
          cur_o_orderdate, cur_o_shippriority,
          cur_c_mktsegment, cur_c_nationkey, cur_c_name, cur_c_address,
          cur_c_phone, cur_c_acctbal, cur_c_comment};
      adapter.insert(vk, v);
      ++rows_inserted;
      return WalkAction::Continue;
   }
};

template <typename Backend>
long populate_col_shared_view(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<col_shared_view_t>&    shared_view)
{
   using ViewAdapterT = typename Backend::template Adapter<col_shared_view_t>;
   ColSharedViewLoadVisitor<ViewAdapterT> visitor{shared_view};
   ::tpch::col_group_walk<Backend>(merged_col, visitor);
   return visitor.rows_inserted;
}

}  // namespace tpch
