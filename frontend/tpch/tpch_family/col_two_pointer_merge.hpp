#pragma once

// tpch_family/col_two_pointer_merge.hpp — shared ORDERS × LINEITEM merge
// kernel for all queries whose view is keyed by (custkey, orderkey, linenumber).
//
// `tpch::col_two_pointer_merge` performs a two-pointer forward merge over
// orders (sorted by orderkey) and lineitem (sorted by (orderkey, linenumber)).
// For each lineitem belonging to an order it calls:
//
//   emit(custkey, orderkey, linenumber, order, lineitem)
//
// The caller supplies the emit callback and is responsible for any
// per-custkey pre-maps (e.g. Q3I's open_due_map, Q5's nationkey_map) that
// must be built before invoking this function.  No filters are applied inside
// the kernel — predicate hoisting keeps secondaries reusable across param sets.
//
// Lifted from q3_family/view_loaders.hpp (populate_q3_view_core) so that
// non-Q3 consumers (Q5, future Q10) can include it without pulling in
// q3_family machinery.  q3_family/view_loaders.hpp is now a thin re-export
// shim that delegates here.
//
// OrdersAdapter   — adapter for orders_t; scanner yields Key + orders_t.
//                   Key must expose an `o_orderkey` field.
// LineitemAdapter — adapter for any lineitem variant; scanner yields a Key
//                   with `l_orderkey` / `l_linenumber` fields + payload.
// EmitFn          — callable with signature:
//                     void(Integer custkey, Integer orderkey,
//                          Integer linenumber,
//                          const Order& order, const Lineitem& lineitem)

#include "../../shared/Types.hpp"

namespace tpch
{

template <typename OrdersAdapter, typename LineitemAdapter, typename EmitFn>
void col_two_pointer_merge(
    OrdersAdapter&   orders,
    LineitemAdapter& lineitem,
    EmitFn           emit)
{
   auto ord_scan = orders.getScanner();
   auto lin_scan = lineitem.getScanner();

   auto cur_ord = ord_scan->next();
   auto cur_lin = lin_scan->next();

   while (cur_ord) {
      const auto& o        = cur_ord->second;
      Integer     orderkey = cur_ord->first.o_orderkey;
      Integer     custkey  = o.o_custkey;

      // Emit one view row per lineitem belonging to this order.
      while (cur_lin && cur_lin->first.l_orderkey == orderkey) {
         emit(custkey, orderkey, cur_lin->first.l_linenumber,
              o, cur_lin->second);
         cur_lin = lin_scan->next();
      }

      cur_ord = ord_scan->next();
   }
}

}  // namespace tpch
