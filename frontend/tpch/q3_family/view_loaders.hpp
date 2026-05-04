#pragma once

// Shared view-loading core for the Q3 query family.
//
// populate_q3_view_core performs the C×O×L two-pointer merge that is common
// to Q3 and Q3I (and future Q3-family variants).  The caller provides an
// emit callback that writes one view row per (order, lineitem) pair; Q3I's
// callback attaches cust_open_due from a pre-built map; Q3's callback omits
// that field.
//
// Algorithm:
//   1. (Caller's responsibility, before calling this function)
//      Pre-build any per-custkey maps needed by the emit callback
//      (e.g. open_due_map, mktseg_map for Q3I).
//   2. Two-pointer merge over orders (sorted by orderkey) and lineitem
//      (sorted by (orderkey, linenumber)).  For each lineitem, invoke:
//        emit(custkey, orderkey, linenumber, order, lineitem)
//      The callback is responsible for assembling and inserting the view row.

#include "../../shared/Types.hpp"

namespace tpch::q3_family
{

// ---------------------------------------------------------------------------
// populate_q3_view_core
//
// OrdersAdapter  — adapter for orders_t (scanner yields Key + orders_t).
// LineitemAdapter — adapter for any lineitem variant (scanner yields Key with
//                  l_orderkey/l_linenumber fields + payload).
// EmitFn         — callable with signature:
//                    void(Integer custkey, Integer orderkey, Integer linenumber,
//                         const Order& order, const Lineitem& lineitem)
//
// The orders and lineitem adapters must both be sorted by orderkey (as they
// are in all TPC-H base-table and COLI-secondary schemas).
//
// No filters are applied here — predicate hoisting means callers load views
// without parameterised filters so they remain reusable across param sets.

template <typename OrdersAdapter, typename LineitemAdapter, typename EmitFn>
void populate_q3_view_core(
    OrdersAdapter&  orders,
    LineitemAdapter& lineitem,
    EmitFn          emit)
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

}  // namespace tpch::q3_family
