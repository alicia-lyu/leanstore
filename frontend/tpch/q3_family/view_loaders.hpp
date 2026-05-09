#pragma once

// q3_family/view_loaders.hpp — re-export shim.
//
// The two-pointer merge kernel was lifted to tpch_family/col_two_pointer_merge.hpp
// so that non-Q3 consumers (Q5, future Q10) can include it without pulling in
// q3_family machinery.
//
// Existing callers (q3/load.tpp, q3i/load.tpp, q5/load.tpp) that include
// this header continue to compile unchanged.  New code should include
// tpch_family/col_two_pointer_merge.hpp directly and call tpch::col_two_pointer_merge.

#include "../tpch_family/col_two_pointer_merge.hpp"

namespace tpch::q3_family
{

// populate_q3_view_core — backwards-compat alias for tpch::col_two_pointer_merge.
//
// Callers that already name populate_q3_view_core keep working; new callers
// should use tpch::col_two_pointer_merge directly.
template <typename OrdersAdapter, typename LineitemAdapter, typename EmitFn>
inline void populate_q3_view_core(
    OrdersAdapter&   orders,
    LineitemAdapter& lineitem,
    EmitFn           emit)
{
   tpch::col_two_pointer_merge(orders, lineitem, std::forward<EmitFn>(emit));
}

}  // namespace tpch::q3_family
