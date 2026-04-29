// Template method bodies for Q12Workload<Backend> load / size / ctor,
// plus the free function populate_q12_view.

#pragma once

#include <gflags/gflags.h>

#include "../../shared/merge-join/binary_merge_join.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// View loading: materialise the ORDERS x LINEITEM join into a flat adapter.
//
// Mirrors ol_pipeline.tpp::populate_merged in structure, but produces
// joined_ol_t rows rather than dual-writing raw records. BinaryMergeJoin
// drives two base-table scanners; each joined pair is inserted into `view`.

template <typename Backend>
static void populate_q12_view(
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<q12_pipeline_view_t>& view)
{
   // Wrap scanners in lambdas matching the BinaryMergeJoin fetch signature:
   //   std::function<std::optional<std::pair<Key, Record>>()>
   auto orders_scanner   = orders.getScanner();
   auto lineitem_scanner = lineitem.getScanner();

   auto fetch_orders   = [&]() { return orders_scanner->next(); };
   auto fetch_lineitem = [&]() { return lineitem_scanner->next(); };

   // Consume callback: insert each joined pair keyed by its joined_ol_t key.
   // joined_ol_t::Key(orders_t::Key, lineitem_t::Key) builds the compound key
   // from the two constituent keys (views_ol.hpp:122).
   auto consume = [&](const joined_ol_t::Key& k, const joined_ol_t& row) {
      view.insert(k, row);
   };

   BinaryMergeJoin<ol_sort_key_t, joined_ol_t, orders_t, lineitem_t> driver(
       fetch_orders, fetch_lineitem, consume);
   driver.run();
}

// ---------------------------------------------------------------------------

template <typename Backend>
Q12Workload<Backend>::Q12Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<q12_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol)
    : tpch(tpch),
      orders(orders),
      lineitem(lineitem),
      ol(orders, lineitem, merged_ol),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q12Workload<Backend>::load()
{
   // Base tables are loaded by the caller via tpch.load() before this method.
   // Secondary structures are dispatched here; each case is a one-liner.
   switch (FLAGS_storage_structure) {
      case 1: case 4: break;                                              // base only
      case 2: populate_q12_view<Backend>(orders, lineitem, pipeline_view); break;
      case 3: ol.populate_merged(); break;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

template <typename Backend>
double Q12Workload<Backend>::get_size() const
{
   switch (FLAGS_storage_structure) {
      case 1: case 4: return 0.0;
      case 2: return pipeline_view.size();
      case 3: return ol.get_merged_size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q12
