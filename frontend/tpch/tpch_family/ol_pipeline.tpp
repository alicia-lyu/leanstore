// Template method bodies for OrdersLineitemPipeline<Backend>.

#pragma once

namespace tpch
{

template <typename Backend>
OrdersLineitemPipeline<Backend>::OrdersLineitemPipeline(
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol)
    : orders(orders), lineitem(lineitem), merged_ol(merged_ol)
{
}

template <typename Backend>
void OrdersLineitemPipeline<Backend>::populate_merged()
{
   auto orders_scanner = orders.getScanner();
   while (auto kv = orders_scanner->next()) {
      merged_ol.template insert<orders_t>(kv->first, kv->second);
   }
   auto lineitem_scanner = lineitem.getScanner();
   while (auto kv = lineitem_scanner->next()) {
      merged_ol.template insert<lineitem_t>(kv->first, kv->second);
   }
}

template <typename Backend>
double OrdersLineitemPipeline<Backend>::get_merged_size() const
{
   return merged_ol.size();
}

}  // namespace tpch
