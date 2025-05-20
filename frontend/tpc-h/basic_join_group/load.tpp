#pragma once
#include "workload.hpp"

namespace basic_join_group
{
template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoinGroup<AdapterType, MergedAdapterType, ScannerType>::load()
{
   workload.load();
   // group lineitem by orderkey into a view with order_date from orders
   auto lineitem_scanner = workload.lineitem.getScanner();
   auto orders_scanner = workload.orders.getScanner();
   Integer curr_orderkey = 0;
   Timestamp curr_orderdate = Timestamp{0};
   Integer count = 0;
   long produced = 0;
   while (true) {
      auto kv = lineitem_scanner->next();
      if (kv == std::nullopt)
         break;
      auto& [lk, lv] = *kv;
      merged.insert(typename merged_lineitem_t::Key(sort_key_t{lk.l_orderkey, Timestamp{0}}, lk), merged_lineitem_t(lv));
      if (curr_orderkey != lk.l_orderkey) {
         if (curr_orderkey != 0) {
            view_t::Key k_new{curr_orderkey, curr_orderdate};
            view_t v_new{count};
            view.insert(k_new, v_new);
            merged.insert(typename merged_view_t::Key(k_new), merged_view_t(v_new));
         }
         auto [ok, ov] = orders_scanner->next().value();
         merged.insert(typename merged_orders_t::Key(sort_key_t{ok.o_orderkey, ov.o_orderdate}, ok), merged_orders_t(ov));
         curr_orderkey = ok.o_orderkey;
         curr_orderdate = ov.o_orderdate;
         count = 0;
      }
      count++;
      TPCH::inspect_produced("Loading all options: ", produced);
   }
   log_sizes();
};

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
double BasicJoinGroup<AdapterType, MergedAdapterType, ScannerType>::get_view_size()
{
   return view.size() + orders.size() + lineitem.size();
};

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
double BasicJoinGroup<AdapterType, MergedAdapterType, ScannerType>::get_merged_size()
{
   return merged.size();
};

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoinGroup<AdapterType, MergedAdapterType, ScannerType>::log_sizes()
{
   workload.log_sizes();
   std::map<std::string, double> sizes = {{"view", get_view_size()}, {"merged", get_merged_size()}};
   logger.log_sizes(sizes);
};
}  // namespace basic_join_group