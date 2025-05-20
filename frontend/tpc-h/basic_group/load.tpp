#pragma once
#include "workload.hpp"

namespace basic_group
{
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
void BasicGroup<AdapterType, MergedAdapterType, merged_view_option_t, merged_partsupp_option_t>::loadBaseTables()
{
   workload.load();
};
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
void BasicGroup<AdapterType, MergedAdapterType, merged_view_option_t, merged_partsupp_option_t>::loadAllOptions()
{
   auto partsupp_scanner = workload.partsupp.getScanner();
   Integer count = 0;
   Numeric supplycost_sum = 0;
   Integer curr_partkey = 0;
   int num_part_keys = 0;
   while (true) {
      auto kv = partsupp_scanner->next();
      if (kv == std::nullopt) {
         insert_agg(curr_partkey, count, supplycost_sum);
         break;
      }
      auto& [k, v] = *kv;
      mergedBasicGroup.insert(typename merged_partsupp_option_t::Key(k), merged_partsupp_option_t(v));
      if (k.ps_partkey == curr_partkey) {
         count++;
         supplycost_sum += v.ps_supplycost;
      } else {
         if (curr_partkey != 0) {
            insert_agg(curr_partkey, count, supplycost_sum);
            std::cout << "\rLoading views and indexes for " << num_part_keys++ << " part keys------------------------------------";
         }

         curr_partkey = k.ps_partkey;
         count = 1;
         supplycost_sum = v.ps_supplycost;
      }
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
void BasicGroup<AdapterType, MergedAdapterType, merged_view_option_t, merged_partsupp_option_t>::insert_agg(Integer curr_partkey,
                                                                                                            Integer count,
                                                                                                            Numeric supplycost_sum)
{
   view.insert(view_t::Key({curr_partkey}), view_t({count, supplycost_sum}));
   mergedBasicGroup.insert(typename merged_view_option_t::Key(curr_partkey), merged_view_option_t(view_t{count, supplycost_sum}));
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
void BasicGroup<AdapterType, MergedAdapterType, merged_view_option_t, merged_partsupp_option_t>::load(){{loadBaseTables();
loadAllOptions();
log_sizes();
}  // namespace basic_group
}
;

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
double BasicGroup<AdapterType, MergedAdapterType, merged_view_option_t, merged_partsupp_option_t>::get_view_size()
{
   return view.size() + partsupp.size();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
double BasicGroup<AdapterType, MergedAdapterType, merged_view_option_t, merged_partsupp_option_t>::get_merged_size()
{
   return mergedBasicGroup.size();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
void BasicGroup<AdapterType, MergedAdapterType, merged_view_option_t, merged_partsupp_option_t>::log_sizes()
{
   workload.log_sizes();
   std::map<std::string, double> sizes = {{"view", view.size() + partsupp.size()}, {"merged", mergedBasicGroup.size()}};

   logger.log_sizes(sizes);
};
}
;  // namespace basic_group