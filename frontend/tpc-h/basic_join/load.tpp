#pragma once
#include "workload.hpp"

namespace basic_join
{
template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoin<AdapterType, MergedAdapterType, ScannerType>::loadBaseTables()
{
   workload.load();
}

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoin<AdapterType, MergedAdapterType, ScannerType>::loadSortedLineitem()
{
   // sort lineitem
   auto lineitem_scanner = workload.lineitem.getScanner();
   while (true) {
      auto kv = lineitem_scanner->next();
      if (kv == std::nullopt)
         break;
      auto& [k, v] = *kv;
      join_key_t jk{v.l_partkey, v.l_suppkey};
      sorted_lineitem_t::Key k_new(jk, k);
      sorted_lineitem_t v_new(v);
      this->sortedLineitem.insert(k_new, v_new);
      if (k.l_linenumber == 1)
         workload.printProgress("sortedLineitem", k.l_orderkey, 1, workload.last_order_id);
   }
}

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoin<AdapterType, MergedAdapterType, ScannerType>::loadBasicJoin()
{
   using Merge = MergeJoin<join_key_t, joinedPPsL_t, part_t, partsupp_t, sorted_lineitem_t>;
   auto part_scanner = part.getScanner();
   auto partsupp_scanner = partsupp.getScanner();
   auto lineitem_scanner = sortedLineitem.getScanner();
   Merge multiway_merge(joinedPPsL, *part_scanner.get(), *partsupp_scanner.get(), *lineitem_scanner.get());
   multiway_merge.run();
};

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoin<AdapterType, MergedAdapterType, ScannerType>::loadMergedBasicJoin()
{
   using Merge = Merge<join_key_t, merged_part_t, merged_partsupp_t, merged_lineitem_t>;
   auto part_scanner = part.getScanner();
   auto partsupp_scanner = partsupp.getScanner();
   auto lineitem_scanner = sortedLineitem.getScanner();
   Merge multiway_merge(mergedPPsL, *part_scanner.get(), *partsupp_scanner.get(), *lineitem_scanner.get());
   multiway_merge.run();
}

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoin<AdapterType, MergedAdapterType, ScannerType>::load()
{
   loadBaseTables();
   loadSortedLineitem();
   loadBasicJoin();
   loadMergedBasicJoin();
   log_sizes();
}

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
double BasicJoin<AdapterType, MergedAdapterType, ScannerType>::get_view_size()
{
   return joinedPPsL.size() + get_base_size();
}
template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
double BasicJoin<AdapterType, MergedAdapterType, ScannerType>::get_merged_size()
{
   return mergedPPsL.size();
}
template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
double BasicJoin<AdapterType, MergedAdapterType, ScannerType>::get_base_size()
{
   return part.size() + partsupp.size() + sortedLineitem.size();
}

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
void BasicJoin<AdapterType, MergedAdapterType, ScannerType>::log_sizes()
{
   workload.log_sizes();
   std::map<std::string, double> sizes = {
       {"view", get_view_size()}, {"merged", get_merged_size()}, {"sortedLineitem", sortedLineitem.size()}, {"base", get_base_size()}};

   logger.log_sizes(sizes);
}
}  // namespace basic_join