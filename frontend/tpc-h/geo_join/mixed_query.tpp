#pragma once

#include <optional>
#include "../merge.hpp"
#include "views.hpp"
#include "workload.hpp"

// county name + city count per county

namespace geo_join
{
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_view()
{
   auto county_scanner = county.getScanner();
   auto view_scanner = city_count_per_county.getScanner();
   BinaryMergeJoin<mixed_view_t::Key, mixed_view_t, county_t, city_count_per_county_t> joiner([&]() { return county_scanner->next(); },
                                                                                              [&]() { return view_scanner->next(); });
   joiner.run();
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_merged()
{
   auto scanner = merged.getScanner();
   int curr_city_cnt = 0;
   mixed_view_t::Key curr_key{0, 0, 0};
   Varchar<25> curr_county_name;
   while (true) {
      auto kv = scanner->next();
      if (kv == std::nullopt)
         break;
      std::optional<mixed_view_t::Key> next_key = std::nullopt;
      std::visit(overloaded{[&](const nation2_t::Key&) {}, [&](const states_t::Key&) {},
                            [&](const county_t::Key& ak) {
                               if (curr_key != mixed_view_t::Key{ak}) {
                                  next_key = mixed_view_t::Key{ak};
                               }
                            },
                            [&](const city_t::Key&) {}},
                 kv->first);
      std::visit(overloaded{[&](const nation2_t&) {}, [&](const states_t&) {},
                            [&](const county_t& av) {
                               [[maybe_unused]] mixed_view_t vv{curr_county_name, curr_city_cnt};
                               curr_county_name = av.name;
                               curr_city_cnt = 0;
                               curr_key = next_key.value();
                            },
                            [&](const city_t&) { curr_city_cnt++; }},
                 kv->second);
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_base()
{
   auto county_scanner = county.getScanner();
   auto city_scanner = city.getScanner();

   BinaryMergeJoin<mixed_view_t::Key, mixed_view_t, county_t, city_count_per_county_t> joiner(
       [&]() { return county_scanner->next(); },
       [&]() {
          auto cnt = 0;
          city_count_per_county_t::Key curr_key{0, 0, 0};
          while (true) {
             auto kv = city_scanner->next();
             if (kv == std::nullopt)
                break;
             city_t::Key& k = kv->first;
             if (cnt == 0) curr_key = city_count_per_county_t::Key{k};
             else if (curr_key != city_count_per_county_t::Key{k}) {
                city_scanner->prev(); // go back so that this key is not skipped
                return std::make_optional(std::make_pair(curr_key, city_count_per_county_t{cnt}));
             }
             cnt++;
          }
       });
    joiner.run();
}

}  // namespace geo_join