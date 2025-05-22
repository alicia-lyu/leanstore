#pragma once

#include "workload.hpp"

// Number of cities per county (not always equal to last_citykey)

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::agg_in_view()
{
   city_count_per_county.scan(
       city_count_per_county_t::Key{0, 0, 0}, [&](const city_count_per_county_t::Key&, const city_count_per_county_t&) { return true; }, []() {});
}
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::agg_by_merged()
{
   auto scanner = merged.getScanner();
   [[maybe_unused]] int curr_city_cnt = 0;
   while (true) {
      auto kv = scanner->next();
      if (kv == nullptr)
         break;
      std::visit(overloaded{[&](const nation2_t::Key&) {}, [&](const states_t::Key&) {}, [&](const county_t::Key&) { curr_city_cnt = 0; },
                            [&](const city_t::Key&) { curr_city_cnt++; }},
                 kv->first);
   }
}
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::agg_by_base()
{
   int curr_countykey = 0;
   [[maybe_unused]] int curr_city_cnt = 0;
   city.scan(
       city_t::Key{0, 0, 0, 0},
       [&](const city_t::Key& k, const city_t&) {
          if (k.countykey != curr_countykey) {
             curr_countykey = k.countykey;
             curr_city_cnt = 0;
          }
          curr_city_cnt++;
          return true;
       },
       []() {});
}

}  // namespace geo_join