#pragma once

#include <chrono>
#include <optional>
#include <variant>
#include "load.hpp"
#include "views.hpp"
#include "workload.hpp"

// SELECT nationkey, statekey, countykey, COUNT(citykey) AS city_count
// FROM city
// counties with no cities are not included

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::agg_in_view()
{
   logger.reset();
   std::cout << "GeoJoin::agg_in_view()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   [[maybe_unused]] long produced = 0;
   city_count_per_county.scan(
       city_count_per_county_t::Key{0, 0, 0},
       [&](const city_count_per_county_t::Key&, const city_count_per_county_t&) {
          TPCH::inspect_produced("Scaning agg view: ", produced);
          return true;
       },
       []() {});
   auto end = std::chrono::high_resolution_clock::now();
   std::cout << "end at " << produced << std::endl;
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "group", "view", get_view_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_agg_by_view()
{
   city_count_per_county_t::Key start_key{workload.getNationID(), params::get_statekey(), params::get_countykey()};
   city_count_per_county.scan(
       start_key,
       [&](const city_count_per_county_t::Key&, const city_count_per_county_t& v) {
          [[maybe_unused]] int curr_city_cnt = v.city_count;
          return false;
       },
       []() {});
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::agg_by_merged()
{
   logger.reset();
   std::cout << "GeoJoin::agg_by_merged()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   [[maybe_unused]] long produced = 0;
   auto scanner = merged.template getScanner<sort_key_t, view_t>();
   [[maybe_unused]] int curr_city_cnt = 0;
   while (true) {
      auto kv = scanner->next();
      if (kv == std::nullopt)
         break;
      std::visit(overloaded{[&](const nation2_t::Key&) {}, [&](const states_t::Key&) {},
                            [&](const county_t::Key&) {
                               if (curr_city_cnt > 0)
                                  TPCH::inspect_produced("Scaning agg merged: ", produced);
                               curr_city_cnt = 0;
                            },
                            [&](const city_t::Key&) { curr_city_cnt++; }},
                 kv->first);
   }
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   std::cout << "end at " << produced << std::endl;
   logger.log(t, "group", "merged", get_merged_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_agg_by_merged()
{
   auto scanner = merged.template getScanner<sort_key_t, view_t>();
   // land on the first city of a county and then scan
   scanner->template seekTyped<city_t>(city_t::Key{workload.getNationID(), params::get_statekey(), params::get_countykey(), 0});
   [[maybe_unused]] int curr_city_cnt = 0;
   bool scan_end = false;
   while (!scan_end) {
      auto kv = scanner->next();
      if (kv == std::nullopt)
         break;
      std::visit(overloaded{[&](const nation2_t::Key&) { scan_end = true; }, [&](const states_t::Key&) { scan_end = true; },
                            [&](const county_t::Key&) { scan_end = true; },  // end of cities for this county
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
   logger.reset();
   std::cout << "GeoJoin::agg_by_base()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   [[maybe_unused]] long produced = 0;
   int curr_countykey = 0;
   [[maybe_unused]] int curr_city_cnt = 0;
   city.scan(
       city_t::Key{0, 0, 0, 0},
       [&](const city_t::Key& k, const city_t&) {
          if (k.countykey != curr_countykey) {
             TPCH::inspect_produced("Scaning agg base: ", produced);
             curr_countykey = k.countykey;
             curr_city_cnt = 0;
          }
          curr_city_cnt++;
          return true;
       },
       []() {});
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   std::cout << "end at " << produced << std::endl;
   logger.log(t, "group", "base", get_indexes_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_agg_by_base()
{
   city_count_per_county_t::Key curr_key{workload.getNationID(), params::get_statekey(), params::get_countykey()};
   bool start = true;
   [[maybe_unused]] int curr_city_cnt = 0;
   city.scan(
       city_t::Key{curr_key.nationkey, curr_key.statekey, curr_key.countykey, 0},
       [&](const city_t::Key& k, const city_t&) {
          if (start) {  // land on the first city of a county
             curr_key = city_count_per_county_t::Key{k};
             start = false;
          } else if (curr_key != city_count_per_county_t::Key{k}) {
             return false;
          }
          curr_city_cnt++;
          return true;
       },
       []() {});
}

}  // namespace geo_join