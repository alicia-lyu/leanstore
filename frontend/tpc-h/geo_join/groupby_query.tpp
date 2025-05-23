#pragma once

#include <optional>
#include <variant>
#include "load.hpp"
#include "views.hpp"
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
   auto scanner = merged.getScanner();
   [[maybe_unused]] int curr_city_cnt = 0;
   while (true) {
      auto kv = scanner->next();
      if (kv == std::nullopt)
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
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_agg_by_merged()
{
   auto scanner = merged.getScanner();
   // find valid county key, so that we do not skip county with no cities
   bool ret = scanner->template seekTyped<county_t>(county_t::Key{workload.getNationID(), params::get_statekey(), params::get_countykey()});
   if (!ret) // no record is scanned (too large a key)
      return;
   auto kv = scanner->current();
   county_t::Key* ck = std::get_if<county_t::Key>(&kv->first);
   assert(ck != nullptr);

   [[maybe_unused]] int curr_city_cnt = 0;
   bool scan_end = false;
   while (!scan_end) {
      auto kv = scanner->next();
      if (kv == std::nullopt)
         break;
      std::visit(overloaded{[&](const nation2_t::Key&) { scan_end = true; }, [&](const states_t::Key&) { scan_end = true; }, [&](const county_t::Key&) { scan_end = true; }, // end of cities for this county
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

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_agg_by_base()
{
   city_count_per_county_t::Key curr_key{workload.getNationID(), params::get_statekey(), params::get_countykey()};
   // find valid county key, so that we do not skip county with no cities
   bool found = false;
   county.scan(
       city_count_per_county_t::Key{curr_key.nationkey, curr_key.statekey, curr_key.countykey},
       [&](const city_count_per_county_t::Key& k, const city_count_per_county_t&) {
          curr_key = k;
          found = true;
          return false;
       },
       []() {});
   if (!found) {
      return;
   }

   [[maybe_unused]] int curr_city_cnt = 0;
   city.scan(
       city_t::Key{curr_key.nationkey, curr_key.statekey, curr_key.countykey, 0},
       [&](const city_t::Key& k, const city_t&) {
          if (curr_key != city_count_per_county_t::Key{k}) {
             return false;
          }
          curr_city_cnt++;
          return true;
       },
       []() {});
}

}  // namespace geo_join