#pragma once
#include "workload.hpp"

#include "../../shared/Adapter.hpp"

namespace geo_join
{
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
auto GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_base()
{
   int n = workload.getNationID();
   int s;
   nation.lookup1(nation2_t::Key{n}, [&](const nation2_t& n) { s = urand(1, n.last_statekey); });
   UpdateDescriptorGenerator1(states_update_desc, states_t, last_countykey);
   int c;
   states.update1(states_t::Key{n, s}, [&](states_t& s) { c = ++s.last_countykey; }, states_update_desc);
   int city_cnt = params::get_city_cnt();
   county.insert(county_t::Key{n, s, c}, county_t::generateRandomRecord(city_cnt));
   for (int i = 1; i <= city_cnt; i++) {
      city.insert(city_t::Key{n, s, c, i}, city_t::generateRandomRecord());
   }
   return std::make_tuple(n, s, c, city_cnt);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_merged()
{
   int n = workload.getNationID();
   int s;
   // UpdateDescriptorGenerator1(nation_update_desc, nation2_t, last_statekey);
   merged.template lookup1<nation2_t>(nation2_t::Key{n}, [&](const nation2_t& n) { s = urand(1, n.last_statekey); });
   int c;
   UpdateDescriptorGenerator1(states_update_desc, states_t, last_countykey);
   merged.template update1<states_t>(states_t::Key{n, s}, [&](states_t& s) { c = ++s.last_countykey; }, states_update_desc);
   int city_cnt = params::get_city_cnt();
   merged.insert(county_t::Key{n, s, c}, county_t::generateRandomRecord(city_cnt));
   for (int i = 1; i <= city_cnt; i++) {
      merged.insert(city_t::Key{n, s, c, i}, city_t::generateRandomRecord());
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::update_state_in_view(int n,
                                                                                                   int s,
                                                                                                   std::function<void(states_t&)> update_fn)
{
   std::vector<view_t::Key> keys;
   join_view.scan(
       view_t::Key{n, s, 0, 0},
       [&](const view_t::Key& k, const view_t&) {
          if (k.jk.nationkey != n || k.jk.statekey != s)
             return false;
          keys.push_back(k);
          return true;
       },
       []() {});
   for (auto& k : keys) {
      join_view.update1(k, [&](view_t& v) { update_fn(std::get<1>(v.payloads)); });
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_view()
{
   int n, s, c, city_cnt;
   std::tie(n, s, c, city_cnt) = maintain_base();
   std::function<void(states_t&)> update_fn = [&](states_t& s) { s.last_countykey = c; };
   update_state_in_view(n, s, update_fn);
   for (int i = 1; i <= city_cnt; i++) {
      join_view.insert(view_t::Key{n, s, c, i}, view_t::generateRandomRecord(s, c, i));
   }
}
} // namespace geo_join