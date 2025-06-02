#pragma once
#include "../../shared/Adapter.hpp"
#include "load.hpp"
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load()
{
   workload.load();
   // --------------------------------------- load geo tables ---------------------------------------
   // county id starts from 1 in each s tate
   // city id starts from 1 in each county
   for (int n = 1; n <= workload.NATION_COUNT; n++) {
      // state id starts from 1 in each nation
      int state_cnt = params::get_state_cnt();
      UpdateDescriptorGenerator1(nation_update_desc, nation2_t, last_statekey);
      auto nk = nation2_t::Key{n};
      nation2_t nv;
      auto update_fn = [&](nation2_t& n) {
         n.last_statekey = state_cnt;
         nv = n;
      };
      nation.update1(nk, update_fn, nation_update_desc);
      merged.insert(nk, nv);
      for (int s = 1; s <= state_cnt; s++) {
         std::cout << "\rLoading nation " << n << "/" << workload.NATION_COUNT << ", state " << s << "/" << state_cnt << "...";
         int county_cnt = params::get_county_cnt();
         auto sk = states_t::Key{n, s};
         auto sv = states_t::generateRandomRecord(county_cnt);
         states.insert(sk, sv);
         merged.insert(sk, sv);
         for (int c = 1; c <= county_cnt; c++) {
            int city_cnt = params::get_city_cnt();
            auto ck = county_t::Key{n, s, c};
            auto cv = county_t::generateRandomRecord(city_cnt);
            county.insert(ck, cv);
            merged.insert(ck, cv);
            if (city_cnt == 0) {
               continue;
            }
            city_count_per_county.insert(city_count_per_county_t::Key{n, s, c}, city_count_per_county_t{city_cnt});  // not including empty counties
            for (int ci = 1; ci <= city_cnt; ci++) {
               auto cik = city_t::Key{n, s, c, ci};
               auto civ = city_t::generateRandomRecord();
               city.insert(cik, civ);
               merged.insert(cik, civ);
            }
         }
      }
   }
   // --------------------------------------- load customer2 table ---------------------------------------
   auto cust_scanner = workload.customer.getScanner();
   while (true) {
      auto kv = cust_scanner->next();
      if (kv == std::nullopt)
         break;
      customerh_t::Key& k = kv->first;
      customerh_t& v = kv->second;
      customer2_t new_v{v};
      int statekey, countykey, citykey;
      city.scan(
          city_t::Key{v.c_nationkey, params::get_statekey(), params::get_countykey(), params::get_citykey()},
          [&](const city_t::Key& k, const city_t&) {
             statekey = k.statekey;
             countykey = k.countykey;
             citykey = k.citykey;
             return false;  // stop after the first match
          },
          []() {});
      customer2_t::Key new_k{k, v.c_nationkey, statekey, countykey, citykey};
      customer2.insert(new_k, new_v);
      merged.insert(new_k, new_v);
   }
   // --------------------------------------- load view ---------------------------------------
   auto merged_scanner = merged.template getScanner<sort_key_t, view_t>();
   PremergedJoin<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t> joiner(*merged_scanner, join_view);
   joiner.run();
   log_sizes();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
double GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::get_view_size()
{
   double indexes_size = get_indexes_size();
   return indexes_size + join_view.size() + city_count_per_county.size();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
double GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::get_indexes_size()
{
   return nation.size() + states.size() + county.size() + city.size();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
double GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::get_merged_size()
{
   return merged.size();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::log_sizes()
{
   workload.log_sizes();
   log_sizes_other();
   std::map<std::string, double> sizes = {{"view", get_view_size()},    {"base", get_indexes_size()},
                                          {"merged", get_merged_size()}};
   logger.log_sizes(sizes);
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::log_sizes_other()
{
   workload.log_sizes();
   std::map<std::string, double> sizes = {{"nation", nation.size()}, {"states", states.size()}, {"county", county.size()}, {"city", city.size()},
                                          {"city_count_per_county", city_count_per_county.size()}, {"join_view", join_view.size()}};
   logger.log_sizes(sizes);
};
}  // namespace geo_join