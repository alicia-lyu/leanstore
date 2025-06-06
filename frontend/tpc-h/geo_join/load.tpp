#pragma once
#include <algorithm>
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
   long county_sum = 0;
   long city_sum = 0;
   std::cout << "Shuffling customer keys..." << std::endl;
   std::vector<Integer> custkeys(workload.last_customer_id - 1);
   std::iota(custkeys.begin(), custkeys.end(), 1);  // customer id starts from 1
   std::random_shuffle(custkeys.begin(), custkeys.end());
   size_t customer_idx = 0;
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
         county_sum += county_cnt;
         auto sk = states_t::Key{n, s};
         auto sv = states_t::generateRandomRecord(county_cnt);
         states.insert(sk, sv);
         merged.insert(sk, sv);
         for (int c = 1; c <= county_cnt; c++) {
            int city_cnt = params::get_city_cnt();
            city_sum += city_cnt;
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
               // insert customer2
               size_t customer_end = customer_idx + params::get_customer_cnt();
               if (customer_end >= custkeys.size() && customer_idx < custkeys.size()) {
                  std::cout << "WARNING: No customer since nation " << n << ", state " << s << ", county " << c << ", city " << ci << ". "
                            << std::endl;
               }
               for (; customer_idx < customer_end && customer_idx < custkeys.size(); customer_idx++) {
                  auto custkey = custkeys[customer_idx];
                  customer2_t::Key cust_key{n, s, c, ci, custkey};
                  customer2_t cuv;
                  workload.customer.lookup1(customerh_t::Key{custkey}, [&](const customerh_t& v) { cuv = customer2_t{v}; });
                  customer2.insert(cust_key, cuv);
                  merged.insert(cust_key, cuv);
               }
            }
         }
      }
   }
   if (customer_idx < custkeys.size()) {
      std::cout << std::endl << "Assigning " << custkeys.size() - customer_idx << " remaining customers...." << std::endl;
      for (; customer_idx < custkeys.size(); customer_idx++) {
         auto custkey = custkeys[customer_idx];
         sort_key_t sk;
         while (true) {
            auto found = find_random_geo_key_in_merged();
            if (found.has_value()) {
               sk = *found;
               break;
            }
         }
         customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, custkey};
         customer2_t cuv;
         workload.customer.lookup1(customerh_t::Key{custkey}, [&](const customerh_t& v) { cuv = customer2_t{v}; });
         customer2.insert(cust_key, cuv);
         merged.insert(cust_key, cuv);
      }
   }
   std::cout << "Loaded " << county_sum << " counties and " << city_sum << " cities." << std::endl;
   // --------------------------------------- load view ---------------------------------------
   auto merged_scanner = merged.template getScanner<sort_key_t, view_t>();
   PremergedJoin<MergedScannerType, sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t> joiner(*merged_scanner, join_view);
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
   return nation.size() + states.size() + county.size() + city.size() + customer2.size();
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
   std::map<std::string, double> sizes = {{"view", get_view_size()}, {"base", get_indexes_size()}, {"merged", get_merged_size()}};
   logger.log_sizes(sizes);
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::log_sizes_sep()
{
   workload.log_sizes();
   std::map<std::string, double> sizes = {{"nation", nation.size()},
                                          {"states", states.size()},
                                          {"county", county.size()},
                                          {"city", city.size()},
                                          {"city_count_per_county", city_count_per_county.size()},
                                          {"join_view", join_view.size()},
                                          {"customer2", customer2.size()}};
   logger.log_sizes(sizes);
};
}  // namespace geo_join