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
   std::cout << "Shuffling customer keys..." << std::endl;
   std::vector<Integer> custkeys(workload.last_customer_id - 1);
   std::iota(custkeys.begin(), custkeys.end(), 1);  // customer id starts from 1
   std::random_shuffle(custkeys.begin(), custkeys.end());
   size_t customer_idx = 0;
   std::vector<city_t::Key> hot_city_candidates = seq_load(custkeys, customer_idx);
   load_hot_cities(custkeys, customer_idx, hot_city_candidates);
   log_sizes();
   // load view
   auto merged_scanner = merged.template getScanner<sort_key_t, view_t>();
   PremergedJoin<MergedScannerType, sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t> joiner(*merged_scanner, join_view);
   joiner.run();
   log_sizes();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
std::vector<city_t::Key> GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::seq_load(std::vector<Integer>& custkeys,
                                                                                                           size_t& customer_idx)
{
   // load geo tables
   // county id starts from 1 in each s tate
   // city id starts from 1 in each county
   long county_sum = 0;
   long city_sum = 0;
   std::vector<city_t::Key> hot_city_candidates;
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
      ns.insert(nk, nv);
      for (int s = 1; s <= state_cnt; s++) {
         std::cout << "\rLoading nation " << n << "/" << workload.NATION_COUNT << ", state " << s << "/" << state_cnt << "...";
         int county_cnt = params::get_county_cnt();
         county_sum += county_cnt;
         auto sk = states_t::Key{n, s};
         auto sv = states_t::generateRandomRecord(county_cnt);
         states.insert(sk, sv);
         merged.insert(sk, sv);
         ns.insert(sk, sv);
         for (int c = 1; c <= county_cnt; c++) {
            int city_cnt = params::get_city_cnt();
            city_sum += city_cnt;
            auto ck = county_t::Key{n, s, c};
            auto cv = county_t::generateRandomRecord(city_cnt);
            county.insert(ck, cv);
            merged.insert(ck, cv);
            ccc.insert(ck, cv);
            if (city_cnt == 0) {
               continue;
            }
            for (int ci = 1; ci <= city_cnt; ci++) {
               auto cik = city_t::Key{n, s, c, ci};
               auto civ = city_t::generateRandomRecord();
               city.insert(cik, civ);
               merged.insert(cik, civ);
               ccc.insert(cik, civ);
               int lottery = urand(1, 100);  // 100 is HARDCODED
               if (lottery == 1) {
                  hot_city_candidates.push_back(cik);
               }
               // insert customer2
               size_t customer_end = customer_idx + params::get_customer_cnt();
               if (customer_end >= custkeys.size() && customer_idx < custkeys.size()) {
                  std::cout << "WARNING: No customer since nation " << n << ", state " << s << ", county " << c << ", city " << ci << ". "
                            << std::endl;
               }
               for (; customer_idx < customer_end && customer_idx < custkeys.size(); customer_idx++) {
                  load_1customer2(n, s, c, ci, custkeys.at(customer_idx));
               }
            }
         }
      }
   }
   std::cout << std::endl << "Loaded " << county_sum << " counties and " << city_sum << " cities." << std::endl;
   return hot_city_candidates;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1customer2(int n, int s, int c, int ci, int cu)
{
   customer2_t::Key cust_key{n, s, c, ci, cu};
   customer2_t cuv;
   workload.customer.lookup1(customerh_t::Key{cu}, [&](const customerh_t& v) { cuv = customer2_t{v}; });
   customer2.insert(cust_key, cuv);
   merged.insert(cust_key, cuv);
   ccc.insert(cust_key, cuv);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_hot_cities(std::vector<Integer>& custkeys,
                                                                                              size_t& customer_idx,
                                                                                              std::vector<city_t::Key>& hot_city_candidates)
{
   assert(customer_idx < custkeys.size());  // create hot keys
   size_t rem_customer_cnt = custkeys.size() - customer_idx;
   std::cout << "Assigning " << rem_customer_cnt << " remaining customers in " << hot_city_candidates.size() << " hot cities..." << std::endl;

   std::cout << "Shuffling hot city candidates..." << std::endl;
   std::random_shuffle(hot_city_candidates.begin(), hot_city_candidates.end());

   auto cust_start = customer_idx;
   for (; customer_idx < custkeys.size(); customer_idx++) {
      city_t::Key cik = hot_city_candidates.at(customer_idx % hot_city_candidates.size());
      load_1customer2(cik.nationkey, cik.statekey, cik.countykey, cik.citykey, custkeys.at(customer_idx));
      TPCH::printProgress("Assigning customers", customer_idx, cust_start, custkeys.size());
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
double GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::get_view_size()
{
   double indexes_size = get_indexes_size();
   return indexes_size + join_view.size();  // + city_count_per_county.size();
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
double GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::get_2merged_size()
{
   return ns.size() + ccc.size();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::log_sizes()
{
   workload.log_sizes();
   double nation_size = nation.size();
   double states_size = states.size();
   double county_size = county.size();
   double city_size = city.size();
   double customer2_size = customer2.size();
   double indexes_size = nation_size + states_size + county_size + city_size + customer2_size;

   double join_view_size = join_view.size();
   // double city_count_per_county_size = city_count_per_county.size();
   double view_size = join_view_size;  // + city_count_per_county_size;

   double merged_size = merged.size();
   double ns_size = ns.size();
   double ccc_size = ccc.size();

   std::map<std::string, double> sizes = {{"nation", nation_size},
                                          {"states", states_size},
                                          {"county", county_size},
                                          {"city", city_size},
                                          {"customer2", customer2_size},
                                          {"indexes", indexes_size},
                                          {"view", view_size},
                                          // {"city_count_per_county", city_count_per_county_size},
                                          {"merged", merged_size},
                                          {"ns", ns_size},
                                          {"ccc", ccc_size}};
   logger.log_sizes(sizes);
};
}  // namespace geo_join