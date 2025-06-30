#pragma once
#include <algorithm>
#include "../../shared/Adapter.hpp"
#include "load.hpp"
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{
void LoadState::advance_customers_in_1city(const size_t step_cnt, int n, int s, int c, int ci)
{
   size_t customer_end = customer_idx + step_cnt;
   if (customer_end >= custkeys.size() && customer_idx < custkeys.size()) {
      std::cout << "WARNING: No customer since nation " << n << ", state " << s << ", county " << c << ", city " << ci << ". " << std::endl;
   }
   for (; customer_idx < custkeys.size() && customer_idx < customer_end; customer_idx++) {
      insert_customer_func(n, s, c, ci, custkeys.at(customer_idx), false); // do not insert view
   }
}

void LoadState::advance_customers_to_hot_cities()
{
   assert(!hot_city_candidates.empty());
   assert(customer_idx < custkeys.size());  // create hot keys
   std::cout << "Assigning " << custkeys.size() - customer_idx << " remaining customers in " << hot_city_candidates.size() << " hot cities..."
             << std::endl;

   std::cout << "Shuffling hot city candidates..." << std::endl;
   std::random_shuffle(hot_city_candidates.begin(), hot_city_candidates.end());
   for (; customer_idx < custkeys.size(); customer_idx++) {
      city_t::Key cik = hot_city_candidates.at(customer_idx % hot_city_candidates.size());
      insert_customer_func(cik.nationkey, cik.statekey, cik.countykey, cik.citykey, custkeys.at(customer_idx), true); // insert view
      if (FLAGS_log_progress && customer_idx % 1000 == 0) {
         std::cout << "\rAssigned " << customer_idx << " customers to hot cities.";
      }
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load()
{
   workload.load();
   load_state = LoadState(workload.last_customer_id, [this](int n, int s, int c, int ci, int cu, bool insert_view) { load_1customer(n, s, c, ci, cu, insert_view); });
   seq_load();
   load_state.advance_customers_to_hot_cities();
   log_sizes();
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::seq_load()
{
   for (int n = 1; n <= workload.NATION_COUNT; n++) {
      // load_1nation(n);
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
      if (!FLAGS_log_progress) {
         std::cout << "\rLoading nation " << n << " with " << state_cnt << " states...";
      }
      for (int s = 1; s <= state_cnt; s++) {
         if (FLAGS_log_progress)
            std::cout << "\rLoading nation " << n << "/" << workload.NATION_COUNT << ", state " << s << "/" << state_cnt << "...";
         load_1state(n, s);
      }
   }
   std::cout << std::endl << "Loaded " << load_state.county_sum << " counties and " << load_state.city_sum << " cities." << std::endl;
      // load view
   auto merged_scanner = merged.template getScanner<sort_key_t, view_t>();
   PremergedJoin<MergedScannerType, sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t> joiner(*merged_scanner, join_view);
   joiner.run();
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1state(int n, int s)
{
   int county_cnt = params::get_county_cnt();
   load_state.county_sum += county_cnt;
   auto sk = states_t::Key{n, s};
   auto sv = states_t::generateRandomRecord(county_cnt);
   states.insert(sk, sv);
   merged.insert(sk, sv);
   ns.insert(sk, sv);
   for (int c = 1; c <= county_cnt; c++) {
      load_1county(n, s, c);
   }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1county(int n, int s, int c)
{
   int city_cnt = params::get_city_cnt();
   load_state.city_sum += city_cnt;
   auto ck = county_t::Key{n, s, c};
   auto cv = county_t::generateRandomRecord(city_cnt);
   county.insert(ck, cv);
   merged.insert(ck, cv);
   ccc.insert(ck, cv);
   for (int ci = 1; ci <= city_cnt; ci++) {
      load_1city(n, s, c, ci);
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1city(int n, int s, int c, int ci)
{
   auto cik = city_t::Key{n, s, c, ci};
   auto civ = city_t::generateRandomRecord();
   city.insert(cik, civ);
   merged.insert(cik, civ);
   ccc.insert(cik, civ);
   int lottery = urand(1, 100);  // 100 is HARDCODED
   if (lottery == 1) {
      load_state.hot_city_candidates.push_back(cik);
   }
   // insert customer2
   load_state.advance_customers_in_1city(params::get_customer_cnt(), n, s, c, ci);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1customer(int n, int s, int c, int ci, int cu, bool insert_view)
{
   customer2_t::Key cust_key{n, s, c, ci, cu};
   assert(s > 0);
   customer2_t cuv;
   workload.customer.lookup1(customerh_t::Key{cu}, [&](const customerh_t& v) { cuv = customer2_t{v}; });
   customer2.insert(cust_key, cuv);
   merged.insert(cust_key, cuv);
   ccc.insert(cust_key, cuv);
   if (!insert_view) {
      return;  // do not insert into view
   }
   view_t::Key view_key{cust_key};
   nation2_t nv;
   nation.lookup1(nation2_t::Key{n}, [&](const nation2_t& v) { nv = v; });
   states_t sv;
   states.lookup1(states_t::Key{n, s}, [&](const states_t& v) { sv = v; });
   county_t cv;
   county.lookup1(county_t::Key{n, s, c}, [&](const county_t& v) { cv = v; });
   city_t civ;
   city.lookup1(city_t::Key{n, s, c, ci}, [&](const city_t& v) { civ = v; });
   view_t view_record{nation2_t{nv}, states_t{sv}, county_t{cv}, city_t{civ}, customer2_t{cuv}};
   join_view.insert(view_key, view_record);
}

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