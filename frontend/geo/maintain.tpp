#pragma once
#include "../shared/adapter-scanner/Adapter.hpp"
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_base()
{
   sort_key_t sk = to_insert.at(maintain_processed++);

   Varchar<25> state_name, county_name, city_name;
   states.lookup1(states_t::Key{sk}, [&](const states_t& s) { state_name = s.name; });
   county.lookup1(county_t::Key{sk}, [&](const county_t& c) { county_name = c.name; });
   city.lookup1(city_t::Key{sk}, [&](const city_t& ci) { city_name = ci.name; });

   customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++workload.last_customer_id};
   customer2_t cust_val = customer2_t::generateRandomRecord(state_name, county_name, city_name);
   customer2.insert(cust_key, cust_val);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_merged()
{
   sort_key_t sk = to_insert.at(maintain_processed++);

   Varchar<25> state_name, county_name, city_name;
   merged.template lookup1<states_t>(states_t::Key{sk}, [&](const states_t& s) { state_name = s.name; });
   merged.template lookup1<county_t>(county_t::Key{sk}, [&](const county_t& c) { county_name = c.name; });
   merged.template lookup1<city_t>(city_t::Key{sk}, [&](const city_t& ci) { city_name = ci.name; });

   customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++workload.last_customer_id};
   customer2_t cust_val = customer2_t::generateRandomRecord(state_name, county_name, city_name);
   merged.insert(cust_key, cust_val);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_view()
{
   sort_key_t sk = to_insert.at(maintain_processed++);
   nation2_t nv;
   states_t sv;
   county_t cv;
   city_t civ;
   nation.lookup1(nation2_t::Key{sk}, [&](const nation2_t& n) { nv = n; });
   states.lookup1(states_t::Key{sk}, [&](const states_t& s) { sv = s; });
   county.lookup1(county_t::Key{sk}, [&](const county_t& c) { cv = c; });
   city.lookup1(city_t::Key{sk}, [&](const city_t& ci) { civ = ci; });
   bool customer_exists = false; // There can be no customers in this city yet
   customer2.scan(
       customer2_t::Key{sk},
       [&](const customer2_t::Key& cuk, const customer2_t& cuv) {
          sort_key_t curr_sk = SKBuilder<sort_key_t>::create(cuk, cuv);
          if (curr_sk.match(sk) == 0) {
             customer_exists = true;
          }
          return false;  // If exists, must be the first one scanned
       },
       []() {});
   customer2_t::Key cuk{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++workload.last_customer_id};
   customer2_t cuv = customer2_t::generateRandomRecord(sv.name, cv.name, civ.name);
   customer2.insert(cuk, cuv);
   view_t::Key vk{cuk};
   view_t vv{nv, sv, cv, civ, cuv};
   join_view.insert(vk, vv);
   if (customer_exists) {
      UpdateDescriptorGenerator1(mixed_update_descriptor, mixed_view_t, payloads);
      mixed_view.update1(mixed_view_t::Key{sk}, [&](mixed_view_t& mv) { std::get<4>(mv.payloads).customer_count++; }, mixed_update_descriptor);
   } else {
      mixed_view.insert(mixed_view_t::Key{sk}, mixed_view_t{nv, sv, cv, civ, customer_count_t{1}});
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_2merged()
{
   sort_key_t sk = to_insert.at(maintain_processed++);

   Varchar<25> state_name, county_name, city_name;
   ns.template lookup1<states_t>(states_t::Key{sk}, [&](const states_t& s) { state_name = s.name; });
   ccc.template lookup1<county_t>(county_t::Key{sk}, [&](const county_t& c) { county_name = c.name; });
   ccc.template lookup1<city_t>(city_t::Key{sk}, [&](const city_t& ci) { city_name = ci.name; });

   customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++workload.last_customer_id};
   customer2_t cust_val = customer2_t::generateRandomRecord(state_name, county_name, city_name);
   ccc.insert(cust_key, cust_val);
}
}  // namespace geo_join