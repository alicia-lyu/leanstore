#pragma once
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
std::pair<customer2_t::Key, customer2_t> GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_base()
{
   std::optional<sort_key_t> sk = std::nullopt;
   while (!sk.has_value()) {
      sk = find_random_geo_key_in_base();
   }
   customer2_t::Key cust_key{sk->nationkey, sk->statekey, sk->countykey, sk->citykey, workload.last_customer_id++};
   customer2_t cust_val = customer2_t::generateRandomRecord();
   customer2.insert(cust_key, cust_val);
   inserted.push_back(*sk);
   return std::make_pair(cust_key, cust_val);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_merged()
{
   std::optional<sort_key_t> sk = std::nullopt;
   while (!sk.has_value()) {
      sk = find_random_geo_key_in_merged();
   }
   customer2_t::Key cust_key{sk->nationkey, sk->statekey, sk->countykey, sk->citykey, workload.last_customer_id++};
   customer2_t cust_val = customer2_t::generateRandomRecord();
   merged.insert(cust_key, cust_val);
   inserted.push_back(*sk);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_view()
{
   auto [cust_key, cust_val] = maintain_base();
   std::optional<nation2_t> nv = std::nullopt;
   std::optional<states_t> sv = std::nullopt;
   std::optional<county_t> cv = std::nullopt;
   std::optional<city_t> civ = std::nullopt;
   nation.lookup1(nation2_t::Key{cust_key.nationkey}, [&](const nation2_t& n) { nv = n; });
   states.lookup1(states_t::Key{cust_key.nationkey, cust_key.statekey}, [&](const states_t& s) { sv = s; });
   county.lookup1(county_t::Key{cust_key.nationkey, cust_key.statekey, cust_key.countykey}, [&](const county_t& c) { cv = c; });
   city.lookup1(city_t::Key{cust_key.nationkey, cust_key.statekey, cust_key.countykey, cust_key.citykey}, [&](const city_t& ci) { civ = ci; });
   assert(nv.has_value() && sv.has_value() && cv.has_value() && civ.has_value());
   view_t::Key vk{cust_key.nationkey, cust_key.statekey, cust_key.countykey, cust_key.citykey, cust_key.custkey};
   view_t vv{*nv, *sv, *cv, *civ, cust_val};
   join_view.insert(vk, vv);
   inserted.push_back(SKBuilder<sort_key_t>::create(cust_key, cust_val));
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_2merged()
{
   std::optional<sort_key_t> sk = std::nullopt;
   while (!sk.has_value()) {
      sk = find_random_geo_key_in_2merged();
   }
   customer2_t::Key cust_key{sk->nationkey, sk->statekey, sk->countykey, sk->citykey, workload.last_customer_id++};
   customer2_t cust_val = customer2_t::generateRandomRecord();
   ccc.insert(cust_key, cust_val);
   inserted.push_back(*sk);
}
} // namespace geo_join