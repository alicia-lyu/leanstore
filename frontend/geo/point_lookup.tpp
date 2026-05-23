#pragma once

#include <cstdlib>
#include <iostream>
#include <vector>
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_lookup_hierarchy_base(const customer2_t::Key& ck)
{
   customer2.lookup1(ck, [](const customer2_t&) {});
   city.lookup1(city_t::Key{ck.nationkey, ck.statekey, ck.countykey, ck.citykey}, [](const city_t&) {});
   county.lookup1(county_t::Key{ck.nationkey, ck.statekey, ck.countykey}, [](const county_t&) {});
   states.lookup1(states_t::Key{ck.nationkey, ck.statekey}, [](const states_t&) {});
   nation.lookup1(nation2_t::Key{ck.nationkey}, [](const nation2_t&) {});
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_lookup_hierarchy_merged(const customer2_t::Key& ck)
{
   merged.template lookup1<customer2_t>(ck, [](const customer2_t&) {});
   merged.template lookup1<city_t>(city_t::Key{ck.nationkey, ck.statekey, ck.countykey, ck.citykey}, [](const city_t&) {});
   merged.template lookup1<county_t>(county_t::Key{ck.nationkey, ck.statekey, ck.countykey}, [](const county_t&) {});
   merged.template lookup1<states_t>(states_t::Key{ck.nationkey, ck.statekey}, [](const states_t&) {});
   merged.template lookup1<nation2_t>(nation2_t::Key{ck.nationkey}, [](const nation2_t&) {});
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
std::vector<customer2_t::Key>
GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::sample_customer_keys_base(size_t N)
{
   std::vector<customer2_t::Key> keys;
   keys.reserve(N);
   auto scanner = customer2.getScanner();
   size_t total = 0;
   while (true) {
      auto kv = scanner->next();
      if (!kv.has_value()) break;
      auto [k, _v] = *kv;
      total++;
      if (keys.size() < N) {
         keys.push_back(k);
      } else {
         size_t j = static_cast<size_t>(rand()) % total;
         if (j < N) keys[j] = k;
      }
   }
   std::cout << "[bg-sample] base: sampled " << keys.size() << " of " << total << " customer keys" << std::endl;
   return keys;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
std::vector<customer2_t::Key>
GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::sample_customer_keys_merged(size_t N)
{
   std::vector<customer2_t::Key> keys;
   keys.reserve(N);
   auto scanner = merged.template getScanner<sort_key_t, view_t>();
   size_t total = 0;
   while (true) {
      auto kv = scanner->next();
      if (!kv.has_value()) break;
      auto [k, v] = *kv;
      sort_key_t sk = SKBuilder<sort_key_t>::create(k, v);
      if (sk.custkey == WILDCARD_KEY) continue;  // skip non-customer (nation/state/county/city) records
      total++;
      customer2_t::Key ck{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, sk.custkey};
      if (keys.size() < N) {
         keys.push_back(ck);
      } else {
         size_t j = static_cast<size_t>(rand()) % total;
         if (j < N) keys[j] = ck;
      }
   }
   std::cout << "[bg-sample] merged: sampled " << keys.size() << " of " << total << " customer keys" << std::endl;
   return keys;
}

}  // namespace geo_join
