#pragma once
#include "../shared/adapter-scanner/Adapter.hpp"
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{

void MaintenanceState::adjust_ptrs()
{
   processed_idx = processed_idx % city_reservoir.size();  // as long as erase is active, allow processed_idx to wrap around
   // insertions go rounds and rounds with no regard for erases
   if (erased_idx == city_reservoir.size()) {  // erase just also go rounds and rounds, as long as erased_last_id < inserted_last_id
      if (FLAGS_log_progress) {
         std::cout << "Resetting maintain_erased to 0. customer_ids that remain in db: " << erased_last_id << "--------" << inserted_last_id_ref
                   << std::endl;
      }
      erased_idx = 0;
   }
}

void MaintenanceState::select(const sort_key_t& sk)
{
   scan_urand_next<sort_key_t>(city_reservoir, [&]() {
      return sk;  // next_element is just the sk
   });
}

sort_key_t MaintenanceState::next_cust_to_erase()
{
   const sort_key_t& city = city_reservoir.at(erased_idx++);
   sort_key_t sk{city.nationkey, city.statekey, city.countykey, city.citykey, ++erased_last_id};
   return sk;
}

sort_key_t MaintenanceState::next_cust_to_insert()
{
   const sort_key_t& city = city_reservoir.at(processed_idx++);
   sort_key_t sk{city.nationkey, city.statekey, city.countykey, city.citykey, ++inserted_last_id_ref};
   return sk;
}

void MaintenanceState::cleanup(std::function<void(const sort_key_t&)> erase_func)
{
   while (erased_idx < city_reservoir.size() && erased_last_id < inserted_last_id_ref) { // can happen when not all cities have a customer inserted when maintenance runs slowly
      erase_func(next_cust_to_erase());
   }
   if (remaining_customers_to_erase() != 0) {
      std::cerr << "Error: not all customers were erased, remaining: " << remaining_customers_to_erase() << std::endl;
   }
   reset();
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::select_merged_to_insert()
{
   size_t city_count = workload.last_customer_id / 20;
   maintenance_state.reset_cities(city_count);
   std::cout << "Doing a full scan of merged to randomly select " << maintenance_state.city_count << " for insertion...";
   auto scanner = merged.template getScanner<sort_key_t, view_t>();
   long long scanned = 0;
   while (true) {
      scanned++;
      auto kv = scanner->next();
      if (!kv.has_value()) {
         break;  // no more cities
      }
      auto [k, v] = *kv;
      sort_key_t sk = SKBuilder<sort_key_t>::create(k, v);
      if (sk.citykey == 0) {
         continue;  // skip nation, states, county records
      } else if (sk.custkey != 0) {
         // do search to avoid scanning many customers in one city
         scanner->seekJK(sort_key_t{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, std::numeric_limits<Integer>::max()});
         continue;
      }
      maintenance_state.select(sk);
      if (scanned % 1000 == 0 && FLAGS_log_progress) {
         std::cout << "\rScanned " << scanned << " cities...";
      }
   }
   std::cout << std::endl;
}
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::select_to_insert()
{
   size_t city_count = workload.last_customer_id / 20;
   maintenance_state.reset_cities(city_count);
   std::cout << "Doing a full scan of cities to randomly select " << maintenance_state.city_count << " for insertion...";
   auto scanner = city.getScanner();
   long long scanned = 0;
   while (true) {
      scanned++;
      auto kv = scanner->next();
      if (!kv.has_value()) {
         break;  // no more cities
      }
      auto [k, v] = *kv;
      sort_key_t sk = SKBuilder<sort_key_t>::create(k, v);
      maintenance_state.select(sk);
      if (scanned % 1000 == 0 && FLAGS_log_progress) {
         std::cout << "\rScanned " << scanned << " cities...";
      }
   }
   std::cout << std::endl;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
bool GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::erase_base()
{
   if (!maintenance_state.customer_to_erase())
      return false;
   // const sort_key_t& sk = maintenance_state.next_to_erase();
   sort_key_t sk = maintenance_state.next_cust_to_erase();
   bool ret = customer2.erase(customer2_t::Key{sk});
   if (!ret)
      std::cerr << "Error erasing customer with key: " << sk << std::endl;
   maintenance_state.adjust_ptrs();
   return true;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
bool GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::erase_merged()
{
   if (!maintenance_state.customer_to_erase())
      return false;
   sort_key_t sk = maintenance_state.next_cust_to_erase();
   bool ret = merged.template erase<customer2_t>(customer2_t::Key{sk});
   if (!ret)
      std::cerr << "Error erasing customer with key: " << sk << std::endl;
   maintenance_state.adjust_ptrs();
   return true;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
bool GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::erase_view()
{
   if (!maintenance_state.customer_to_erase())
      return false;
   sort_key_t sk = maintenance_state.next_cust_to_erase();
   view_t::Key vk{sk};
   bool ret_jv = join_view.erase(vk);
   bool ret_c = customer2.erase(customer2_t::Key{sk});
   if (!ret_jv || !ret_c) {
      std::stringstream ss;
      ss << "Error erasing customer in view, ret_jv: " << ret_jv << ", ret_c: " << ret_c << ", sk: " << sk;
      throw std::runtime_error(ss.str());
   }
   mixed_view_t::Key mixed_vk{sk};
   UpdateDescriptorGenerator1(mixed_view_decrementer, mixed_view_t, payloads);
   mixed_view.update1(mixed_vk, [](mixed_view_t& v) { std::get<4>(v.payloads).customer_count--; }, mixed_view_decrementer);
   maintenance_state.adjust_ptrs();
   return true;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_base()
{
   sort_key_t sk = maintenance_state.next_cust_to_insert();

   // Varchar<25> state_name, county_name, city_name;
   // states.lookup1(states_t::Key{sk}, [&](const states_t& s) { state_name = s.name; });
   // county.lookup1(county_t::Key{sk}, [&](const county_t& c) { county_name = c.name; });
   // city.lookup1(city_t::Key{sk}, [&](const city_t& ci) { city_name = ci.name; });

   customer2_t cust_val = customer2_t::generateRandomRecord();
      // state_name, county_name, city_name);
   customer2_t::Key cust_key{sk};
   customer2.insert(cust_key, cust_val);
   if (maintenance_state.erased_idx == 0)
      maintenance_state.delta_table << cust_key << cust_val << "\n";
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_merged()
{
   sort_key_t sk = maintenance_state.next_cust_to_insert();

   Varchar<25> state_name, county_name, city_name;
   // merged.template lookup1<states_t>(states_t::Key{sk}, [&](const states_t& s) { state_name = s.name; });
   // merged.template lookup1<county_t>(county_t::Key{sk}, [&](const county_t& c) { county_name = c.name; });
   // merged.template lookup1<city_t>(city_t::Key{sk}, [&](const city_t& ci) { city_name = ci.name; });

   customer2_t::Key cust_key{sk};
   customer2_t cust_val = customer2_t::generateRandomRecord();
      // state_name, county_name, city_name);
   merged.insert(cust_key, cust_val);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::maintain_view()
{
   sort_key_t sk = maintenance_state.next_cust_to_insert();
   nation2_t nv;
   states_t sv;
   county_t cv;
   city_t civ;
   nation.lookup1(nation2_t::Key{sk}, [&](const nation2_t& n) { nv = n; });
   states.lookup1(states_t::Key{sk}, [&](const states_t& s) { sv = s; });
   county.lookup1(county_t::Key{sk}, [&](const county_t& c) { cv = c; });
   city.lookup1(city_t::Key{sk}, [&](const city_t& ci) { civ = ci; });

   customer2_t::Key cuk{sk};
   customer2_t cuv = customer2_t::generateRandomRecord();
      // sv.name, cv.name, civ.name);
   customer2.insert(cuk, cuv);
   
   view_t::Key vk{sk};
   view_t v{nv, sv, cv, civ, cuv};
   join_view.insert(vk, v);

   mixed_view_t::Key mixed_vk{sk};
   bool customer_exists = mixed_view.tryLookup(mixed_vk, [&](const mixed_view_t&) {});
   if (customer_exists) {
      UpdateDescriptorGenerator1(mixed_update_descriptor, mixed_view_t, payloads);
      mixed_view.update1(mixed_vk, [&](mixed_view_t& mv) { std::get<4>(mv.payloads).customer_count++; }, mixed_update_descriptor);
   } else {
      mixed_view.insert(mixed_vk, mixed_view_t{nv, sv, cv, civ, customer_count_t{1}});
   }
}
}  // namespace geo_join