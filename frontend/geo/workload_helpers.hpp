#pragma once
#include "views.hpp"
#include "leanstore/Config.hpp"
#include <functional>
#include <vector>

namespace geo_join {
    template <typename E>
inline void scan_urand_next(std::vector<E>& container, std::function<E()> next_element)
{
   size_t i = container.size();
   if (i < container.capacity()) {  // First, fill city_reservoir with the first 1% of cities
      container.push_back(next_element());
   } else {  // Then, for each subsequent key i, replace a random element in the reservoir with the new key with probability k/i.
      size_t j = rand() % (i + 1);
      if (j < container.capacity()) {
         container.at(j) = next_element();
      }
   }
   if (i % 100 == 1 && FLAGS_log_progress) {
      std::cout << "\rScanned " << i + 1 << " cities..." << std::flush;
   }
};

struct MaintenanceState {
   std::vector<sort_key_t> city_reservoir;  // reservoir sampling for cities
   int& inserted_last_id_ref;
   int erased_last_id;
   size_t city_count;
   size_t processed_idx = 0;
   size_t erased_idx = 0;

   MaintenanceState(int& inserted_last_id_ref) : inserted_last_id_ref(inserted_last_id_ref), erased_last_id(inserted_last_id_ref), city_count(0) {}

   void adjust_ptrs();

   void reset_cities(size_t city_count)
   {
      reset();
      if (this->city_count != 0) {
         return;  // already reserved
      }
      city_reservoir.reserve(city_count);
      this->city_count = city_count;
   }

   void reset()
   {
      city_reservoir.clear();
      processed_idx = 0;
      erased_idx = 0;
      erased_last_id = inserted_last_id_ref;
   }
   bool insertion_complete() const { return processed_idx >= city_reservoir.size(); }
   size_t remaining_customers_to_erase() const { return inserted_last_id_ref - erased_last_id; }
   bool customer_to_erase() const { return remaining_customers_to_erase() > 0; }

   void select(const sort_key_t& sk);

   sort_key_t next_cust_to_erase();

   sort_key_t next_cust_to_insert();

   void cleanup(std::function<void(const sort_key_t&)> erase_func);
};

}