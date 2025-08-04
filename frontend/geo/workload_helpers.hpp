#pragma once
#include <functional>
#include <vector>
#include "leanstore/Config.hpp"  // IWYU pragma: keep
#include "views.hpp"

namespace geo_join
{

struct WorkloadStats {
   long long ns_sum = 0;
   long long ns_count = 0;
   long long nsc_sum = 0;
   long long nsc_count = 0;
   long long nscci_sum = 0;
   long long nscci_count = 0;

   long long ns_cust_sum = 0;
   long long ns_mixed_count = 0;
   long long nsc_cust_sum = 0;
   long long nsc_mixed_count = 0;
   long long nscci_cust_sum = 0;
   long long nscci_mixed_count = 0;

   ~WorkloadStats()
   {
      std::cout << "----- WorkloadStats -----" << std::endl;
      std::cout << "join-ns produced on avg: " << (ns_count > 0 ? (double)ns_sum / ns_count : 0) << std::endl;
      std::cout << "join-nsc produced on avg: " << (nsc_count > 0 ? (double)nsc_sum / nsc_count : 0) << std::endl;
      std::cout << "join-nscci produced on avg: " << (nscci_count > 0 ? (double)nscci_sum / nscci_count : 0) << std::endl;
      std::cout << "mixed-ns customer_count per query: " << (nscci_count > 0 ? (double)ns_cust_sum / ns_mixed_count : 0) << std::endl;
      std::cout << "mixed-nsc customer_count per query: " << (nsc_count > 0 ? (double)nsc_cust_sum / nsc_mixed_count : 0) << std::endl;
      std::cout << "mixed-nscci customer_count per query: " << (nscci_count > 0 ? (double)nscci_cust_sum / nscci_mixed_count : 0) << std::endl;
   }
};

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
};

struct MaintenanceState {
   std::vector<sort_key_t> city_reservoir;  // reservoir sampling for cities
   int& inserted_last_id_ref;
   int erased_last_id;
   size_t city_count;
   size_t processed_idx = 0;
   size_t erased_idx = 0;

   MaintenanceState(int& inserted_last_id_ref) : inserted_last_id_ref(inserted_last_id_ref), erased_last_id(inserted_last_id_ref), city_count(0) {}

   ~MaintenanceState() {
      std::cout << "MaintenanceState: Inserted/erased customers till " << inserted_last_id_ref << std::endl;
   }

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

}  // namespace geo_join