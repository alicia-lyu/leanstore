#pragma once

#include <gflags/gflags.h>
#include "../randutils.hpp"
#include "views.hpp"

// 1 nation + 1--80 states --- 1--6 pages
// 1 county + 1--4 cities + 0--8 customers --- 1 page

namespace geo_join
{
struct LoadState {
   long county_sum = 0;
   long city_sum = 0;
   std::vector<Integer> custkeys;
   size_t customer_idx = 0;
   std::vector<city_t::Key> hot_city_candidates;
   std::function<void(int, int, int, int, int, bool)> insert_customer_func;

   LoadState() = default;

   LoadState(int last_customer_id, std::function<void(int, int, int, int, int, bool)> insert_customer_func)
       : custkeys(last_customer_id - 1), customer_idx(0), hot_city_candidates(), insert_customer_func(insert_customer_func)
   {
      std::iota(custkeys.begin(), custkeys.end(), 1);  // customer id starts from 1
      std::random_shuffle(custkeys.begin(), custkeys.end());
   }

   void if_customers_drain(size_t customer_end, int n, int s, int c, int ci);

   void advance_customers_in_1city(const size_t step_cnt, int n, int s, int c, int ci);

   void advance_customers_to_hot_cities();
};
struct Params {
   int nation_count;
   int state_max;     // in a nation
   int county_max;    // in a state
   int city_max;      // in a county
   int customer_max;  // in a city

   Params() : nation_count(25), state_max(80), county_max(200), city_max(4), customer_max(2) {}

   Params(int nation_count, int state_max, int county_max, int city_max, int customer_max)
       : nation_count(nation_count), state_max(state_max), county_max(county_max), city_max(city_max), customer_max(customer_max)
   {
   }

   int get_state_cnt() { return randutils::urand(1, state_max); }

   int get_county_cnt() { return randutils::urand(1, county_max); }

   int get_city_cnt() { return randutils::urand(1, city_max); }

   int get_customer_cnt()  // avg: 1.5
   {
      // int lottery = randutils::urand(1, 3);
      // if (lottery <= 2) return 0;
      // else return randutils::urand(0, 12);
      return randutils::urand(0, customer_max);
      // some hot cities emerge during the final assignment
   }

   int get_nationkey() { return randutils::urand(1, nation_count); }
   int get_statekey() { return randutils::urand(1, state_max / 2); }
   int get_countykey() { return randutils::urand(1, county_max / 2); }
   int get_citykey() { return randutils::urand(1, city_max / 2); }
};
}  // namespace geo_join