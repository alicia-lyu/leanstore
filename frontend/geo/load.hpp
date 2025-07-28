#pragma once

#include <gflags/gflags.h>
#include "../shared/randutils.hpp"
#include "../tpc-h/workload.hpp"
#include "views.hpp"

// 1 nation + 1--80 states --- 1--6 pages
// 1 county + 1--4 cities + 0--8 customers --- 1 page

namespace geo_join
{
// state controller for active loading process
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

   void advance_customers_in_1city(const size_t step_cnt, int n, int s, int c, int ci)
   {
      size_t customer_end = customer_idx + step_cnt;
      if (customer_end >= custkeys.size() && customer_idx < custkeys.size()) {
         std::cout << "WARNING: No customer since nation " << n << ", state " << s << ", county " << c << ", city " << ci << ". " << std::endl;
      }
      for (; customer_idx < custkeys.size() && customer_idx < customer_end; customer_idx++) {
         insert_customer_func(n, s, c, ci, custkeys.at(customer_idx), false);  // do not insert view
      }
   }

   void advance_customers_to_hot_cities()
   {
      assert(!hot_city_candidates.empty());
      assert(customer_idx < custkeys.size());  // create hot keys
      std::cout << "Assigning " << custkeys.size() - customer_idx << " remaining customers in " << hot_city_candidates.size() << " hot cities..."
                << std::endl;
      // No need to shuffle hot cities because custkeys are already shuffled
      for (; customer_idx < custkeys.size(); customer_idx++) {
         city_t::Key cik = hot_city_candidates.at(customer_idx % hot_city_candidates.size());
         insert_customer_func(cik.nationkey, cik.statekey, cik.countykey, cik.citykey, custkeys.at(customer_idx), true);  // insert view
         if (FLAGS_log_progress && customer_idx % 1000 == 0) {
            std::cout << "\rPending " << custkeys.size() - customer_idx << " customers to hot cities.";
         }
      }
   }
};
struct Params {
   int nation_count;
   int state_max;     // in a nation
   int county_max;    // in a state
   int city_max;      // in a county
   int customer_max;  // in a city

   const int nation_multiplier = std::min(FLAGS_tpch_scale_factor, 1);
   const int county_multiplier = std::min(FLAGS_tpch_scale_factor / nation_multiplier, 10);
   const double city_multiplier = (double)FLAGS_tpch_scale_factor / (county_multiplier * nation_multiplier);

   Params()
   {
      nation_count = 25 * nation_multiplier;
      state_max = 60;
      county_max = 20 * county_multiplier;
      city_max = std::floor(4 * city_multiplier);
      customer_max = 2;
      std::cout << "params: nation_count = " << nation_count << ", state_max = " << state_max << ", county_max = " << county_max
                << ", city_max = " << city_max << ", customer_max = " << customer_max << std::endl;
   }

   Params(int nation_count, int state_max, int county_max, int city_max, int customer_max)
       : nation_count(nation_count), state_max(state_max), county_max(county_max), city_max(city_max), customer_max(customer_max)
   {
   }

   int get_state_cnt() { return randutils::urand(1, state_max); }

   int get_county_cnt() { return randutils::urand(1, county_max); }

   int get_city_cnt() { return randutils::urand(1, city_max); }

   int get_customer_cnt()  // avg: 1.5
   {
      return randutils::urand(0, customer_max);
   }

   int get_nationkey() { return randutils::urand(1, nation_count); }
   int get_statekey() { return randutils::urand(1, state_max / 2); }
   int get_countykey() { return randutils::urand(1, county_max / 2); }
   int get_citykey() { return randutils::urand(1, city_max / 2); }
};
}  // namespace geo_join