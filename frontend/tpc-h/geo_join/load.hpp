#pragma once

#include "../randutils.hpp"

// 1 nation + 1--80 states --- 1--6 pages
// 1 county + 1--4 cities + 0--8 customers --- 1 page

namespace geo_join
{
struct params {
   static constexpr int STATE_MAX = 80;    // in a nation
   static constexpr int COUNTY_MAX = 200;  // in a state
   static constexpr int CITY_MAX = 4;      // in a county

   static int get_state_cnt() { return randutils::urand(1, STATE_MAX); }

   static int get_county_cnt() { return randutils::urand(1, COUNTY_MAX); }

   static int get_city_cnt()
   {
      return randutils::urand(1, CITY_MAX);
   }

   static int get_customer_cnt() // avg: 1.5
   {
      // int lottery = randutils::urand(1, 3);
      // if (lottery <= 2) return 0;
      // else return randutils::urand(0, 12);
      return randutils::urand(0, 2);
      // some hot cities emerge during the final assignment
   }

   static int get_statekey() { return randutils::urand(1, STATE_MAX / 2); }
   static int get_countykey() { return randutils::urand(1, COUNTY_MAX / 2); }
   static int get_citykey() { return randutils::urand(1, CITY_MAX / 2); }
};
}  // namespace geo_join
