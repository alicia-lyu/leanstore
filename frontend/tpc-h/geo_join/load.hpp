#pragma once

#include <cstddef>
#include "../randutils.hpp"

namespace geo_join
{
struct params {
   static constexpr size_t STATE_MAX = 80;    // in a nation
   static constexpr size_t COUNTY_MAX = 200;  // in a state
   static constexpr size_t CITY_MAX = 5;      // in a county

   static int get_state_cnt() { return randutils::urand(1, STATE_MAX); }

   static int get_county_cnt() { return randutils::urand(1, COUNTY_MAX); }

   static int get_city_cnt()
   {
      int lottery = randutils::urand(1, 3);
      switch (lottery) {
         case 1:
            return 0;
         case 2:
            return 1;
         default:
            return randutils::urand(2, CITY_MAX);
      }
   }

   static int get_statekey() { return randutils::urand(1, STATE_MAX / 2); }
   static int get_countykey() { return randutils::urand(1, COUNTY_MAX / 2); }
   static int get_citykey() { return randutils::urand(1, CITY_MAX / 2); }
};
}  // namespace geo_join
