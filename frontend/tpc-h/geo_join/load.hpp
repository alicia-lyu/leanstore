#pragma once

#include <cstddef>
#include "../randutils.hpp"

namespace geo_join
{
struct params {
   static constexpr size_t STATE_MAX = 80;   // in a nation
   static constexpr size_t COUNTY_MAX = 50;  // in a state
   static constexpr size_t CITY_MAX = 20;    // in a county
   static int getStateKey() { return randutils::urand(1, STATE_MAX); }

   static int getCountyKey() { return randutils::urand(1, COUNTY_MAX); }

   static int getCityKey() { return randutils::urand(1, CITY_MAX); }
};
}  // namespace geo_join
