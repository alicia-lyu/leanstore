#include "load.hpp"
#include <gflags/gflags.h>

namespace geo_join
{
int params::STATE_MAX = 80;
int params::COUNTY_MAX = 200;
int params::CITY_MAX = 4;
int params::CUSTOMER_MAX = 2;  // for average non-hot cities
};  // namespace geo_join