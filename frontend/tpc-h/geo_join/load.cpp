#include "load.hpp"
#include <gflags/gflags.h>
#include "../tpch_workload.hpp"

namespace geo_join
{
int params::STATE_MAX = 80;
int county_scale_factor = FLAGS_tpch_scale_factor / std::min(FLAGS_tpch_scale_factor, 5);  // nation scale factor
int params::COUNTY_MAX = 20 * county_scale_factor;
double city_scale_factor = (double) county_scale_factor / std::min(county_scale_factor, 10);  // scale from state
int params::CITY_MAX = 4 * std::floor(city_scale_factor);
int params::CUSTOMER_MAX = 2;  // for average non-hot cities
};  // namespace geo_join