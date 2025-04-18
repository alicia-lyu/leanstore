#include "experiment_template.hpp"
#include "views.hpp"

using namespace leanstore;
using namespace basic_group;

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Join TPC-H");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   return run<merged_count_partsupp_t, merged_sum_supplycost_t, merged_partsupp_t>();
}
