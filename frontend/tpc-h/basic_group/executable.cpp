#include "experiment_template.hpp"
#include "views.hpp"

using namespace leanstore;
using namespace basic_group;

DEFINE_int32(tpch_scale_factor, 1000, "TPC-H scale factor");
DEFINE_int32(tx_seconds, 10, "Number of seconds to run each type of transactions");
DEFINE_int32(storage_structure, 0, "Storage structure: 0 for traditional indexes, 2 for merged indexes");
DEFINE_int32(warmup_seconds, 0, "Warmup seconds");


int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Join TPC-H");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   return run<merged_view_t, merged_partsupp_t>();
}
