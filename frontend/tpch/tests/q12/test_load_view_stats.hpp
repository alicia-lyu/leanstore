#pragma once
// View stats for the Q12 standalone load test.
//
// Walks the q12_pipeline_view_t (joined_ol_t) adapter, counts rows and samples
// the orderkey range. Used by test_load_q12_{rocksdb,leanstore}.cpp to verify
// that populate_q12_view produced the same content as MI[0].
//
// compare_mi_and_view cross-checks cardinality and orderkey range against
// the MI[0] stats returned by dump_merged_ol_stats, printing [OK]/[FAIL] tags.

#include <algorithm>
#include <iostream>
#include <limits>

#include "../test_load_merged_stats.hpp"
#include "../../q12/workload.hpp"

namespace tpch::q12
{

struct ViewStats {
   long    row_count    = 0;
   Integer min_orderkey = std::numeric_limits<Integer>::max();
   Integer max_orderkey = std::numeric_limits<Integer>::min();
   double  size_mib     = 0.0;
};

// Walk the pipeline view adapter and collect stats. Also prints a summary.
template <typename Backend>
ViewStats dump_view_stats(
    typename Backend::template Adapter<q12_pipeline_view_t>& view)
{
   ViewStats stats;
   auto scanner = view.getScanner();
   while (auto kv = scanner->next()) {
      stats.row_count++;
      // After G8d the view key is q12_pipeline_view_t::Key{o_orderkey,
      // l_linenumber}; orderkey sits directly in the key.
      const Integer ok = kv->first.o_orderkey;
      stats.min_orderkey = std::min(stats.min_orderkey, ok);
      stats.max_orderkey = std::max(stats.max_orderkey, ok);
   }
   stats.size_mib = view.size();

   std::cout << "\n=== Q12 pipeline view stats ===\n";
   std::cout << "       row count:     " << stats.row_count << "\n";
   std::cout << "       orderkey range: [" << stats.min_orderkey
             << ", " << stats.max_orderkey << "]\n";
   std::cout << "       view size:     " << stats.size_mib << " MiB\n";

   return stats;
}

// Cross-check: view row count must equal MI[0] lineitem count (each joined
// (orders, lineitem) pair produces exactly one view row == one lineitem).
// Orderkey ranges must also agree.
inline void compare_mi_and_view(
    const ViewStats& v,
    const ::tpch::MergedOlStats& mi)
{
   auto pass = [](bool ok) { return ok ? "[OK]  " : "[FAIL]"; };

   std::cout << "\n=== MI[0] vs pipeline view cross-check ===\n";
   std::cout << pass(v.row_count == mi.lineitems_count)
             << " view rows == MI lineitem rows: "
             << v.row_count << " vs " << mi.lineitems_count << "\n";
   std::cout << pass(v.min_orderkey == mi.min_orderkey)
             << " min orderkey agree: "
             << v.min_orderkey << " vs " << mi.min_orderkey << "\n";
   std::cout << pass(v.max_orderkey == mi.max_orderkey)
             << " max orderkey agree: "
             << v.max_orderkey << " vs " << mi.max_orderkey << "\n";
   // mi.size_mib is the MI[0] approximate size (MergedOlStats::size_mib).
   // View rows carry both ORDERS and LINEITEM payloads so view size should be
   // in the same order of magnitude as MI[0]. Bounds are loose to accommodate
   // RocksDB approximation slack and encoding differences.
   const double lower = 0.3 * mi.size_mib;
   const double upper = 5.0 * mi.size_mib;
   const bool size_ok = v.size_mib >= lower && v.size_mib <= upper;
   std::cout << pass(size_ok)
             << " view size in plausible range vs MI: "
             << v.size_mib << " MiB (expected "
             << lower << "-" << upper << " MiB)\n";
}

}  // namespace tpch::q12
