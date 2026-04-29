#pragma once
// Distribution stats for the standalone MI[0] load tests.
//
// Walks the merged ORDERS x LINEITEM index, counts each variant arm, samples
// orderkey range, and compares against TPCHWorkload's expected scale-derived
// counts. Used by test_load_merged_{rocksdb,leanstore}.cpp and by the Q12
// load-test (test_load_q12_*) which feeds the returned struct into the
// cross-check in test_load_view_stats.hpp.
//
// Reports a "[FAIL]" / "[OK]" tag per check so the eye catches errors at a
// glance without staring at raw numbers.

#include <gflags/gflags.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>
#include <variant>

#include "tpch_tables.hpp"
#include "views_ol.hpp"

DECLARE_int32(tpch_scale_factor);

namespace tpch
{

struct MergedOlStats {
   long    orders_count   = 0;
   long    lineitems_count = 0;
   Integer min_orderkey   = std::numeric_limits<Integer>::max();
   Integer max_orderkey   = std::numeric_limits<Integer>::min();
   double  size_mib       = 0.0;
};

// expected_orders_count: number of orders rows expected (= ORDERS_SCALE * SF).
// expected_max_orderkey:  the largest sparse orderkey actually inserted, taken
//                         from TPCHWorkload::last_order_id. Per TPC-H spec §4.2.3,
//                         O_ORDERKEY is sparse (8 of every 32 keys are populated)
//                         so max_orderkey != expected_orders_count.
template <typename Backend>
MergedOlStats dump_merged_ol_stats(
    typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol,
    Integer expected_orders_count,
    Integer expected_max_orderkey)
{
   auto scanner = merged_ol.template getScanner<ol_sort_key_t, joined_ol_t>();

   MergedOlStats stats;
   long max_lineitems_per_order = 0;
   long current_lineitems = 0;
   Integer current_orderkey = -1;

   while (auto kv = scanner->next()) {
      std::visit(
          [&](auto&& key) {
             using K = std::decay_t<decltype(key)>;
             if constexpr (std::is_same_v<K, orders_t::Key>) {
                if (current_orderkey != -1) {
                   max_lineitems_per_order = std::max(max_lineitems_per_order, current_lineitems);
                }
                stats.orders_count++;
                current_orderkey = key.o_orderkey;
                current_lineitems = 0;
                stats.min_orderkey = std::min(stats.min_orderkey, key.o_orderkey);
                stats.max_orderkey = std::max(stats.max_orderkey, key.o_orderkey);
             } else if constexpr (std::is_same_v<K, lineitem_t::Key>) {
                stats.lineitems_count++;
                current_lineitems++;
             }
          },
          kv->first);
   }
   max_lineitems_per_order = std::max(max_lineitems_per_order, current_lineitems);
   stats.size_mib = merged_ol.size();

   const long total = stats.orders_count + stats.lineitems_count;
   const double avg_li_per_order =
       stats.orders_count > 0
           ? static_cast<double>(stats.lineitems_count) / stats.orders_count
           : 0.0;

   auto pass = [](bool ok) { return ok ? "[OK]  " : "[FAIL]"; };

   const long expected_li_min = expected_orders_count * 1;
   const long expected_li_max = expected_orders_count * 7;

   std::cout << "\n=== MI[0] distribution (scale_factor=" << FLAGS_tpch_scale_factor
             << ") ===\n";
   std::cout << pass(stats.orders_count == expected_orders_count)
             << " orders count: " << stats.orders_count
             << "  (expected " << expected_orders_count << ")\n";
   std::cout << pass(stats.lineitems_count >= expected_li_min
                     && stats.lineitems_count <= expected_li_max)
             << " lineitems count: " << stats.lineitems_count
             << "  (expected " << expected_li_min << "-" << expected_li_max
             << ", typical ~" << (expected_orders_count * 4) << ")\n";
   std::cout << pass(total == stats.orders_count + stats.lineitems_count)
             << " total MI[0] keys: " << total << "\n";
   std::cout << pass(stats.max_orderkey == expected_max_orderkey)
             << " orderkey range: [" << stats.min_orderkey << ", " << stats.max_orderkey
             << "]  (expected max " << expected_max_orderkey
             << " from TPC-H sparse keys)\n";
   std::cout << pass(avg_li_per_order >= 1.0 && avg_li_per_order <= 7.0)
             << " avg lineitems/order: " << std::fixed << std::setprecision(2)
             << avg_li_per_order << "  (expected 1-7, typical ~4)\n";
   std::cout << "       max lineitems for any single order: " << max_lineitems_per_order
             << "  (note: only meaningful if every lineitem has a matching order)\n";
   std::cout << "       MI[0] size: " << stats.size_mib << " MiB\n";

   return stats;
}

}  // namespace tpch
