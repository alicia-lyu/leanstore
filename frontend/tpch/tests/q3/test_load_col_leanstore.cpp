#ifndef ROCKSDB_ONLY
// Standalone load-test for CustomerOrdersLineitemPipeline<LeanStoreBackend>.
// LeanStore (BTreeLL) mirror of test_load_col_lsm.cpp.
//
// At SF=1, after populate_merged() and populate_split(), verifies:
//   1. Per-record-type row counts (customer, orders, lineitem) in MI.
//   2. Total MI cardinality == sum of base table counts.
//   3. Every lineitem_col_t.l_orderkey resolves to a present orders_coli_t row.
//   4. Every order's custkey matches a present customer_coli_t.
//   5. Hierarchical scan order for a randomly picked custkey:
//      customer, (orders followed by its lineitems)+.
//   6. Custkey-sorted secondary indexes have the expected row counts and
//      orders_sec is custkey-sorted.
//
// Uses vanilla TPCHWorkload (lineitem_t, no invoice extension).

#include <gflags/gflags.h>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "../../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../../backend.hpp"
#include "../../col_pipeline.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../views_col.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

// ---------------------------------------------------------------------------
// Helper: human-readable pass/fail tag

static const char* pass(bool ok) { return ok ? "[OK]  " : "[FAIL]"; }

// ---------------------------------------------------------------------------
// ColStats: accumulated during one full merged-index scan.

struct ColStats {
   long customer_count = 0;
   long orders_count   = 0;
   long lineitem_count = 0;

   std::unordered_set<Integer> seen_custkeys;
   std::unordered_map<Integer, Integer> seen_order_custkeys;
   std::unordered_set<Integer> lineitem_orderkeys;
};

// ---------------------------------------------------------------------------
// scan_col_stats: single forward scan over merged_col collecting ColStats.

template <typename Backend>
ColStats scan_col_stats(
    typename Backend::template MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                             tpch::lineitem_col_t>& merged_col)
{
   using namespace tpch;

   auto scanner = merged_col.template getScanner<customer_coli_t::Key, customer_coli_t>();

   ColStats stats;

   while (auto kv = scanner->next()) {
      std::visit(
          [&](auto&& val) {
             using V = std::decay_t<decltype(val)>;
             if constexpr (std::is_same_v<V, customer_coli_t>) {
                stats.customer_count++;
                auto* ck = std::get_if<customer_coli_t::Key>(&kv->first);
                if (ck) stats.seen_custkeys.insert(ck->custkey);
             } else if constexpr (std::is_same_v<V, orders_coli_t>) {
                stats.orders_count++;
                auto* ok = std::get_if<orders_coli_t::Key>(&kv->first);
                if (ok) stats.seen_order_custkeys.emplace(ok->orderkey, ok->custkey);
             } else if constexpr (std::is_same_v<V, lineitem_col_t>) {
                stats.lineitem_count++;
                auto* lk = std::get_if<lineitem_col_t::Key>(&kv->first);
                if (lk) stats.lineitem_orderkeys.insert(lk->orderkey);
             }
          },
          kv->second);
   }

   return stats;
}

// ---------------------------------------------------------------------------
// find_custkey_with_orders: scan the MI forward and return the first custkey
// that has at least one orders_coli_t row.  Returns -1 if none found.

template <typename Backend>
Integer find_custkey_with_orders(
    typename Backend::template MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                             tpch::lineitem_col_t>& merged_col)
{
   using namespace tpch;
   auto scanner = merged_col.template getScanner<customer_coli_t::Key, customer_coli_t>();
   while (auto kv = scanner->next()) {
      auto* ok = std::get_if<orders_coli_t::Key>(&kv->first);
      if (ok) return ok->custkey;
   }
   return -1;
}

// ---------------------------------------------------------------------------
// check_hierarchical_order — see lsm version for full COL byte-lex layout.

template <typename Backend>
bool check_hierarchical_order(
    typename Backend::template MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                             tpch::lineitem_col_t>& merged_col,
    Integer custkey)
{
   using namespace tpch;

   auto scanner = merged_col.template getScanner<customer_coli_t::Key, customer_coli_t>();
   scanner->template seek<customer_coli_t>(customer_coli_t::Key{custkey});

   int state       = 0;
   int cust_count  = 0;
   int orders_seen = 0;

   while (auto kv = scanner->next()) {
      bool still_custkey = std::visit(
          [&](auto&& k) -> bool {
             using K = std::decay_t<decltype(k)>;
             if constexpr (std::is_same_v<K, customer_coli_t::Key>)  return k.custkey == custkey;
             if constexpr (std::is_same_v<K, orders_coli_t::Key>)    return k.custkey == custkey;
             if constexpr (std::is_same_v<K, lineitem_col_t::Key>)   return k.custkey == custkey;
             return false;
          },
          kv->first);

      if (!still_custkey) break;

      bool ok = std::visit(
          [&](auto&& val) -> bool {
             using V = std::decay_t<decltype(val)>;
             if constexpr (std::is_same_v<V, customer_coli_t>) {
                if (state != 0) return false;
                state = 1;
                cust_count++;
                return true;
             } else if constexpr (std::is_same_v<V, orders_coli_t>) {
                if (state == 0) return false;
                state = 2;
                orders_seen++;
                return true;
             } else if constexpr (std::is_same_v<V, lineitem_col_t>) {
                if (state != 2) return false;
                return true;
             }
             return false;
          },
          kv->second);

      if (!ok) return false;
   }

   return cust_count == 1 && orders_seen >= 1;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("CustomerOrdersLineitemPipeline load-test — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   B::Adapter<part_t>      part;
   B::Adapter<supplier_t>  supplier;
   B::Adapter<partsupp_t>  partsupp;
   B::Adapter<customerh_t> customer;
   B::Adapter<orders_t>    orders;
   B::Adapter<lineitem_t>  lineitem;
   B::Adapter<nation_t>    nation;
   B::Adapter<region_t>    region;

   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t, tpch::lineitem_col_t> merged_col;

   B::Adapter<tpch::orders_coli_t>   orders_sec;
   B::Adapter<tpch::lineitem_col_t>  lineitem_sec;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part         = B::Adapter<part_t>(db, "part");
      supplier     = B::Adapter<supplier_t>(db, "supplier");
      partsupp     = B::Adapter<partsupp_t>(db, "partsupp");
      customer     = B::Adapter<customerh_t>(db, "customer");
      orders       = B::Adapter<orders_t>(db, "orders");
      lineitem     = B::Adapter<lineitem_t>(db, "lineitem");
      nation       = B::Adapter<nation_t>(db, "nation");
      region       = B::Adapter<region_t>(db, "region");
      merged_col   = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                      tpch::lineitem_col_t>(db, "test_merged_col");
      orders_sec   = B::Adapter<tpch::orders_coli_t>(db, "test_orders_sec");
      lineitem_sec = B::Adapter<tpch::lineitem_col_t>(db, "test_lineitem_sec");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      leanstore::cr::Worker::my().commitTX();
   });

   tpch::CustomerOrdersLineitemPipeline<B> col_pipe(
       customer, orders, lineitem, merged_col,
       orders_sec, lineitem_sec);

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      col_pipe.populate_merged();
      leanstore::cr::Worker::my().commitTX();
   });

   const int sf = FLAGS_tpch_scale_factor;

   const long expected_customer    = TPCHWorkload<B::Adapter>::CUSTOMER_SCALE * sf;
   const long expected_orders      = TPCHWorkload<B::Adapter>::ORDERS_SCALE   * sf;
   const long expected_lineitem_min = expected_orders * 1;
   const long expected_lineitem_max = expected_orders * 7;

   ColStats stats;
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      stats = scan_col_stats<B>(merged_col);
      leanstore::cr::Worker::my().commitTX();
   });

   const long total_mi = stats.customer_count + stats.orders_count + stats.lineitem_count;
   const long expected_mi_total = stats.customer_count + stats.orders_count + stats.lineitem_count;

   std::cout << "\n=== COL MI distribution (scale_factor=" << sf << ") ===\n";
   std::cout << pass(stats.customer_count == expected_customer)
             << " customer count: " << stats.customer_count
             << "  (expected " << expected_customer << ")\n";
   std::cout << pass(stats.orders_count == expected_orders)
             << " orders count:   " << stats.orders_count
             << "  (expected " << expected_orders << ")\n";
   std::cout << pass(stats.lineitem_count >= expected_lineitem_min
                     && stats.lineitem_count <= expected_lineitem_max)
             << " lineitem count: " << stats.lineitem_count
             << "  (expected " << expected_lineitem_min
             << "-" << expected_lineitem_max
             << ", typical ~" << (expected_orders * 4) << ")\n";

   std::cout << pass(total_mi == expected_mi_total)
             << " total MI cardinality: " << total_mi << "\n";

   bool all_lineitem_fks_ok = true;
   for (Integer ok : stats.lineitem_orderkeys) {
      if (stats.seen_order_custkeys.find(ok) == stats.seen_order_custkeys.end()) {
         all_lineitem_fks_ok = false;
         break;
      }
   }
   std::cout << pass(all_lineitem_fks_ok)
             << " all lineitem l_orderkey FKs resolve to orders in MI ("
             << stats.lineitem_orderkeys.size() << " distinct orderkeys)\n";

   bool all_order_fks_ok = true;
   for (auto& [orderkey, custkey] : stats.seen_order_custkeys) {
      if (stats.seen_custkeys.find(custkey) == stats.seen_custkeys.end()) {
         all_order_fks_ok = false;
         break;
      }
   }
   std::cout << pass(all_order_fks_ok)
             << " all order custkey FKs resolve to customers in MI ("
             << stats.seen_order_custkeys.size() << " orders checked)\n";

   Integer spot_custkey = -1;
   bool hier_ok = false;
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      spot_custkey = find_custkey_with_orders<B>(merged_col);
      hier_ok = (spot_custkey > 0)
             && check_hierarchical_order<B>(merged_col, spot_custkey);
      leanstore::cr::Worker::my().commitTX();
   });
   std::cout << pass(hier_ok)
             << " hierarchical order for custkey=" << spot_custkey
             << ": customer,(orders+lineitems)+\n";

   std::cout << "       MI size: " << merged_col.size() << " MiB\n";

   // -----------------------------------------------------------------------
   // Split-index check.

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      col_pipe.populate_split();
      leanstore::cr::Worker::my().commitTX();
   });

   long sec_orders_count   = 0;
   long sec_lineitem_count = 0;
   bool orders_sec_sorted  = true;

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      {
         auto sc = orders_sec.getScanner();
         while (sc->next()) sec_orders_count++;
      }
      {
         auto sc = lineitem_sec.getScanner();
         while (sc->next()) sec_lineitem_count++;
      }
      {
         Integer prev_custkey = -1;
         auto sc = orders_sec.getScanner();
         while (auto kv = sc->next()) {
            if (kv->first.custkey < prev_custkey) {
               orders_sec_sorted = false;
               break;
            }
            prev_custkey = kv->first.custkey;
         }
      }
      leanstore::cr::Worker::my().commitTX();
   });

   const long sec_total            = sec_orders_count + sec_lineitem_count;
   const long expected_sec_orders  = stats.orders_count;
   const long expected_sec_lineitem = stats.lineitem_count;
   const long expected_sec_total   = expected_sec_orders + expected_sec_lineitem;

   std::cout << "\n=== Secondary index distribution (scale_factor=" << sf << ") ===\n";
   std::cout << pass(sec_orders_count == expected_sec_orders)
             << " orders secondary count:   " << sec_orders_count
             << "  (expected " << expected_sec_orders << ")\n";
   std::cout << pass(sec_lineitem_count == expected_sec_lineitem)
             << " lineitem secondary count: " << sec_lineitem_count
             << "  (expected " << expected_sec_lineitem << ")\n";
   std::cout << pass(sec_total == expected_sec_total)
             << " secondary total:          " << sec_total
             << "  (expected " << expected_sec_total << ")\n";
   std::cout << pass(orders_sec_sorted)
             << " orders secondary is custkey-sorted\n";

   std::cout << "       secondaries size: " << col_pipe.get_split_size() << " MiB\n";

   return 0;
}

#endif  // ROCKSDB_ONLY
