// Standalone load-test for CustomerOrdersLineitemPipeline<RocksDBBackend>.
//
// At SF=1, after populate_merged(), verifies:
//   1. Per-record-type row counts (customer, orders, lineitem).
//   2. Total MI cardinality == sum of base table counts.
//   3. Every lineitem_col_t.l_orderkey resolves to a present orders_coli_t row.
//   4. Every order's custkey matches a present customer_coli_t.
//   5. Hierarchical scan order for a randomly picked custkey:
//      customer, (orders followed by its lineitems)+.
//
// Uses vanilla TPCHWorkload (lineitem_t, no invoice extension).

#include <gflags/gflags.h>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../tpch_family/col_pipeline.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../tpch_family/views_col.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// Helper: human-readable pass/fail tag

static const char* pass(bool ok) { return ok ? "[OK]  " : "[FAIL]"; }

// ---------------------------------------------------------------------------
// ColStats: accumulated during one full merged-index scan.

struct ColStats {
   long customer_count = 0;
   long orders_count   = 0;
   long lineitem_count = 0;

   // For FK checks: set of custkeys seen as customer_coli_t rows.
   std::unordered_set<Integer> seen_custkeys;
   // For FK checks: map from orderkey → custkey seen as orders_coli_t rows.
   std::unordered_map<Integer, Integer> seen_order_custkeys;
   // orderkeys referenced by lineitem rows.
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
// check_hierarchical_order: scan the custkey range for a specific custkey and
// verify the byte-lex sequence matches the COL tagged-key encoding.
//
// COL byte-lex order for a given custkey:
//   1. customer_coli_t  — key: [t=1(cust)][custkey][t=0(idx)][idx=0]
//      Sorts FIRST: sentinel (t=0) < domain tags (3, 4).
//   2. Per orderkey group (orderkeys in ascending order):
//      - orders_coli_t  — key: [1][custkey][3(ord)][orderkey][0][idx=1]
//        Parent order sorts BEFORE its lineitems (sentinel 0 < lineitem tag 4).
//      - lineitem_col_t rows — key: [1][custkey][3][orderkey][4(li)][linenumber][0][idx=36]
//
// Expected invariants:
//   - Exactly one customer record.
//   - At least one orderkey group (each group = one order followed by its lineitems).
//   - No customer record after the first order.

template <typename Backend>
bool check_hierarchical_order(
    typename Backend::template MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                             tpch::lineitem_col_t>& merged_col,
    Integer custkey)
{
   using namespace tpch;

   auto scanner = merged_col.template getScanner<customer_coli_t::Key, customer_coli_t>();

   // Seek to the customer record for this custkey.
   scanner->template seek<customer_coli_t>(customer_coli_t::Key{custkey});

   // State: 0=before customer, 1=after customer (orders allowed), 2=inside orders+lineitems.
   int state       = 0;
   int cust_count  = 0;
   int orders_seen = 0;

   while (auto kv = scanner->next()) {
      // Stop when we leave this custkey's range.
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
                if (state != 0) return false;  // customer must be first
                state = 1;
                cust_count++;
                return true;
             } else if constexpr (std::is_same_v<V, orders_coli_t>) {
                // Orders follow customer; transition to state 2 on first order.
                if (state == 0) return false;
                state = 2;
                orders_seen++;
                return true;
             } else if constexpr (std::is_same_v<V, lineitem_col_t>) {
                // Lineitems follow their parent order; must be in state 2.
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
   gflags::SetUsageMessage("CustomerOrdersLineitemPipeline load-test — RocksDB");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // All 8 TPC-H base tables (TPCHWorkload requires them all).
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // The COL merged index under test.
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t> merged_col(rocks_db);

   // S1 custkey-sorted secondary indexes under test.
   B::Adapter<tpch::orders_coli_t>   orders_sec(rocks_db);
   B::Adapter<tpch::lineitem_col_t>  lineitem_sec(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   // Vanilla TPCHWorkload: lineitem_t (no invoice extension).
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch.load();

   tpch::CustomerOrdersLineitemPipeline<B> col_pipe(
       customer, orders, lineitem, merged_col,
       orders_sec, lineitem_sec);
   col_pipe.populate_merged();

   // -----------------------------------------------------------------------
   // Scan the merged index and collect stats

   const int sf = FLAGS_tpch_scale_factor;

   const long expected_customer    = TPCHWorkload<B::Adapter>::CUSTOMER_SCALE * sf;
   const long expected_orders      = TPCHWorkload<B::Adapter>::ORDERS_SCALE   * sf;
   const long expected_lineitem_min = expected_orders * 1;
   const long expected_lineitem_max = expected_orders * 7;

   ColStats stats = scan_col_stats<B>(merged_col);

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

   // Total MI cardinality check.
   std::cout << pass(total_mi == expected_mi_total)
             << " total MI cardinality: " << total_mi << "\n";

   // FK check: every lineitem_col_t.l_orderkey must resolve to an orders_coli_t row.
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

   // FK check: every order's custkey must resolve to a customer_coli_t row.
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

   // Hierarchical order spot-check.
   const Integer spot_custkey = find_custkey_with_orders<B>(merged_col);
   bool hier_ok = (spot_custkey > 0)
               && check_hierarchical_order<B>(merged_col, spot_custkey);
   std::cout << pass(hier_ok)
             << " hierarchical order for custkey=" << spot_custkey
             << ": customer,(orders+lineitems)+\n";

   std::cout << "       MI size: " << merged_col.size() << " MiB\n";

   // -----------------------------------------------------------------------
   // Split-index check: populate_split() and verify row counts.

   col_pipe.populate_split();

   long sec_orders_count   = 0;
   long sec_lineitem_count = 0;

   {
      auto sc = orders_sec.getScanner();
      while (sc->next()) sec_orders_count++;
   }
   {
      auto sc = lineitem_sec.getScanner();
      while (sc->next()) sec_lineitem_count++;
   }

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

   // Verify orders secondary is custkey-sorted.
   bool orders_sec_sorted = true;
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
   std::cout << pass(orders_sec_sorted)
             << " orders secondary is custkey-sorted\n";

   std::cout << "       secondaries size: " << col_pipe.get_split_size() << " MiB\n";

   return 0;
}
