#pragma once

// Q10 per-customer aggregator + admit helpers shared across S1 / S2 / S4.
//
// Used by:
//   - S1 query_by_base   (BMJ-like nested scan over COL split indexes)
//   - S2 query_by_view   (D4 chain: per-orderkey rollup then per-customer SUM)
//   - S4 query_by_hash   (PK-only HashJoin chain with INL recovery)
//
// S3 query_by_merged uses Q10QuerySink directly via Q10GroupWalkVisitor's
// per-group accumulator — it does not need the multi-customer hashmap because
// the COL walker finalises one group at a time.
//
// Shape: a per-`c_custkey` partial-aggregate bucket carrying revenue and the
// 7 FD output cols (snapshotted on first lineitem per custkey). NATION INL
// is deferred to q10_finalize_aggregator at emit time (Decision D6 —
// per-customer point lookup on NATION PK, cached per-query, ≤ 25 entries).
//
// Convention reference: CONVENTIONS Rule 4 (PK-only build payloads — Q10's
// aggregator carries FD cols rather than rebuilding them, because the FD
// cols are *output* not *join build payload*; the bucket is downstream of
// the join, not part of it), Rule 13 (INL at record-assembly — n_name).
//
// Q10-local per user-memory rule [[feedback_col_walk_is_shared_util]]:
// only col_group_walk is shared family code; per-query post-pipeline
// helpers stay under qN_family/.

#include <stdexcept>
#include <unordered_map>

#include "../tpch_tables.hpp"
#include "../q10/views.hpp"
#include "../q10/workload.hpp"
#include "out_class.hpp"

namespace tpch::q10
{

// Per-bucket partial aggregate. Snapshot of the 7 FD customer cols plus the
// running revenue sum.
struct Q10PartialCustomerAgg {
   Numeric      revenue       = 0;
   Varchar<25>  c_name        = {};
   Numeric      c_acctbal     = 0;
   Integer      c_nationkey   = 0;
   Varchar<40>  c_address     = {};
   Varchar<15>  c_phone       = {};
   Varchar<117> c_comment     = {};
   bool         customer_seen = false;
};

struct Q10PerCustomerAggregator {
   std::unordered_map<Integer, Q10PartialCustomerAgg> buckets;
   Q10Stats* stats = nullptr;
};

// Snapshot the 7 FD customer cols into a bucket. Called once per
// first-lineitem-of-customer in S1 / S4; called when initialising the bucket
// from an S2 view row's FD payload in S2.
inline void q10_snapshot_customer(Q10PartialCustomerAgg& b, const customerh_t& c)
{
   b.c_name        = c.c_name;
   b.c_acctbal     = c.c_acctbal;
   b.c_nationkey   = c.c_nationkey;
   b.c_address     = c.c_address;
   b.c_phone       = c.c_phone;
   b.c_comment     = c.c_comment;
   b.customer_seen = true;
}

// S1 / S4 per-lineitem entry. Bucket is created on demand; FD cols are
// snapshotted on first sighting per c_custkey.
inline void q10_admit_lineitem_from_join(
    Integer c_custkey, const customerh_t& c,
    Numeric extprice, Numeric disc,
    Q10PerCustomerAggregator& agg, Q10Stats* /*stats*/)
{
   auto& b = agg.buckets[c_custkey];
   if (!b.customer_seen) q10_snapshot_customer(b, c);
   b.revenue += extprice * (Numeric{1} - disc);
}

// S2 per-order entry. The caller has already rolled lineitem revenue for
// the order; the FD customer snapshot is supplied directly (built once at
// the order's first lineitem during the view scan).
inline void q10_admit_revenue_from_join(
    Integer c_custkey,
    const Q10PartialCustomerAgg& cust_snapshot,
    Numeric pre_rolled_revenue,
    Q10PerCustomerAggregator& agg, Q10Stats* /*stats*/)
{
   auto& b = agg.buckets[c_custkey];
   if (!b.customer_seen) {
      b = cust_snapshot;
      b.revenue = 0;
      b.customer_seen = true;
   }
   b.revenue += pre_rolled_revenue;
}

// Drain buckets → per-customer NATION INL on PK (D6, cached) → build
// q10_agg_row_t → offer to sink. nation_cache is caller-owned so the
// ≤25-entry map can outlive a single finalize call (cheap repeat hits
// across multiple query invocations within one harness run).
template <typename NationAdapter>
inline void q10_finalize_aggregator(
    Q10PerCustomerAggregator& agg,
    NationAdapter& nation,
    std::unordered_map<Integer, Varchar<25>>& nation_cache,
    Q10QuerySink& sink,
    Q10Stats* stats)
{
   for (auto& [custkey, b] : agg.buckets) {
      if (!b.customer_seen) {
         throw std::logic_error(
             "Q10 finalize: aggregator bucket without customer snapshot");
      }
      Varchar<25> n_name{};
      auto it = nation_cache.find(b.c_nationkey);
      if (it != nation_cache.end()) {
         n_name = it->second;
      } else {
         nation.lookup1(typename nation_t::Key{b.c_nationkey},
                        [&](const nation_t& n) { n_name = n.n_name; });
         nation_cache.emplace(b.c_nationkey, n_name);
         if (stats) ++stats->nation_inl_lookups;
      }
      q10_agg_row_t row{};
      row.c_custkey = custkey;
      row.revenue   = b.revenue;
      row.c_name    = b.c_name;
      row.c_acctbal = b.c_acctbal;
      row.n_name    = n_name;
      row.c_address = b.c_address;
      row.c_phone   = b.c_phone;
      row.c_comment = b.c_comment;
      sink.emit(row);
   }
   if (stats) {
      stats->aggregator_rows_out =
          static_cast<long>(agg.buckets.size());
   }
}

}  // namespace tpch::q10
