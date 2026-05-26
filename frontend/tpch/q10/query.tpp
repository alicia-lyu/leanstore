// Template method bodies for Q10Workload<Backend> query methods,
// Params::defaults(), q10_pipeline_view_t::print(), q10_agg_row_t::print(),
// and predicate implementations.
// Operator-translation reference: see ../OPERATORS.md §3.
//
// Phase 1: all query_by_* return empty results (XOR digest = 0x0).
// Real bodies land in Phase 4 §7.1 (S3), §7.2 (S1), §7.3 (S2), §7.5 (S4).

#pragma once

#include <gflags/gflags.h>
#include <ostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../tpch_tables.hpp"
#include "../tpch_family/col_pipeline.hpp"
#include "../q10_family/admit.hpp"
#include "../q10_family/visitor.hpp"
#include "../q10_family/out_class.hpp"
#include "../q10_family/acol_walk.tpp"

DECLARE_int32(storage_structure);
DECLARE_string(q10_view_variant);

namespace tpch::q10
{

// ---------------------------------------------------------------------------
// Q10 SUBSTITUTION-PARAMETER rotation table (Decision E4 — 24 entries).
//
// TPC-H §2.4.10 domain for `:d`:
//   first day of a month between 1993-02-01 and 1995-01-01 inclusive
//   (24 valid month starts).  Validation default = 1993-10-01.
//
// Days since 1970-01-01.  DATE_1994_01_01 = 8766 and DATE_1995_01_01 = 9131
// ship in tpch_tables.hpp.  1993-01-01 = DATE_1994_01_01 − 365.  Year 1993,
// 1994, 1995 are all non-leap, so month offsets follow the standard
// Jan(31)/Feb(28)/Mar(31)/Apr(30)/May(31)/Jun(30)/Jul(31)/Aug(31)/
// Sep(30)/Oct(31)/Nov(30)/Dec(31) cumulative pattern.
//
// Validation default first so iter=0 is always spec-valid.  The PLAYBOOK
// rationale (§3.6): a fixed-param loop hid Q3I's pre_revenue bug for weeks;
// domain-full rotation surfaces param-bake regressions within one helper.run().

static constexpr Timestamp Q10_DATE_1993_01_01 = DATE_1994_01_01 - 365;  // 8401

static constexpr Timestamp PARAM_TABLE[] = {
    Q10_DATE_1993_01_01 + 273,  // 1993-10-01 = 8674  [validation default]
    Q10_DATE_1993_01_01 +  31,  // 1993-02-01
    Q10_DATE_1993_01_01 +  59,  // 1993-03-01
    Q10_DATE_1993_01_01 +  90,  // 1993-04-01
    Q10_DATE_1993_01_01 + 120,  // 1993-05-01
    Q10_DATE_1993_01_01 + 151,  // 1993-06-01
    Q10_DATE_1993_01_01 + 181,  // 1993-07-01
    Q10_DATE_1993_01_01 + 212,  // 1993-08-01
    Q10_DATE_1993_01_01 + 243,  // 1993-09-01
    Q10_DATE_1993_01_01 + 304,  // 1993-11-01
    Q10_DATE_1993_01_01 + 334,  // 1993-12-01
    DATE_1994_01_01,            // 1994-01-01 = 8766
    DATE_1994_01_01 +  31,      // 1994-02-01
    DATE_1994_01_01 +  59,      // 1994-03-01
    DATE_1994_01_01 +  90,      // 1994-04-01
    DATE_1994_01_01 + 120,      // 1994-05-01
    DATE_1994_01_01 + 151,      // 1994-06-01
    DATE_1994_01_01 + 181,      // 1994-07-01
    DATE_1994_01_01 + 212,      // 1994-08-01
    DATE_1994_01_01 + 243,      // 1994-09-01
    DATE_1994_01_01 + 273,      // 1994-10-01
    DATE_1994_01_01 + 304,      // 1994-11-01
    DATE_1994_01_01 + 334,      // 1994-12-01
    DATE_1995_01_01,            // 1995-01-01 = 9131
};

static constexpr long PARAM_TABLE_SIZE =
    static_cast<long>(sizeof(PARAM_TABLE) / sizeof(PARAM_TABLE[0]));
static_assert(PARAM_TABLE_SIZE == 24,
              "Q10 PARAM_TABLE must cover all 24 valid month starts in [1993-02-01, 1995-01-01]");

// ---------------------------------------------------------------------------

inline Params Params::defaults()
{
   return Params{PARAM_TABLE[0]};
}

// Rotate through the substitution-parameter table so each TX iteration
// exercises a distinct date.  Surfaces param-bake regressions that a
// fixed-default loop would miss (PLAYBOOK §3.6 rationale).
template <typename Backend>
void Q10Workload<Backend>::set_params_for_iter(long iter)
{
   params.date_lo = PARAM_TABLE[iter % PARAM_TABLE_SIZE];
}

// ---------------------------------------------------------------------------
// Predicate implementations.

inline bool q10_predicate_orders(const orders_t& o, const Params& p)
{
   // o_orderdate ∈ [date_lo, date_lo + 3 months).
   // 3 months ≈ 90 days for the spec's monthly grid; consistent with how
   // Q3 / Q5 lower their date-window predicates.
   return o.o_orderdate >= p.date_lo
       && o.o_orderdate <  p.date_lo + 90;
}

inline bool q10_predicate_lineitem(const lineitem_t& l, const Params& /*p*/)
{
   // l_returnflag = 'R' (spec-hardcoded; kept live for view reusability).
   return l.l_returnflag.data[0] == 'R';
}

// ---------------------------------------------------------------------------
// Output formatting.

inline void q10_pipeline_view_t::print(std::ostream& os) const
{
   os << l_extendedprice << '\t' << l_discount << '\t'
      << l_returnflag    << '\t' << o_orderdate << '\t'
      << c_name          << '\t' << c_nationkey << '\n';
}

inline void q10_pipeline_view_preagg_t::print(std::ostream& os) const
{
   os << returned_revenue << '\t' << o_orderdate << '\t'
      << c_name           << '\t' << c_nationkey << '\n';
}

inline void q10_agg_row_t::print(std::ostream& os) const
{
   os << c_custkey << '\t' << c_name      << '\t' << revenue   << '\t'
      << c_acctbal << '\t' << n_name      << '\t' << c_address << '\t'
      << c_phone   << '\t' << c_comment   << '\n';
}

// ---------------------------------------------------------------------------
// Stub query bodies — Phase 1 commit 1.
// Real bodies land in Phase 4 §7.1/§7.2/§7.3/§7.5.

template <typename Backend>
long Q10Workload<Backend>::query_by_base(std::vector<q10_agg_row_t>& out)
{
   // S1: traditional indexes over the custkey-sorted COL split secondaries
   // (D7 — reused verbatim from Q5). The plan calls for a 2-BMJ chain;
   // we implement the equivalent shape as a nested forward walk: outer
   // scan over CUSTOMER (custkey-sorted by PK), per-customer seek into
   // split_orders[custkey, ...] applying the orderdate window, per-order
   // seek into split_lineitem[custkey, orderkey, ...] applying the
   // returnflag filter. Each surviving lineitem feeds the per-customer
   // aggregator via q10_admit_lineitem_from_join; NATION INL fires per
   // surviving customer at finalize (Decision D6). Functionally identical
   // to BMJ for correctness purposes — same access pattern, same emit
   // cardinality, same XOR digest as S3 by construction.
   out.clear();

   Q10QuerySink sink(stats);
   Q10PerCustomerAggregator agg{};
   agg.stats = stats;
   std::unordered_map<Integer, Varchar<25>> nation_cache;

   auto cust_sc = customer.getScanner();
   auto ord_sc  = col.split_orders().getScanner();
   auto lin_sc  = col.split_lineitem().getScanner();

   while (auto ckv = cust_sc->next()) {
      if (stats) stats->customers_scanned++;
      const Integer       custkey = ckv->first.c_custkey;
      const customerh_t&  c       = ckv->second;

      ord_sc->seek(orders_coli_t::Key{custkey, 0});
      while (auto okv = ord_sc->next()) {
         if (okv->first.custkey != custkey) break;
         if (stats) stats->orders_scanned++;
         if (okv->second.o_orderdate < params.date_lo
             || okv->second.o_orderdate >= params.date_lo + 90) {
            continue;
         }
         if (stats) stats->orders_passing_date++;
         const Integer orderkey = okv->first.orderkey;

         lin_sc->seek(lineitem_col_t::Key{custkey, orderkey, 0});
         while (auto lkv = lin_sc->next()) {
            if (lkv->first.custkey != custkey
                || lkv->first.orderkey != orderkey) break;
            if (stats) stats->lineitems_scanned++;
            if (lkv->second.l_returnflag.data[0] != 'R') continue;
            if (stats) stats->lineitems_passing_returnflag++;
            q10_admit_lineitem_from_join(custkey, c,
                                         lkv->second.l_extendedprice,
                                         lkv->second.l_discount,
                                         agg, stats);
         }
      }
   }

   q10_finalize_aggregator(agg, nation, nation_cache, sink, stats);
   sink.finalize(out);
   return static_cast<long>(out.size());
}

// S2/S6 shared per-lineitem (variant-A) scan core with the D4 anomaly chain:
//
//   view scan → returnflag filter → SUM-per-orderkey
//   → Filter[orderdate ∈ window] → SUM-per-c_custkey → NATION INL → TopN
//
// Templated on the scanner type so it runs over BOTH the per-query
// q10_pipeline_view_t adapter (S2) and the COL-family shared col_shared_view_t
// adapter (S6) — the union row is a column superset, so the body reads its
// subset (l_returnflag, o_orderdate, l_extendedprice, l_discount, the 6 FD
// customer cols, c_nationkey) unchanged. NATION is resolved post-pipeline via
// per-customer INL (D6) for both — q10 never stores n_name, so S6 needs no
// special handling here. Anti-pattern #30 bound: |per-orderkey map| ≤ |ORDERS|.
template <typename Scanner, typename NationAdapter>
static long q10_run_view_scan_lineitem(Scanner& scanner, const Params& params,
                                       NationAdapter& nation, Q10Stats* stats,
                                       std::vector<q10_agg_row_t>& out)
{
   out.clear();

   Q10QuerySink sink(stats);
   Q10PerCustomerAggregator agg{};
   agg.stats = stats;
   std::unordered_map<Integer, Varchar<25>> nation_cache;

   struct PerOrderState {
      Numeric                  sum_revenue   = 0;
      Timestamp                o_orderdate   = 0;
      Integer                  custkey       = 0;
      Q10PartialCustomerAgg    cust_snapshot{};
      bool                     order_seen    = false;
   };
   std::unordered_map<Integer, PerOrderState> per_orderkey;

   while (auto kv = scanner.next()) {
      const auto& k = kv->first;
      const auto& v = kv->second;
      if (stats) stats->view_rows_scanned++;
      if (stats) stats->lineitems_scanned++;

      // Per-lineitem returnflag filter (Q10's spec-hardcoded predicate;
      // kept live for view reusability per PLAYBOOK §3.5 step 7).
      if (v.l_returnflag.data[0] != 'R') continue;
      if (stats) stats->lineitems_passing_returnflag++;

      auto& st = per_orderkey[k.orderkey];
      if (!st.order_seen) {
         st.o_orderdate = v.o_orderdate;
         st.custkey     = k.custkey;
         st.cust_snapshot.c_name        = v.c_name;
         st.cust_snapshot.c_acctbal     = v.c_acctbal;
         st.cust_snapshot.c_nationkey   = v.c_nationkey;
         st.cust_snapshot.c_address     = v.c_address;
         st.cust_snapshot.c_phone       = v.c_phone;
         st.cust_snapshot.c_comment     = v.c_comment;
         st.cust_snapshot.customer_seen = true;
         st.order_seen = true;
      }
      st.sum_revenue += v.l_extendedprice * (Numeric{1} - v.l_discount);
   }

   // Drain the per-orderkey map, applying the orderdate filter at order
   // granularity (D4) and folding survivors into the per-customer
   // aggregator.
   for (auto& [orderkey, st] : per_orderkey) {
      if (stats) stats->orders_scanned++;
      if (st.o_orderdate < params.date_lo
          || st.o_orderdate >= params.date_lo + 90) continue;
      if (stats) stats->orders_passing_date++;
      q10_admit_revenue_from_join(st.custkey, st.cust_snapshot,
                                   st.sum_revenue, agg, stats);
   }

   q10_finalize_aggregator(agg, nation, nation_cache, sink, stats);
   sink.finalize(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q10Workload<Backend>::query_by_view(std::vector<q10_agg_row_t>& out)
{
   // S2 A/B dispatch (q10/PERFORMANCE.md). Variant B (per-order pre-aggregated
   // view) is the perf-investigation fix for the btree S2 regression; variant
   // A (the shared core) is the original per-lineitem view + D4 anomaly chain.
   if (FLAGS_q10_view_variant == "preagg") {
      return query_by_view_preagg(out);
   }
   auto sc = pipeline_view.getScanner();
   return q10_run_view_scan_lineitem(*sc, params, nation, stats, out);
}

template <typename Backend>
long Q10Workload<Backend>::query_by_shared_view(std::vector<q10_agg_row_t>& out)
{
   // S6: the per-lineitem variant-A body over the COL-family shared view
   // (col_shared_view_t). The shared union view is per-lineitem grain, so the
   // preagg variant B (q10_pipeline_view_preagg_t) is out of scope here; no
   // --q10_view_variant dispatch.
   auto sc = shared_view.getScanner();
   return q10_run_view_scan_lineitem(*sc, params, nation, stats, out);
}

template <typename Backend>
long Q10Workload<Backend>::query_by_view_preagg(std::vector<q10_agg_row_t>& out)
{
   // S2 variant B: per-order pre-aggregated view scan. The D4 anomaly is
   // gone — each view row IS one order carrying SUM(returned revenue) baked
   // at load, so there is no per-orderkey rollup and no per-row returnflag
   // filter. Chain collapses to:
   //
   //   view scan → Filter[orderdate ∈ window] → SUM-per-c_custkey
   //   → NATION INL → TopN
   //
   // The orderdate window is still applied live (parameterised; hoisted out
   // of the view payload). Reads ~|ORDERS-with-returns| narrow rows instead
   // of the full per-lineitem view, so it does not churn the buffer pool.
   out.clear();

   Q10QuerySink sink(stats);
   Q10PerCustomerAggregator agg{};
   agg.stats = stats;
   std::unordered_map<Integer, Varchar<25>> nation_cache;

   auto sc = pipeline_view_preagg.getScanner();
   while (auto kv = sc->next()) {
      const auto& k = kv->first;
      const auto& v = kv->second;
      if (stats) { stats->view_rows_scanned++; stats->orders_scanned++; }

      // Order-granular date window (the only live filter — returnflag is
      // baked into returned_revenue at load).
      if (v.o_orderdate < params.date_lo
          || v.o_orderdate >= params.date_lo + 90) continue;
      if (stats) stats->orders_passing_date++;

      // Build a one-shot FD snapshot from the view payload; q10_admit_revenue
      // adopts it on the customer's first surviving order and folds revenue.
      Q10PartialCustomerAgg snap{};
      snap.c_name        = v.c_name;
      snap.c_acctbal     = v.c_acctbal;
      snap.c_nationkey   = v.c_nationkey;
      snap.c_address     = v.c_address;
      snap.c_phone       = v.c_phone;
      snap.c_comment     = v.c_comment;
      snap.customer_seen = true;
      q10_admit_revenue_from_join(k.custkey, snap, v.returned_revenue,
                                   agg, stats);
   }

   q10_finalize_aggregator(agg, nation, nation_cache, sink, stats);
   sink.finalize(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q10Workload<Backend>::query_by_merged(std::vector<q10_agg_row_t>& out)
{
   // S3: col_group_walk over the 3-table COL MI using the bespoke
   // Q10GroupWalkVisitor (Decision D1; user-memory
   // `[[feedback_col_walk_is_shared_util]]` — NOT a Q3FamilyVisitor
   // subclass). Per-customer accumulator finalises at on_group_end
   // (D2); NATION INL on PK resolves n_name inline (D6); the
   // assembled row is offered to a bounded TopN(20) sink
   // (CONVENTIONS §Post-pipeline OutClass).
   out.clear();

   using NationAdapterT = typename Backend::template Adapter<nation_t>;
   Q10QuerySink sink(stats);
   std::unordered_map<Integer, Varchar<25>> nation_cache;  // ≤25 entries

   Q10GroupWalkVisitor<NationAdapterT, Q10QuerySink, Q10FilterMode::Query>
       visitor{params, nation, sink, nation_cache, stats};
   ::tpch::col_group_walk<Backend>(col.merged_adapter(), visitor);

   sink.finalize(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q10Workload<Backend>::query_by_hash(std::vector<q10_agg_row_t>& out)
{
   // S4: HashJoin chain over base tables only — PK-only builds + sorted-seek
   // lineitem probe + INL recovery at orderkey transitions (Decisions D5
   // + D6, CONVENTIONS Rules 4 / 13). Mirrors Q5 S4 with the per-customer
   // predicate dropped (Q10 has none — cust_set is just the full PK set).
   //
   //   (1) cust_set   = {c_custkey from CUSTOMER scan}     (Rule 4)
   //   (2) orders_set = {o_orderkey from ORDERS scan filtered by
   //                     orderdate window + o_custkey ∈ cust_set}
   //   (3) Sequential LINEITEM probe with seek-on-miss to {K+1, 0};
   //       per orderkey transition recover c_custkey via orders.lookup1
   //       (Rule 13) + customer record via customer.lookup1; cache
   //       across the ~4 lineitems sharing the order. Apply returnflag
   //       filter; feed q10_admit_lineitem_from_join. NATION INL on PK
   //       fires per surviving customer at finalize (D6).
   out.clear();

   Q10QuerySink sink(stats);
   Q10PerCustomerAggregator agg{};
   agg.stats = stats;
   std::unordered_map<Integer, Varchar<25>> nation_cache;

   // (1) cust_set — Q10 has no per-customer predicate, so this is just the
   // full c_custkey PK set (Rule 4: PK-only, no FD payload).
   std::unordered_set<Integer> cust_set;
   {
      auto sc = customer.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->customers_scanned++;
         cust_set.insert(kv->first.c_custkey);
      }
   }

   // (2) orders_set — orderdate window + cust_set membership; PK-only.
   std::unordered_set<Integer> orders_set;
   {
      auto sc = orders.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->orders_scanned++;
         const Timestamp od = kv->second.o_orderdate;
         if (od < params.date_lo || od >= params.date_lo + 90) continue;
         if (cust_set.find(kv->second.o_custkey) == cust_set.end()) continue;
         if (stats) stats->orders_passing_date++;
         orders_set.insert(kv->first.o_orderkey);
      }
   }

   // (3) Lineitem sorted-seek probe with per-orderkey INL recovery.
   {
      auto sc = lineitem.getScanner();
      Integer    cached_ok      = -1;
      Integer    cached_custkey = 0;
      customerh_t cached_cust{};

      sc->seek(typename lineitem_t::Key{1, 0});

      while (auto kv = sc->next()) {
         if (stats) stats->lineitems_scanned++;
         const Integer ok = kv->first.l_orderkey;

         if (orders_set.count(ok) == 0) {
            sc->seek(typename lineitem_t::Key{ok + 1, 0});
            continue;
         }

         if (ok != cached_ok) {
            // Per-orderkey-transition INL recovery (Rule 13). Both
            // lookups must succeed — orders_set membership guarantees
            // the ORDERS row exists, and cust_set membership above
            // guarantees the CUSTOMER row exists.
            Integer custkey = 0;
            orders.lookup1(typename orders_t::Key{ok},
                           [&](const orders_t& o) { custkey = o.o_custkey; });
            if (stats) ++stats->orders_inl_lookups;
            customer.lookup1(typename customerh_t::Key{custkey},
                             [&](const customerh_t& c) { cached_cust = c; });
            if (stats) ++stats->customer_inl_lookups;
            cached_ok      = ok;
            cached_custkey = custkey;
         }

         if (kv->second.l_returnflag.data[0] != 'R') continue;
         if (stats) stats->lineitems_passing_returnflag++;

         q10_admit_lineitem_from_join(cached_custkey, cached_cust,
                                      kv->second.l_extendedprice,
                                      kv->second.l_discount,
                                      agg, stats);
      }
   }

   q10_finalize_aggregator(agg, nation, nation_cache, sink, stats);
   sink.finalize(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q10Workload<Backend>::query_by_aggregated(std::vector<q10_agg_row_t>& out)
{
   // S5: hand-rolled acol_group_walk over the aCOL MI
   // (customer_coli_t + orders_acol_t). Per-order returned revenue is BAKED at
   // load, so there are no lineitems to read and no returnflag filter — the
   // walk snapshots the customer FD cols, sums the baked revenue of in-window
   // orders, and emits one row per custkey group (NATION INL at on_group_end,
   // bounded TopN(20)). The hand-rolled walker (fused raw-dispatch, no
   // std::variant) is the whole point: aCOLI's generic getScanner+std::visit
   // is why its S5 lost to S3 (q3i/PERFORMANCE.md). Same per-customer
   // finalisation as S3 (query_by_merged), minus the lineitem level.
   out.clear();

   using NationAdapterT = typename Backend::template Adapter<nation_t>;
   Q10QuerySink sink(stats);
   std::unordered_map<Integer, Varchar<25>> nation_cache;  // ≤25 entries

   Q10AcolVisitor<NationAdapterT, Q10QuerySink>
       visitor{params, nation, sink, nation_cache, stats};
   acol_group_walk<Backend>(acol, visitor);

   sink.finalize(out);
   return static_cast<long>(out.size());
}

}  // namespace tpch::q10
