// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 Filter Pushdown            — predicate hoisting / short-circuit rules
//   §7 (comparison-integrity)     — read before changing join strategy
//
// Template method bodies for Q3IWorkload<Backend> query methods,
// Params::defaults(), q3i_agg_row_t::print(), and predicate implementations.

#pragma once

#include <chrono>
#include <ostream>
#include <string_view>
#include <unordered_map>

#include "../operators.hpp"
#include "../q3_family/accumulators.hpp"
#include "../q3_family/coli_visitors.hpp"
#include "../q3_family/lineitem_revenue_aggregator.hpp"
#include "../q3_family/params.hpp"
#include "../q3_family/predicates.hpp"
#include "../../shared/merge-join/binary_merge_join.hpp"
#include "../../shared/merge-join/hash_join.hpp"
#include "../../shared/scanner_helpers.hpp"

// Q3IPerfCapture<Backend>: per-query RAII counter accumulator. Resolves
// to PerfContextCapture (RocksDB PerfContext + IOStatsContext) on the
// RocksDB backend, and LeanStorePerfContextCapture
// (WorkerCounters dt_* + scanner_perf::iter_next_ns) on the LeanStore
// backend. See perf_context_capture.hpp for the mapping.
#include "perf_context_capture.hpp"

// micro_perf_stats(): returns stats if micro_perf instrumentation is enabled,
// nullptr otherwise. Using the workload's micro_perf flag (not FLAGS_micro_perf
// directly) avoids a tpch_flags.hpp include-order dependency in this .tpp.
#define MICRO_PERF_STATS(wl) ((wl).micro_perf ? (wl).stats : nullptr)

DECLARE_string(coli_walker_variant);
DECLARE_int32(use_seek_skip);

namespace tpch::q3i
{

// RAII stage timer: accumulates microseconds into a long counter.
// Use as: { StageTimer t(stats ? &stats->stage_us_join : nullptr); ... }
// The pointer-or-nullptr form means stat-disabled paths pay no cost.
struct StageTimer {
   long* acc;
   std::chrono::high_resolution_clock::time_point t0;
   explicit StageTimer(long* a) : acc(a), t0(std::chrono::high_resolution_clock::now()) {}
   ~StageTimer() {
      if (!acc) return;
      auto t1 = std::chrono::high_resolution_clock::now();
      *acc += std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
   }
};

// ---------------------------------------------------------------------------
// Params::defaults — Q3I SQL validation values.

inline Params Params::defaults()
{
   return {
       Varchar<10>("BUILDING"),  // c_mktsegment filter
       DATE_1995_03_15,          // o_orderdate upper bound (orders before this date)
       DATE_1995_03_15,          // l_shipdate lower bound (lineitems shipped after this date)
       Numeric(0),               // minimum cust_open_due to include
   };
}

// ---------------------------------------------------------------------------
// Q3IWorkload::set_params_for_iter — rotate through TPC-H §2.4.3 / Q3I
// substitution params so each TX iteration exercises a distinct
// (segment, date) combination.  threshold is always 0 (spec validation
// default; non-zero thresholds shrink the result set and would make
// cross-structure cardinality comparison noisier without adding signal).
//
// SEGMENT domain: {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"}
// DATE domain:    March 15 of each year in [1993, 1997] (same date drives
//                 both the o_orderdate upper bound and the l_shipdate lower
//                 bound per §2.4.3).
//
// Date constants (days since 1970-01-01):
//   DATE_1995_03_15 = 9204  (defined in tpch_tables.hpp)
//   1993-03-15 = 9204 - 2*365     = 8474  (1994 and 1995 are not leap years)
//   1994-03-15 = 9204 - 365       = 8839
//   1996-03-15 = 9204 + 366       = 9570  (1996 is a leap year)
//   1997-03-15 = 9204 + 366 + 365 = 9935

template <typename Backend>
void Q3IWorkload<Backend>::set_params_for_iter(long iter)
{
   // Shared rotation table: all 5 segments × 5 March-15 dates in [1993,1997].
   // threshold is always 0 (spec validation default; non-zero thresholds shrink
   // the result set without adding signal for cross-structure comparison).
   const q3_family::ParamEntry& e =
       q3_family::PARAM_TABLE[iter % q3_family::PARAM_TABLE_SIZE];
   params = {Varchar<10>(e.segment), e.date, e.date, Numeric(0)};
}

// ---------------------------------------------------------------------------
// Print helpers for new view / intermediate row types.

inline void q3i_pipeline_view_t::print(std::ostream& os) const
{
   os << "view(ep=" << l_extendedprice << ",disc=" << l_discount
      << ",ship=" << l_shipdate << ",open_due=" << cust_open_due
      << ",date=" << o_orderdate << ",shippri=" << o_shippriority << ")\n";
}

inline void cust_open_due_t::print(std::ostream& os) const
{
   os << "open_due(" << cust_open_due << ")\n";
}

// lineitem_agg_t::print is defined inline in q3_family/lineitem_agg.hpp
// (the type was hoisted Phase 4 §7.2).

// ---------------------------------------------------------------------------
// q3i_agg_row_t::print — tab-separated output.

inline void q3i_agg_row_t::print(std::ostream& os) const
{
   print_base(os);
   os << "\t" << cust_open_due << "\n";
}

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to an orders row before joining (S1, S3).
// Delegates to the family predicate so Q3 and Q3I share identical filter logic.
inline bool q3i_predicate_orders(const orders_t& o, const Params& p)
{
   return q3_family::q3_predicate_orders(o, p);
}

// Applied to a raw lineitem before joining (S1, S3).
// Delegates to the family predicate for the same reason.
inline bool q3i_predicate_lineitem(const lineitem_t& l, const Params& p)
{
   return q3_family::q3_predicate_lineitem(l, p);
}

// Applied to a fully-assembled joined_ol_t (S2).
// Delegates to per-table predicates to keep S1–S3 semantically identical
// (defends OPERATORS.md §6.1: same predicate across structures).
inline bool q3i_predicate_joined(const joined_ol_t& j, const Params& p)
{
   return q3i_predicate_orders(j.order(), p) && q3i_predicate_lineitem(j.line(), p);
}

// Applied to an invoice row for the open-due sub-aggregate.
inline bool q3i_predicate_invoice(const invoice_t& i)
{
   auto s = std::string_view(i.i_status.data, i.i_status.length);
   return s == "O";
}

// ---------------------------------------------------------------------------
// Reusable accumulator helpers.
//
// These are the inside-pipeline aggregate primitives factored as standalone
// structs so that Phase 2's S1 path (BinaryMergeJoin over custkey-sorted
// secondary scanners) can reuse them without modification. Sharing the same
// accumulator code is what makes the S1 vs S3 comparison apples-to-apples
// per OPERATORS.md §6.1.
//
// Both accumulators support overloaded consume* for _t (base) and _coli_t
// (tagged COLI) variants so the same struct compiles in both contexts.

// Per-customer sub-aggregate: SUM(i_totaldue WHERE i_status='O').
// One instance per query; reset() at each custkey transition.
struct CustomerOpenDueAccumulator {
   Numeric value = 0;

   void consume_invoice(const invoice_t& i) {
      auto s = std::string_view(i.i_status.data, i.i_status.length);
      if (s == "O") value += i.i_totaldue;
   }

   void consume_invoice(const invoice_coli_t& i) {
      auto s = std::string_view(i.i_status.data, i.i_status.length);
      if (s == "O") value += i.i_totaldue;
   }

   void reset() { value = 0; }
};

// Per-orderkey revenue accumulator: SUM(l_extendedprice * (1 - l_discount))
// across same-orderkey lineitems passing the shipdate filter.
// Instantiated on Q3I's Params; the template body lives in q3_family/accumulators.hpp.
using LineitemRevenueAccumulator = q3_family::LineitemRevenueAccumulator<Params>;

// ---------------------------------------------------------------------------
// COLIGroupWalkVisitor — Q3I subclass of Q3FamilyVisitor.
//
// Derives from the shared C×O×L base visitor and overlays the invoice arm:
//   • on_invoice: accumulates cust_open_due before the first on_order fires
//     (guaranteed by invoice tag=2 < orders tag=3 in COLI byte-lex order).
//   • per_order_admit_check: gates the OL sub-tree on cust_open_due > threshold
//     at the first on_order call, when open_due is fully accumulated.
//   • extra_emit_fields: attaches cust_open_due to each emitted q3i_agg_row_t.
//
// Design notes:
//   • threshold_ok is evaluated once per custkey group, at the first on_order
//     call.  Because invoices precede orders in COLI byte-lex order (tag 2 <
//     tag 3), open_due.value is fully accumulated before the first on_order —
//     so the threshold check is final at that point.  When the check fails,
//     per_order_admit_check returns false; the base sets skip_group_pending,
//     and the rest of the OL sub-tree is physically skipped by the walker
//     (OPERATORS.md §6 Filter Pushdown: aggregate-output filter fuses with
//     the SortedAggregate, never a post-walk Filter node).
//   • The Visitor collects all qualifying rows into `out`; apply_topN is
//     called by query_by_merged after the walk to enforce ORDER BY revenue
//     DESC LIMIT 10 (OPERATORS.md §3 op 8–9).
//   • (invoice→customer is 1:1 after the sub-aggregate; this is not a
//     4-way M:N — see q3i/CLAUDE.md §Cardinality structure.)

template <typename Sink>
struct COLIGroupWalkVisitor
    : q3_family::Q3FamilyVisitor<COLIGroupWalkVisitor<Sink>, Params,
                                  lineitem_coli_t, q3i_agg_row_t, Q3IStats, Sink> {
   using Base = q3_family::Q3FamilyVisitor<COLIGroupWalkVisitor<Sink>, Params,
                                            lineitem_coli_t, q3i_agg_row_t,
                                            Q3IStats, Sink>;
   // Bring dependent-base members into scope for unqualified use.
   using Base::params;
   using Base::stats;
   using Base::sink;

   // Forwarding constructor — base fields (params, sink, stats) live in the base.
   COLIGroupWalkVisitor(const Params& p, Sink& s, Q3IStats* st = nullptr)
       : Base{p, s, st}
   {}

   // Per-custkey invoice sub-aggregate: SUM(i_totaldue WHERE i_status='O').
   // Accumulated by on_invoice before the first on_order fires.
   CustomerOpenDueAccumulator open_due;

   // Accumulate open-due sub-aggregate before any order rows arrive
   // (guaranteed by invoice tag = 2 < orders tag = 3 in COLI byte-lex order).
   // Not part of the base — only the COLI walker dispatches on_invoice.
   // Always returns Continue (no per-invoice skip case).
   ::tpch::WalkAction on_invoice(const invoice_coli_t::Key&, const invoice_coli_t& i) {
      if (stats) stats->invoices_scanned++;
      Numeric before = open_due.value;
      open_due.consume_invoice(i);
      if (open_due.value != before && stats) stats->invoices_passing_filter++;
      return ::tpch::WalkAction::Continue;
   }

   // CRTP hook: called at the first on_order for a custkey group, after all
   // invoices have been consumed.  Returns false (and triggers a group skip)
   // when cust_open_due <= threshold.
   bool per_order_admit_check(const orders_coli_t& /*o*/) {
      return open_due.value > params.threshold;
   }

   // CRTP hook: attaches cust_open_due to the emitted row.
   void extra_emit_fields(q3i_agg_row_t& row) {
      row.cust_open_due = open_due.value;
   }

   // CRTP hook: reset open_due at each custkey group boundary.
   // Called from on_group_end() in the base after flushing the last order.
   // We override on_group_end to also reset open_due.
   void on_group_end(Integer ck) {
      // Delegate to base to flush the last open order and reset OL state.
      Base::on_group_end(ck);
      open_due.reset();
   }
};

// ---------------------------------------------------------------------------
// Scanner-wrapper aggregators for S1 (3-BMJ chain over custkey-sorted splits).
//
// Both wrappers drive a split adapter's scanner internally, accumulate via
// the shared accumulator structs above, and emit one output row per key group
// at the next group boundary (lazy emission). This is the SortedAggregate
// pattern from OPERATORS.md §3 op 6.
//
// CustomerOpenDueAggregator: wraps split_invoice (custkey-sorted invoice_coli_t
//   records). Emits (cust_open_due_t::Key, cust_open_due_t) per custkey group
//   only when cust_open_due > threshold (threshold filter fused at emit time,
//   per OPERATORS.md §6 Filter Pushdown: aggregate-output filter at the
//   SortedAggregate, not a downstream Filter node).
//
// LineitemRevenueAggregator: wraps split_lineitem (custkey, orderkey, linenumber
//   sorted lineitem_coli_t records). Emits (lineitem_agg_t::Key, lineitem_agg_t)
//   per (custkey, orderkey) group. Filter l_shipdate > shipdate fused at consume
//   time via LineitemRevenueAccumulator::consume.

template <typename Backend>
class CustomerOpenDueAggregator
{
   using Scanner = decltype(std::declval<typename Backend::template Adapter<invoice_coli_t>>().getScanner());
   Scanner scanner_;
   const Params& params_;

   // Buffered next emission: set when a group boundary is crossed.
   std::optional<std::pair<cust_open_due_t::Key, cust_open_due_t>> pending_;

   // Current group state.
   Integer               cur_custkey_ = -1;
   CustomerOpenDueAccumulator acc_;

   // Lookahead: the first invoice_coli_t row of the next group, held across
   // the group boundary so we don't lose it.
   std::optional<std::pair<invoice_coli_t::Key, invoice_coli_t>> lookahead_;
   bool exhausted_ = false;

   // Flush the current group into pending_ if it passes the threshold.
   void flush_group()
   {
      if (cur_custkey_ < 0) return;
      if (acc_.value > params_.threshold) {
         pending_ = {cust_open_due_t::Key{cur_custkey_},
                     cust_open_due_t{acc_.value}};
      }
      acc_.reset();
   }

  public:
   explicit CustomerOpenDueAggregator(
       typename Backend::template Adapter<invoice_coli_t>& adapter,
       const Params& params)
       : scanner_(adapter.getScanner()), params_(params)
   {
      // Prime the lookahead.
      lookahead_ = scanner_->next();
      if (!lookahead_) exhausted_ = true;
   }

   // Forward-only seek to the first invoice with custkey >= K. Discards any
   // partial-group accumulation and pending emission for keys < K. Used by
   // S1's customer-skip wrapper (G1) to bypass invoices for custkeys the
   // customer-side mktsegment filter rejected. Returns true if the scanner
   // was actually advanced (i.e. K is strictly ahead of the current cursor).
   bool seek_to_custkey(Integer K)
   {
      // Already past K — nothing to skip. cur_custkey_ tracks the group
      // currently being accumulated; if it's >= K we're already there.
      if (cur_custkey_ >= K) return false;
      if (lookahead_ && lookahead_->first.custkey >= K) return false;
      // Discard mid-group state: any partial accumulation for a custkey
      // < K is dropped (BMJ has already advanced past it on the customer side).
      pending_ = std::nullopt;
      acc_.reset();
      cur_custkey_ = -1;
      typename invoice_coli_t::Key seek_key{K, 0};
      scanner_->seek(seek_key);
      lookahead_ = scanner_->next();
      if (!lookahead_) exhausted_ = true;
      return true;
   }

   std::optional<std::pair<cust_open_due_t::Key, cust_open_due_t>> next()
   {
      // Drain any pending emission first.
      if (pending_) {
         auto out = std::move(pending_);
         pending_ = std::nullopt;
         return out;
      }
      if (exhausted_ && cur_custkey_ < 0) return std::nullopt;

      // Consume rows until we complete a new group.
      for (;;) {
         if (lookahead_) {
            auto& [k, v] = *lookahead_;
            if (k.custkey != cur_custkey_) {
               // Group boundary: flush the old group.
               Integer prev_key = cur_custkey_;
               flush_group();
               cur_custkey_ = k.custkey;
               acc_.consume_invoice(v);
               lookahead_ = scanner_->next();
               if (!lookahead_) exhausted_ = true;
               if (prev_key >= 0 && pending_) {
                  auto out = std::move(pending_);
                  pending_ = std::nullopt;
                  return out;
               }
               // prev_key was -1 (first row) or threshold not met — continue.
               continue;
            }
            // Same custkey: accumulate.
            acc_.consume_invoice(v);
            lookahead_ = scanner_->next();
            if (!lookahead_) exhausted_ = true;
         } else {
            // Scanner exhausted — flush last group.
            if (cur_custkey_ < 0) return std::nullopt;
            flush_group();
            cur_custkey_ = -1;
            if (pending_) {
               auto out = std::move(pending_);
               pending_ = std::nullopt;
               return out;
            }
            return std::nullopt;
         }
      }
   }
};

// LineitemRevenueAggregator hoisted to q3_family/lineitem_revenue_aggregator.hpp
// (Phase 4 §7.2) so Q3 (lineitem_col_t) and Q3I (lineitem_coli_t) share one
// implementation.  Aliased here for callers below.
template <typename Backend>
using LineitemRevenueAggregator =
    q3_family::LineitemRevenueAggregator<Backend, lineitem_coli_t, Params>;

// ---------------------------------------------------------------------------
// Q3IWorkload query methods

template <typename Backend>
long Q3IWorkload<Backend>::query_by_merged(std::vector<q3i_agg_row_t>& out)
{
   // S3: COLIGroupWalk over the 4-table COLI MI using the namespace-scope
   // COLIGroupWalkVisitor.  After the walk, apply_topN enforces
   // ORDER BY revenue DESC LIMIT 10 (OPERATORS.md §3 op 8–9).
   //
   // Byte-lex order within a custkey group:
   //   customer → invoice* → (orders → lineitems*)+
   //
   // cust_open_due is fully accumulated before the first on_order fires,
   // so the threshold check is computed once and used as a short-circuit
   // gate for the entire OL sub-hierarchy of each custkey group
   // (OPERATORS.md §6 Filter Pushdown: aggregate-output filter fuses with
   // the SortedAggregate, never a post-walk Filter node).
   out.clear();
   Q3IPerfCapture<Backend> _pc(MICRO_PERF_STATS(*this));
   // Streaming top-K sink — bounded memory (K = 10), same role as
   // Q5's NNameRevenueAggregator and the Q3 sinks: visitor pushes
   // rows in via flush_order → sink.offer; final drain sorts.
   auto cmp = [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3i_agg_row_t, decltype(cmp)> sink(10, cmp);
   COLIGroupWalkVisitor<decltype(sink)> v(params, sink, stats);
   {
      // S3 fuses scan / aggregator / join into a single walk; attribute
      // the whole walk to the join stage for cross-path comparison.
      StageTimer t(stats ? &stats->stage_us_join : nullptr);
      // A2c: dispatch to fused_emit walker when requested via --coli_walker_variant.
      // Both paths produce identical output; the flag selects only the dispatch
      // mechanism (std::variant vs tag-byte switch).
      if (FLAGS_coli_walker_variant == "fused_emit") {
         coli_group_walk_fused_emit<Backend>(coli.merged_adapter(), v);
      } else {
         coli_group_walk<Backend>(coli.merged_adapter(), v);
      }
   }
   // Drain the sink's top-10 into `out` ordered by revenue DESC outside
   // the pipeline (OPERATORS.md §3 op 8–9).  o_orderdate ASC, then
   // o_orderkey ASC as tiebreakers keep ordering deterministic across
   // paths whose input ordering differs (view scan vs. hash-map iteration
   // vs. merged scan); cmp is the same q3_agg_row_base_t::cmp.
   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      sink.drain_sorted(out);
   }
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_base(std::vector<q3i_agg_row_t>& out)
{
   // S1: 3-BMJ chain over custkey-sorted COLI split indexes.
   //
   // Chain:
   //   BMJ #1: customerh_t ⋈ cust_open_due_t on custkey
   //           (customer scan filter-pushed: c_mktsegment == params.mktsegment)
   //           (threshold filter fused inside CustomerOpenDueAggregator at emit)
   //   BMJ #2: BMJ#1 result ⋈ orders_coli_t on custkey
   //           (orderdate filter fused in fetch lambda)
   //   BMJ #3: BMJ#2 result ⋈ lineitem_agg_t on (custkey, orderkey)
   //           (shipdate filter fused inside LineitemRevenueAggregator at consume)
   //
   // OPERATORS.md §3 op 4 (S1); accumulators reuse the same structs as S3
   // per OPERATORS.md §6.1 comparison-integrity.
   out.clear();
   Q3IPerfCapture<Backend> _pc(MICRO_PERF_STATS(*this));

   auto cmp = [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3i_agg_row_t, decltype(cmp)> sink(10, cmp);

   // --- scanner-wrapper aggregators ---
   // These drive the custkey-sorted COLI split adapters and emit aggregated
   // rows. Aggregators own their scanner internally.
   CustomerOpenDueAggregator<Backend> agg_inv(coli.split_invoice(), params);
   LineitemRevenueAggregator<Backend> agg_lin(coli.split_lineitem(), params);

   // --- filter-pushed scanners ---
   auto cust_scan_ptr = customer.getScanner();
   auto ord_scan_ptr  = coli.split_orders().getScanner();

   // G1 customer-level Seek-skip on the invoice aggregator only.
   //
   // Trait-gated by Backend::USE_PHYSICAL_SEEK_SKIP, runtime-overridden by
   // --use_seek_skip. See q3i/PERFORMANCE.md §3 A/B-1.
   //
   // Why only agg_inv (1:1 per custkey): fetch_cust returns one passing
   // customer K per call; agg_inv is positioned at the matching open_due
   // by Seek + next() returning the K-row. Forward-only seek is safe.
   //
   // G6 attempted (and abandoned): deferred Seek on ord_scan / agg_lin
   // (the BMJ#2/#3 right sides, 1:N per custkey). Three mechanics tried:
   //
   //   (a) Stash target in fetch_cust, apply on next fetch_ord. Bug: each
   //       fetch_cust returns the *next* passing customer, so the target
   //       gets overwritten before bmj2 has finished the prior group.
   //   (b) Read bmj2.jk_to_join() inside fetch_ord. Bug: jk_to_join is the
   //       smaller of next_left.JK and next_right.JK, so each gap-custkey
   //       refill sees jk_to_join walk one step at a time — never a
   //       sufficient jump to trigger a skip.
   //   (c) Read bmj2.next_left, gated by a consume_joined-driven flag.
   //       Bug: consume fires *after* a group's cross-product is queued;
   //       by the time it sets the flag, ord_scan is already past that
   //       group, but bmj2 may still be processing the *next* passing
   //       group (next_left has advanced ahead of the active jk). Seek
   //       skips the active group's remaining orders.
   //
   // The fundamental obstacle: BMJ doesn't expose "I am done refilling for
   // jk=X" without intrusive hooks. A clean fix needs either:
   //   - A buffered wrapper around ord_scan / agg_lin that pre-loads
   //     orders for K passing customers ahead of bmj2 (defeats streaming).
   //   - An `on_group_flush(jk)` callback inside BMJ::next_jk after refill
   //     completes (intrusive change to the shared primitive).
   // Both are bigger refactors. Deferred until measurements justify them
   // (A/B-1 G6 attempt: btree gain ~10%, RocksDB regression — not worth
   // the refactor cost). Records `bj_ord_skips` / `bj_lin_skips` are
   // declared in the stats struct for forward compatibility but stay 0.
   const bool seek_skip =
       FLAGS_use_seek_skip < 0 ? Backend::USE_PHYSICAL_SEEK_SKIP
                               : (FLAGS_use_seek_skip != 0);

   // Fetch customer rows passing the mktsegment filter.
   auto fetch_cust = [&]() -> std::optional<std::pair<customerh_t::Key, customerh_t>> {
      while (auto kv = cust_scan_ptr->next()) {
         if (stats) stats->customers_scanned++;
         auto sm  = std::string_view(kv->second.c_mktsegment.data,
                                     kv->second.c_mktsegment.length);
         auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
         if (sm == psm) {
            if (stats) stats->customers_passing_filter++;
            if (seek_skip) {
               const Integer K = kv->first.c_custkey;
               if (agg_inv.seek_to_custkey(K) && stats) stats->bj_groups_skipped++;
            }
            return kv;
         }
      }
      return std::nullopt;
   };

   // Fetch cust_open_due rows from the aggregator.
   auto fetch_inv_agg = [&]() { return agg_inv.next(); };

   // BMJ #1: customer ⋈ cust_open_due on custkey.
   // Join key = cust_open_due_t::Key (custkey only).
   BinaryMergeJoin<cust_open_due_t::Key, q3i_jr1_t, customerh_t, cust_open_due_t>
       bmj1(fetch_cust, fetch_inv_agg);

   // BMJ #2: q3i_jr1_t ⋈ orders_coli_t on custkey.
   // fetch_ord skips orders with o_orderdate >= params.orderdate.
   auto fetch_ord = [&]() -> std::optional<std::pair<orders_coli_t::Key, orders_coli_t>> {
      while (auto kv = ord_scan_ptr->next()) {
         if (stats) stats->orders_scanned++;
         if (kv->second.o_orderdate < params.orderdate) {
            if (stats) stats->orders_passing_filter++;
            return kv;
         }
      }
      return std::nullopt;
   };

   auto fetch_bmj1 = [&]() {
      auto kv = bmj1.next();
      if (kv && stats) stats->join1_output_rows++;
      return kv;
   };

   BinaryMergeJoin<cust_open_due_t::Key, q3i_jr2_t, q3i_jr1_t, orders_coli_t>
       bmj2(fetch_bmj1, fetch_ord);

   // BMJ #3: q3i_jr2_t ⋈ lineitem_agg_t on (custkey, orderkey).
   auto fetch_bmj2 = [&]() {
      auto kv = bmj2.next();
      if (kv && stats) stats->join2_output_rows++;
      return kv;
   };
   auto fetch_lin_agg = [&]() { return agg_lin.next(); };

   BinaryMergeJoin<lineitem_agg_t::Key, q3i_jr3_t, q3i_jr2_t, lineitem_agg_t>
       bmj3(fetch_bmj2, fetch_lin_agg);

   // Drain BMJ #3: each JR3 carries (q3i_jr2_t, lineitem_agg_t).
   {
      StageTimer t(stats ? &stats->stage_us_join : nullptr);
      while (auto kv = bmj3.next()) {
         if (stats) {
            stats->join_callbacks++;
            stats->join3_output_rows++;
         }
         const q3i_jr3_t& jr3 = kv->second;
         const q3i_jr2_t& jr2 = jr3.jr2();
         const lineitem_agg_t& lagg = jr3.linagg();
         const orders_coli_t&  o   = jr2.order();
         const cust_open_due_t& due = jr2.jr1().due();
         Integer orderkey = kv->first.jk.orderkey;
         if (stats) {
            stats->aggregator_rows_out++;
            stats->topN_candidates++;
         }
         sink.offer({orderkey, lagg.revenue, o.o_orderdate,
                     o.o_shippriority, due.cust_open_due});
      }
   }

   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      sink.drain_sorted(out);
   }
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_view(std::vector<q3i_agg_row_t>& out)
{
   // S2: sequential scan of the materialised q3i_pipeline_view_t.
   //
   // Schema redesign (2026-05-03): the view now stores one row per
   // (custkey, orderkey, linenumber) carrying unaggregated lineitem fields.
   // Revenue is accumulated per orderkey at query time, with the shipdate
   // filter applied live.  This makes S2 reusable across all DATE param sets
   // (the original per-orderkey schema baked l_shipdate > DATE_1995_03_15 at
   // load time, breaking correctness for any other DATE value).
   //
   // Scan order: (custkey, orderkey, linenumber) ascending.  Within a
   // (custkey, orderkey) group the parameterised filters (mktsegment,
   // threshold, orderdate) are identical for every row, so we check them once
   // when we first see a new orderkey.  Per-lineitem we apply l_shipdate.
   // At each orderkey boundary we emit the per-orderkey agg row if it has
   // any qualifying lineitems (revenue > 0).
   //
   // OPERATORS.md §4 / §6: all parameterised filters applied at query time;
   // none baked into the view at load time.
   out.clear();
   Q3IPerfCapture<Backend> _pc(MICRO_PERF_STATS(*this));

   auto cmp = [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3i_agg_row_t, decltype(cmp)> sink(10, cmp);

   // Per-orderkey accumulator state — tracks the currently-open order group.
   Integer   cur_orderkey    = -1;
   Timestamp cur_orderdate   = 0;
   Integer   cur_shippriority = 0;
   Numeric   cur_open_due    = 0;
   bool      cur_order_ok    = false;  // passes mktsegment + threshold + orderdate
   LineitemRevenueAccumulator rev;

   // Emit the currently-open order group if it produced any revenue.
   auto flush_order = [&]() {
      if (!cur_order_ok || cur_orderkey < 0) return;
      if (rev.revenue <= Numeric(0)) { rev.reset(); return; }
      if (stats) {
         stats->join_callbacks++;
         stats->join3_output_rows++;
         stats->aggregator_rows_out++;
         stats->topN_candidates++;
      }
      sink.offer({cur_orderkey, rev.revenue, cur_orderdate,
                  cur_shippriority, cur_open_due});
      rev.reset();
   };

   {
      StageTimer t(stats ? &stats->stage_us_scan_filter : nullptr);
      auto vs = pipeline_view.getScanner();
      while (auto kv = vs->next()) {
         const q3i_pipeline_view_t::Key& k   = kv->first;
         const q3i_pipeline_view_t&      row = kv->second;

         // Orderkey transition: flush the previous group and re-evaluate
         // per-order gates for the new (custkey, orderkey).
         if (k.orderkey != cur_orderkey) {
            flush_order();
            cur_orderkey     = k.orderkey;
            cur_orderdate    = row.o_orderdate;
            cur_shippriority = row.o_shippriority;
            cur_open_due     = row.cust_open_due;
            cur_order_ok     = false;

            // Mktsegment filter.
            auto sm  = std::string_view(row.c_mktsegment.data, row.c_mktsegment.length);
            auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
            if (sm != psm) continue;

            // Threshold filter.
            if (row.cust_open_due <= params.threshold) continue;

            // Orderdate filter.
            if (row.o_orderdate >= params.orderdate) {
               if (stats) stats->customers_passing_filter++;
               continue;
            }
            if (stats) {
               stats->customers_passing_filter++;
               stats->orders_passing_filter++;
            }
            cur_order_ok = true;
         }

         if (!cur_order_ok) continue;

         // Per-lineitem: apply shipdate filter and accumulate revenue.
         // LineitemRevenueAccumulator::consume(lineitem_t, params) checks
         // l_shipdate > params.shipdate — reusing the same accumulator as S1/S3
         // for OPERATORS.md §6.1 comparison-integrity.
         lineitem_t proxy;
         proxy.l_shipdate      = row.l_shipdate;
         proxy.l_extendedprice = row.l_extendedprice;
         proxy.l_discount      = row.l_discount;
         rev.consume(proxy, params);
      }
      flush_order();
   }

   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      sink.drain_sorted(out);
   }
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_hash(std::vector<q3i_agg_row_t>& out)
{
   // S4: 3-HJ chain over base tables (no merged index, no custkey ordering).
   //
   // Pre-aggregation steps (before any join):
   //   a) Invoice hash-aggregate → per-custkey open_due map.
   //      Threshold filter fused at emit time (same as CustomerOpenDueAggregator
   //      in S1 — OPERATORS.md §6.1 comparison-integrity).
   //   b) Customer hash lookup map → per-custkey mktsegment filter.
   //
   // HJ chain (probe side = lineitem, the largest table):
   //   HJ #1: customer ⋈ cust_open_due on custkey (build = cust_open_due_map)
   //   HJ #2: HJ#1 result ⋈ orders on custkey      (build = orders_by_custkey)
   //   HJ #3: HJ#2 result ⋈ lineitem on orderkey   (build = lineitem_by_orderkey)
   //
   // Post-join: hash-aggregate by orderkey (HashJoin output is unsorted).
   //
   // S4 calls the base-table _t overloads of the accumulators (not _coli_t)
   // per OPERATORS.md §6.1 comparison-integrity — same arithmetic as S1/S3.
   out.clear();
   Q3IPerfCapture<Backend> _pc(MICRO_PERF_STATS(*this));

   // S4 fuses invoice aggregate, customer filter, order build, and lineitem
   // probe into one conceptual join chain — the same notion as S1/S3 where
   // scan + filter + aggregate all run inside the merge/walk driver and are
   // attributed to stage_us_join. A single outer StageTimer wraps the whole
   // HJ chain so S1/S3/S4 all report stage_us_join + stage_us_topN, making
   // the per-stage wall-clock directly comparable across structures.
   // (stage_us_scan_filter and stage_us_aggregator stay at zero for S4.)

   // --- Step a: invoice hash-aggregate with threshold filter ---
   // Reuses CustomerOpenDueAccumulator's consume_invoice(invoice_t) overload.
   std::unordered_map<Integer, Numeric> open_due_map;
   // --- Step b: customer mktsegment filter map ---
   std::unordered_map<Integer, bool> cust_ok;  // true = passes mktsegment filter
   // --- Post-join aggregate by orderkey ---
   std::unordered_map<Integer, q3i_agg_row_t> per_order;

   // Scan orders (orderdate filter fused here).
   auto ord_scan = orders.getScanner();
   auto lin_scan = lineitem.getScanner();

   struct OrderSlot {
      Integer custkey;
      Timestamp orderdate;
      Integer shippriority;
   };
   std::unordered_map<Integer, OrderSlot> ord_map;

   {
      // One outer timer covers the entire HJ chain: invoice aggregate,
      // customer filter, order build, and lineitem probe.
      StageTimer t(stats ? &stats->stage_us_join : nullptr);

      // Step a: invoice hash-aggregate.
      {
         CustomerOpenDueAccumulator acc;
         auto inv_scan = invoice.getScanner();
         while (auto kv = inv_scan->next()) {
            if (stats) stats->invoices_scanned++;
            const invoice_t& inv = kv->second;
            // consume_invoice fuses the i_status='O' filter.
            Numeric before = acc.value;
            acc.consume_invoice(inv);
            if (acc.value != before) {
               if (stats) stats->invoices_passing_filter++;
               open_due_map[inv.i_custkey] = open_due_map[inv.i_custkey] + (acc.value - before);
            }
            acc.reset();
         }
         // Apply threshold filter: drop custkeys that don't qualify.
         for (auto it = open_due_map.begin(); it != open_due_map.end(); ) {
            if (it->second <= params.threshold) it = open_due_map.erase(it);
            else ++it;
         }
      }

      // Step b: customer mktsegment filter map.
      {
         auto cust_scan = customer.getScanner();
         while (auto kv = cust_scan->next()) {
            if (stats) stats->customers_scanned++;
            const customerh_t& c = kv->second;
            auto sm  = std::string_view(c.c_mktsegment.data, c.c_mktsegment.length);
            auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
            if (sm == psm) {
               cust_ok[kv->first.c_custkey] = true;
               if (stats) stats->customers_passing_filter++;
            }
         }
      }

      // Build orders map filtered by date, custkey membership in open_due_map,
      // and mktsegment filter.
      //
      // Capture o.o_custkey / o.o_orderdate / o.o_shippriority into locals
      // before the gate checks so the `count()` probe and the OrderSlot store
      // see the same value even if `kv->second` is a buffer-pool reference
      // that gets re-bound across field accesses on LeanStore. Without this,
      // q3i_btree S4 at SF=15 dram=0.1 hits the post-gate invariant violation
      // logged at the lineitem probe loop below.
      while (auto kv = ord_scan->next()) {
         if (stats) stats->orders_scanned++;
         const orders_t& o = kv->second;
         const Timestamp od      = o.o_orderdate;
         if (od >= params.orderdate) continue;
         if (stats) stats->orders_passing_filter++;
         const Integer   ck      = o.o_custkey;
         const Integer   shippri = o.o_shippriority;
         if (!open_due_map.count(ck)) continue;
         if (!cust_ok.count(ck))      continue;
         if (stats) stats->join1_output_rows++;  // post HJ#1+HJ#2 (cust_open_due ∩ cust_seg)
         ord_map[kv->first.o_orderkey] = {ck, od, shippri};
         if (stats) stats->join2_output_rows++;  // each surviving (custkey,orderkey)
      }

      // G2: build a sorted vector of surviving orderkeys for orderkey-
      // level Seek-skip on the lineitem probe. The orders-build phase
      // populates ord_map keyed by surviving orderkeys; HashJoin's
      // probe-miss path otherwise iterates one lineitem at a time
      // through every orderkey. Trait-gated via use_seek_skip; counter
      // increments per skip event. See q3i/PERFORMANCE.md §3 A/B-1.
      const bool seek_skip =
          FLAGS_use_seek_skip < 0 ? Backend::USE_PHYSICAL_SEEK_SKIP
                                  : (FLAGS_use_seek_skip != 0);
      std::vector<Integer> surviving_orderkeys;
      if (seek_skip) {
         surviving_orderkeys.reserve(ord_map.size());
         for (const auto& [ok, _] : ord_map) surviving_orderkeys.push_back(ok);
         std::sort(surviving_orderkeys.begin(), surviving_orderkeys.end());
      }

      // Probe lineitem against orders map; accumulate revenue per orderkey.
      {
         LineitemRevenueAccumulator acc;
         while (auto kv = lin_scan->next()) {
            if (stats) stats->lineitems_scanned++;
            const lineitem_t& l = kv->second;
            auto it = ord_map.find(kv->first.l_orderkey);
            if (it == ord_map.end()) {
               if (seek_skip) {
                  // Lineitem's orderkey missed ord_map. Find the next
                  // surviving orderkey > current and seek the lineitem
                  // scanner there. Orderkeys-sorted-vector binary search.
                  auto sit = std::upper_bound(
                      surviving_orderkeys.begin(),
                      surviving_orderkeys.end(),
                      kv->first.l_orderkey);
                  if (sit == surviving_orderkeys.end()) break;  // no more
                  if (stats) stats->hj_groups_skipped++;
                  lin_scan->seek(lineitem_i_t::Key{*sit, Integer(0)});
               }
               continue;
            }
            if (!acc.consume(l, params)) continue;  // shipdate filter fused in consume
            if (stats) {
               stats->lineitems_passing_filter++;
               stats->join_callbacks++;
               stats->join3_output_rows++;
            }
            const OrderSlot& slot = it->second;
            Integer orderkey = kv->first.l_orderkey;
            auto& row = per_order[orderkey];
            if (row.o_orderkey == Integer(0)) {
               // First lineitem for this order: populate order fields.
               // Defensive lookup: by construction (line 851's gate), every
               // slot.custkey was in open_due_map when ord_map was populated.
               // If we ever miss here, log and skip — this is the symptom of
               // the SF=15 dram=0.1 q3i_btree S4 crash before the local-copy
               // fix above. Once that fix holds, this branch should never fire.
               auto due_it = open_due_map.find(slot.custkey);
               if (due_it == open_due_map.end()) {
                  std::cerr << "[q3i S4 invariant violation] slot.custkey="
                            << slot.custkey << " orderkey=" << orderkey
                            << " open_due_map.size=" << open_due_map.size()
                            << " ord_map.size=" << ord_map.size() << "\n";
                  continue;
               }
               row.o_orderkey     = orderkey;
               row.o_orderdate    = slot.orderdate;
               row.o_shippriority = slot.shippriority;
               row.cust_open_due  = due_it->second;
            }
            row.revenue += acc.revenue;
            acc.reset();
         }
      }
   }  // end StageTimer: entire HJ chain attributed to stage_us_join

   {
      auto cmp = [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
         return q3_family::q3_agg_row_base_t::cmp(a, b);
      };
      TopNSink<q3i_agg_row_t, decltype(cmp)> sink(10, cmp);
      for (auto& [_, row] : per_order) {
         if (row.revenue > Numeric(0)) {
            if (stats) {
               stats->aggregator_rows_out++;
               stats->topN_candidates++;
            }
            sink.offer(std::move(row));
         }
      }
      {
         StageTimer t(stats ? &stats->stage_us_topN : nullptr);
         sink.drain_sorted(out);
      }
   }
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_aggregated(std::vector<q3i_agg_row_t>& out)
{
   // S5: scan the aCOLI 3-type MI
   //     (customer_acoli_t + orders_acoli_t + lineitem_acoli_t).
   //
   // Schema redesign (2026-05-03): the MI now stores unaggregated lineitems
   // (lineitem_acoli_t) instead of a pre-baked pre_revenue on orders_acoli_t.
   // Revenue is accumulated per orderkey at query time using
   // LineitemRevenueAccumulator — the same accumulator as S1/S3 per
   // OPERATORS.md §6.1 comparison-integrity.  pre_open_due on customer_acoli_t
   // is still baked (parameter-independent: i_status='O' is hardcoded by spec).
   //
   // Byte-lex sort order within a custkey group (tagged keys, Step 4b):
   //   customer_acoli_t (7 B) → orders_coli_t (12 B) → lineitem_acoli_t (17 B)
   //
   // Walk logic per custkey group:
   //   on_customer: check mktsegment + threshold; gate further processing.
   //   on_order:    check o_orderdate < params.orderdate; open per-order accumulator.
   //   on_lineitem: apply l_shipdate > params.shipdate; accumulate revenue.
   //   order-transition flush: emit q3i_agg_row_t when revenue > 0.
   out.clear();
   Q3IPerfCapture<Backend> _pc(MICRO_PERF_STATS(*this));

   auto cmp = [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3i_agg_row_t, decltype(cmp)> sink(10, cmp);

   {
      StageTimer t(stats ? &stats->stage_us_join : nullptr);

      auto& mi      = coli.acoli_adapter();
      auto  scanner = mi.template getScanner<customer_acoli_t::Key, customer_acoli_t>();

      // Per-group state.
      bool    cust_passes   = false;
      Numeric cur_open_due  = 0;

      // Per-order state.
      bool      order_open      = false;
      Integer   cur_orderkey    = -1;
      Timestamp cur_orderdate   = 0;
      Integer   cur_shippriority = 0;
      LineitemRevenueAccumulator rev;

      // Emit the currently-open order if it has positive revenue.
      auto flush_order = [&]() {
         if (!order_open) return;
         order_open = false;
         if (rev.revenue > Numeric(0)) {
            if (stats) {
               stats->acoli_orders_emitted++;
               stats->aggregator_rows_out++;
               stats->topN_candidates++;
            }
            sink.offer({cur_orderkey, rev.revenue,
                        cur_orderdate, cur_shippriority, cur_open_due});
         }
         rev.reset();
      };

      while (auto kv = scanner->next()) {
         std::visit(
             [&](auto&& val) {
                using V = std::decay_t<decltype(val)>;

                if constexpr (std::is_same_v<V, customer_acoli_t>) {
                   // New custkey group: flush any open order from the previous group.
                   flush_order();
                   if (stats) stats->acoli_customers_scanned++;
                   auto sm  = std::string_view(val.c_mktsegment.data,
                                               val.c_mktsegment.length);
                   auto psm = std::string_view(params.mktsegment.data,
                                               params.mktsegment.length);
                   cur_open_due = val.pre_open_due;
                   cust_passes  = (sm == psm) && (cur_open_due > params.threshold);
                   if (cust_passes && stats) stats->acoli_customers_passing_filter++;

                } else if constexpr (std::is_same_v<V, orders_coli_t>) {
                   if (!cust_passes) return;
                   // Order transition: flush the previous order before opening a new one.
                   // orders_acoli_t was collapsed into orders_coli_t (Step 4b).
                   flush_order();
                   if (stats) stats->acoli_orders_scanned++;
                   if (val.o_orderdate >= params.orderdate) return;
                   // Open a new per-order accumulator.
                   const auto* ok = std::get_if<orders_coli_t::Key>(&kv->first);
                   if (!ok) return;
                   order_open      = true;
                   cur_orderkey    = ok->orderkey;
                   cur_orderdate   = val.o_orderdate;
                   cur_shippriority = val.o_shippriority;

                } else if constexpr (std::is_same_v<V, lineitem_acoli_t>) {
                   if (!cust_passes || !order_open) return;
                   if (stats) stats->acoli_lineitems_scanned++;
                   // Reuse LineitemRevenueAccumulator via a lineitem_t proxy
                   // so the arithmetic is identical to S1/S3 (OPERATORS.md §6.1).
                   lineitem_t proxy;
                   proxy.l_shipdate      = val.l_shipdate;
                   proxy.l_extendedprice = val.l_extendedprice;
                   proxy.l_discount      = val.l_discount;
                   if (rev.consume(proxy, params)) {
                      if (stats) stats->acoli_lineitems_passing++;
                   }
                }
             },
             kv->second);
      }
      // Flush the final open order.
      flush_order();
   }

   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      sink.drain_sorted(out);
   }
   return static_cast<long>(out.size());
}

}  // namespace tpch::q3i
