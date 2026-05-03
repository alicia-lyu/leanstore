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
DECLARE_bool(acoli_projected);

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

inline void lineitem_agg_t::print(std::ostream& os) const
{
   os << "lineitem_agg(" << revenue << ")\n";
}

// ---------------------------------------------------------------------------
// q3i_agg_row_t::print — tab-separated output.

inline void q3i_agg_row_t::print(std::ostream& os) const
{
   os << o_orderkey << "\t" << revenue << "\t" << o_orderdate << "\t"
      << o_shippriority << "\t" << cust_open_due << "\n";
}

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to an orders row before joining (S1, S3).
inline bool q3i_predicate_orders(const orders_t& o, const Params& p)
{
   return o.o_orderdate < p.orderdate;
}

// Applied to a raw lineitem before joining (S1, S3).
inline bool q3i_predicate_lineitem(const lineitem_t& l, const Params& p)
{
   return l.l_shipdate > p.shipdate;
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

// Per-orderkey revenue: SUM(l_extendedprice * (1 - l_discount))
// across same-orderkey lineitems passing the shipdate filter.
struct LineitemRevenueAccumulator {
   Numeric revenue = 0;

   // Returns true if the lineitem passed the filter and contributed to revenue.
   bool consume(const lineitem_t& l, const Params& p) {
      if (l.l_shipdate <= p.shipdate) return false;
      revenue += l.l_extendedprice * (Numeric(1) - l.l_discount);
      return true;
   }

   bool consume(const lineitem_coli_t& l, const Params& p) {
      if (l.l_shipdate <= p.shipdate) return false;
      revenue += l.l_extendedprice * (Numeric(1) - l.l_discount);
      return true;
   }

   void reset() { revenue = 0; }
};

// ---------------------------------------------------------------------------
// COLIGroupWalkVisitor — lifted to namespace scope so Phase 2B S1/S2/S4
// drivers can reference the same Visitor type without duplication.
//
// Design notes:
//   • threshold_ok is computed once per custkey group, the first time
//     on_order fires.  Because invoices precede orders in COLI byte-lex
//     order (tag 2 < tag 3), open_due.value is fully accumulated before
//     the first on_order call — so the threshold check is final at that
//     point.  on_order / on_lineitem no-op for the rest of the group when
//     threshold_ok is false, honouring OPERATORS.md §6 Filter Pushdown:
//     the aggregate-output filter fuses with the SortedAggregate, never
//     a post-walk Filter node.
//   • The Visitor collects all qualifying rows into `out`; apply_topN is
//     called by query_by_merged after the walk to enforce ORDER BY revenue
//     DESC LIMIT 10 (OPERATORS.md §3 op 8–9).
//   • (invoice→customer is 1:1 after the sub-aggregate; this is not a
//     4-way M:N — see q3i/CLAUDE.md §Cardinality structure.)

struct COLIGroupWalkVisitor {
   const Params& params;
   std::vector<q3i_agg_row_t>& out;
   Q3IStats* stats = nullptr;  // optional — bumped by walker hooks below

   // Walker hooks — coli_group_walk dispatches to these via SFINAE.
   void on_record_visited() { if (stats) stats->mi_records_visited++; }
   void on_group_skipped(Integer /*ck*/) { if (stats) stats->mi_groups_skipped++; }

   // Visitor-driven custkey skip: set true to request walker to seek to
   // the next custkey. Used when the cust_open_due threshold finalises
   // false on the first on_order (the rest of the OL sub-tree of this
   // custkey is doomed by the aggregate-output filter — no point
   // streaming it).
   bool skip_group_pending = false;
   bool wants_skip_group() {
      bool s = skip_group_pending;
      skip_group_pending = false;  // one-shot — consumed by walker
      return s;
   }

   // Visitor-driven order skip: set true to request walker to forward-iterate
   // past the current order's lineitems without dispatching on_lineitem. Used
   // when on_order rejects the orderdate filter — the lineitems under that
   // order are all doomed, so skipping them avoids per-row dispatch cost
   // (≈75% of lineitems at default params with a 1995-03-15 cutoff).
   bool skip_order_pending = false;
   bool wants_skip_order() {
      bool s = skip_order_pending;
      skip_order_pending = false;  // one-shot — consumed by walker
      return s;
   }

   // Inside-pipeline accumulators (factored — S1 reuses these in Phase 2).
   CustomerOpenDueAccumulator open_due;
   LineitemRevenueAccumulator rev;

   // Per-custkey gates.
   bool mktsegment_ok = false;  // set by on_customer; gate for entire group
   bool threshold_ok  = false;  // set on first on_order; gate for OL work

   // Current open order register: reset per order, flushed at next order
   // or at on_group_end.  Lineitems are co-located under their parent order
   // in COLI byte order, so a single register suffices.
   bool      have_open_order = false;
   Integer   cur_orderkey    = 0;
   Timestamp cur_orderdate   = 0;
   Integer   cur_shippriority = 0;

   // Emit the current order and reset per-order state.
   // Called at each on_order boundary and at on_group_end.
   void flush_order() {
      if (!have_open_order) return;
      have_open_order = false;
      // threshold_ok already confirmed. Only emit when at least one lineitem
      // passed the shipdate filter (revenue > 0). Orders whose lineitems all
      // fail the shipdate predicate have no matching rows in the SQL result.
      if (rev.revenue > Numeric(0)) {
         // join2/join3_output_rows are S1/S4 chain-join abstractions; S3 is a
         // single fused walk — those counters stay at zero for S3. See
         // q3i/CLAUDE.md §Cardinality structure.
         out.push_back({cur_orderkey, rev.revenue, cur_orderdate,
                        cur_shippriority, open_due.value});
      }
      rev.reset();
   }

   // Gate predicate: returning false suppresses on_invoice / on_order /
   // on_lineitem for the entire custkey group (on_group_end still fires).
   bool on_customer(Integer /*ck*/, const customer_coli_t& c) {
      if (stats) stats->customers_scanned++;
      auto sm  = std::string_view(c.c_mktsegment.data, c.c_mktsegment.length);
      auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
      mktsegment_ok = (sm == psm);
      if (mktsegment_ok && stats) stats->customers_passing_filter++;
      return mktsegment_ok;
   }

   // Accumulate open-due sub-aggregate before any order rows arrive
   // (guaranteed by invoice tag = 2 < orders tag = 3 in byte-lex order).
   void on_invoice(const invoice_coli_t::Key&, const invoice_coli_t& i) {
      if (stats) stats->invoices_scanned++;
      Numeric before = open_due.value;
      open_due.consume_invoice(i);
      if (open_due.value != before && stats) stats->invoices_passing_filter++;
   }

   // First on_order call: finalise threshold_ok (open_due is complete).
   // Subsequent calls: flush the previous order register, open a new one
   // only when both the date filter and the threshold pass.
   void on_order(const orders_coli_t::Key& k, const orders_coli_t& o) {
      if (stats) stats->orders_scanned++;
      if (!threshold_ok) {
         // First order for this custkey — evaluate threshold now that all
         // invoices have been consumed.
         threshold_ok = (open_due.value > params.threshold);
         if (!threshold_ok) {
            // Request a physical custkey-skip from the walker; the rest
            // of the OL sub-tree is rejected by the aggregate-output
            // filter on cust_open_due (Q3I `oi.cust_open_due > :threshold`).
            skip_group_pending = true;
            return;
         }
      } else {
         flush_order();  // close previous order before opening a new one
      }
      if (o.o_orderdate >= params.orderdate) {
         // Date filter failed: skip this order's lineitems without dispatch.
         // The walker will forward-iterate past them when wants_skip_order()
         // fires. have_open_order stays false so flush_order() is a no-op.
         skip_order_pending = true;
         have_open_order    = false;
         return;
      }
      if (stats) {
         stats->orders_passing_filter++;
         // join1_output_rows is a S1/S4 chain-join counter; S3 is a fused
         // walk so this counter stays at zero — see §Cardinality structure.
      }
      have_open_order  = true;
      cur_orderkey     = k.orderkey;
      cur_orderdate    = o.o_orderdate;
      cur_shippriority = o.o_shippriority;
   }

   // Accumulate revenue; no-op when threshold failed or order was filtered.
   void on_lineitem(const lineitem_coli_t::Key&, const lineitem_coli_t& l) {
      if (stats) stats->lineitems_scanned++;
      if (!threshold_ok || !have_open_order) return;
      Numeric before = rev.revenue;
      rev.consume(l, params);
      if (rev.revenue != before && stats) {
         stats->lineitems_passing_filter++;
         stats->join_callbacks++;
      }
   }

   // Emit last open order for this group, then reset per-group state.
   void on_group_end(Integer /*ck*/) {
      if (threshold_ok) flush_order();
      mktsegment_ok  = false;
      threshold_ok   = false;
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

// LineitemRevenueAggregator: emits one (lineitem_agg_t::Key, lineitem_agg_t)
// per (custkey, orderkey) group from custkey-sorted split_lineitem records.
// l_shipdate filter fused at consume time.
template <typename Backend>
class LineitemRevenueAggregator
{
   using Scanner = decltype(std::declval<typename Backend::template Adapter<lineitem_coli_t>>().getScanner());
   Scanner scanner_;
   const Params& params_;

   std::optional<std::pair<lineitem_agg_t::Key, lineitem_agg_t>> pending_;

   Integer cur_custkey_  = -1;
   Integer cur_orderkey_ = -1;
   LineitemRevenueAccumulator acc_;

   std::optional<std::pair<lineitem_coli_t::Key, lineitem_coli_t>> lookahead_;
   bool exhausted_ = false;

   void flush_group()
   {
      if (cur_custkey_ < 0) return;
      // Emit only groups with non-zero revenue (no revenue means all lineitems
      // were filtered out; suppress so downstream BMJ skips the orderkey).
      if (acc_.revenue > Numeric(0)) {
         pending_ = {lineitem_agg_t::Key{cur_custkey_, cur_orderkey_},
                     lineitem_agg_t{acc_.revenue}};
      }
      acc_.reset();
   }

  public:
   explicit LineitemRevenueAggregator(
       typename Backend::template Adapter<lineitem_coli_t>& adapter,
       const Params& params)
       : scanner_(adapter.getScanner()), params_(params)
   {
      lookahead_ = scanner_->next();
      if (!lookahead_) exhausted_ = true;
   }

   // Forward-only seek to the first lineitem with custkey >= K. Drops any
   // partial-group accumulation for custkeys < K. See
   // CustomerOpenDueAggregator::seek_to_custkey for the rationale (G1 S1).
   bool seek_to_custkey(Integer K)
   {
      if (cur_custkey_ >= K) return false;
      if (lookahead_ && lookahead_->first.custkey >= K) return false;
      pending_ = std::nullopt;
      acc_.reset();
      cur_custkey_  = -1;
      cur_orderkey_ = -1;
      typename lineitem_coli_t::Key seek_key{K, 0, 0, 0};
      scanner_->seek(seek_key);
      lookahead_ = scanner_->next();
      if (!lookahead_) exhausted_ = true;
      return true;
   }

   std::optional<std::pair<lineitem_agg_t::Key, lineitem_agg_t>> next()
   {
      if (pending_) {
         auto out = std::move(pending_);
         pending_ = std::nullopt;
         return out;
      }
      if (exhausted_ && cur_custkey_ < 0) return std::nullopt;

      for (;;) {
         if (lookahead_) {
            auto& [k, v] = *lookahead_;
            bool same_group = (k.custkey == cur_custkey_ && k.orderkey == cur_orderkey_);
            if (!same_group) {
               bool had_group = (cur_custkey_ >= 0);
               flush_group();
               cur_custkey_  = k.custkey;
               cur_orderkey_ = k.orderkey;
               acc_.consume(v, params_);
               lookahead_ = scanner_->next();
               if (!lookahead_) exhausted_ = true;
               if (had_group && pending_) {
                  auto out = std::move(pending_);
                  pending_ = std::nullopt;
                  return out;
               }
               continue;
            }
            acc_.consume(v, params_);
            lookahead_ = scanner_->next();
            if (!lookahead_) exhausted_ = true;
         } else {
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
   // All remaining Visitor fields have in-class default initializers; only
   // params and out lack defaults so they are named explicitly.
   COLIGroupWalkVisitor v{.params = params, .out = out, .stats = stats};
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
   // Mirror the aggregator_rows_out semantics from the other paths so the
   // [card] table reports a non-zero `agg` column for S3.
   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   // Apply top-10 ordered by revenue DESC outside the pipeline
   // (OPERATORS.md §3 op 8–9).  o_orderdate ASC, then o_orderkey ASC as
   // tiebreakers keep partial_sort deterministic across paths whose input
   // ordering differs (view scan vs. hash-map iteration vs. merged scan).
   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      apply_topN(out, 10, [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
         if (a.revenue    != b.revenue)    return a.revenue    > b.revenue;
         if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
         return a.o_orderkey < b.o_orderkey;
      });
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
   // Why only agg_inv: BMJ#1's right side (cust_open_due) is 1:1 per
   // custkey — exactly one open_due per customer. By the time bmj1's
   // refill_current_key advances next_left via fetch_cust, the previous
   // customer's open_due has been emplaced and the next agg_inv.next()
   // is what we want to align with the new customer.
   //
   // Why NOT ord_scan / agg_lin: BMJ#2/#3 right sides are 1:N (multiple
   // orders / lineitems per custkey). When fetch_cust is called from
   // inside bmj1's refill, bmj2's cached next_right may point at one of
   // many K_prev orders that haven't been emplaced yet — seeking ahead
   // would skip the rest. The S3 COLI walker avoids this because it has
   // a single scanner with no intermediate caching layer. Mirroring the
   // S3 trick on a 3-BMJ chain would require either (a) a buffered
   // OrdersByCustkey wrapper or (b) intrusive BMJ changes; both are
   // bigger refactors deferred until A/B-1 measures the agg_inv-only win.
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
         out.push_back({orderkey, lagg.revenue, o.o_orderdate,
                        o.o_shippriority, due.cust_open_due});
      }
   }

   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      apply_topN(out, 10, [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
         if (a.revenue     != b.revenue)     return a.revenue     > b.revenue;
         if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
         return a.o_orderkey < b.o_orderkey;
      });
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
      }
      out.push_back({cur_orderkey, rev.revenue, cur_orderdate,
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

   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      apply_topN(out, 10, [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
         if (a.revenue     != b.revenue)     return a.revenue     > b.revenue;
         if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
         return a.o_orderkey < b.o_orderkey;
      });
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
                  lin_scan->seek(lineitem_t::Key{*sit, Integer(0)});
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

   for (auto& [_, row] : per_order) {
      if (row.revenue > Numeric(0)) out.push_back(row);
   }

   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      apply_topN(out, 10, [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
         if (a.revenue     != b.revenue)     return a.revenue     > b.revenue;
         if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
         return a.o_orderkey < b.o_orderkey;
      });
   }
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_aggregated(std::vector<q3i_agg_row_t>& out)
{
   // S5: scan the aCOLI 2-type MI (customer_acoli_t + orders_acoli_t).
   //
   // The aCOLI MI pre-bakes two aggregates at load time:
   //   customer_acoli_t.pre_open_due  = SUM(i_totaldue WHERE i_status='O')
   //   orders_acoli_t.pre_revenue     = SUM(l_extendedprice*(1-l_discount)
   //                                        WHERE l_shipdate > DATE_1995_03_15)
   //
   // Query logic per customer group:
   //   1. Check c_mktsegment filter.
   //   2. Check pre_open_due > threshold.
   //   3. For each order under that customer: check o_orderdate < orderdate
   //      AND pre_revenue > 0 (revenue guard; mirrors S1/S3/S4 zero-revenue
   //      suppression — an order with pre_revenue=0 had no qualifying lineitems).
   //   4. Emit directly (no accumulators needed — aggregates are pre-built).
   //
   // Because there are no invoice or lineitem rows in the aCOLI MI, the scan
   // cardinality is |customer| + |orders| ≈ 1.15M at SF=1, vs ~8M for COLI.
   //
   // Parity caveat: pre_open_due and pre_revenue are baked in with the default
   // constants (i_status='O', l_shipdate > DATE_1995_03_15).  If params deviate
   // from these defaults, S5 results will diverge from S1/S3/S4 — see test
   // harness for the runtime check and [SKIP S5] guard.
   out.clear();
   Q3IPerfCapture<Backend> _pc(MICRO_PERF_STATS(*this));

   // Scan aCOLI MI by custkey order.  The MI's key sort order is:
   //   customer_acoli_t (Key = custkey)  < orders_acoli_t (Key = custkey,orderkey)
   // because the simple fold-length of customer (4 bytes) is shorter than the
   // order fold-length (8 bytes) — standard fold-length discrimination.
   //
   // We drive the scan via two typed scans (one per record type) interleaved
   // by custkey, mirroring how coli_group_walk works but for 2 types only.
   // G5: branch on FLAGS_acoli_projected. Visitor logic is identical
   // structurally — only the record types differ — so we factor it
   // into a templated lambda parameterised by (CustT, OrdT).
   auto run_walk = [&](auto& mi, auto cust_tag, auto ord_tag) {
      using CustT = decltype(cust_tag);
      using OrdT  = decltype(ord_tag);
      auto scanner = mi.template getScanner<typename CustT::Key, CustT>();

      bool    cust_passes  = false;
      Numeric cur_open_due = 0;

      while (auto kv = scanner->next()) {
         std::visit(
             [&](auto&& val) {
                using V = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<V, CustT>) {
                   if (stats) stats->acoli_customers_scanned++;
                   auto sm  = std::string_view(val.c_mktsegment.data,
                                               val.c_mktsegment.length);
                   auto psm = std::string_view(params.mktsegment.data,
                                               params.mktsegment.length);
                   cur_open_due = val.pre_open_due;
                   cust_passes  = (sm == psm) && (cur_open_due > params.threshold);
                   if (cust_passes && stats) stats->acoli_customers_passing_filter++;
                } else if constexpr (std::is_same_v<V, OrdT>) {
                   if (!cust_passes) return;
                   if (stats) stats->acoli_orders_scanned++;
                   if (val.o_orderdate >= params.orderdate) return;
                   if (val.pre_revenue <= Numeric(0)) return;
                   const auto* ok = std::get_if<typename OrdT::Key>(&kv->first);
                   if (!ok) return;
                   if (stats) stats->acoli_orders_emitted++;
                   out.push_back({ok->orderkey, val.pre_revenue,
                                  val.o_orderdate, val.o_shippriority, cur_open_due});
                }
             },
             kv->second);
      }
   };

   {
      StageTimer t(stats ? &stats->stage_us_join : nullptr);
      if (FLAGS_acoli_projected) {
         run_walk(coli.acoli_proj_adapter(),
                  customer_acoli_q3i_t{}, orders_acoli_q3i_t{});
      } else {
         run_walk(coli.acoli_adapter(),
                  customer_acoli_t{}, orders_acoli_t{});
      }
   }

   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   {
      StageTimer t(stats ? &stats->stage_us_topN : nullptr);
      apply_topN(out, 10, [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
         if (a.revenue     != b.revenue)     return a.revenue     > b.revenue;
         if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
         return a.o_orderkey < b.o_orderkey;
      });
   }
   return static_cast<long>(out.size());
}

}  // namespace tpch::q3i
