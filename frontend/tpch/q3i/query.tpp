// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 Filter Pushdown            — predicate hoisting / short-circuit rules
//   §7 (comparison-integrity)     — read before changing join strategy
//
// Template method bodies for Q3IWorkload<Backend> query methods,
// Params::defaults(), q3i_agg_row_t::print(), and predicate implementations.

#pragma once

#include <ostream>
#include <string_view>

#include "../operators.hpp"

namespace tpch::q3i
{

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
   os << "view(" << revenue << "," << cust_open_due << "," << o_orderdate
      << "," << o_shippriority << ")\n";
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

struct COLIGroupWalkVisitor {
   const Params& params;
   std::vector<q3i_agg_row_t>& out;

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
      // threshold_ok already confirmed — emit unconditionally.
      out.push_back({cur_orderkey, rev.revenue, cur_orderdate,
                     cur_shippriority, open_due.value});
      have_open_order = false;
      rev.reset();
   }

   // Gate predicate: returning false suppresses on_invoice / on_order /
   // on_lineitem for the entire custkey group (on_group_end still fires).
   bool on_customer(Integer /*ck*/, const customer_coli_t& c) {
      auto sm  = std::string_view(c.c_mktsegment.data, c.c_mktsegment.length);
      auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
      mktsegment_ok = (sm == psm);
      return mktsegment_ok;
   }

   // Accumulate open-due sub-aggregate before any order rows arrive
   // (guaranteed by invoice tag = 2 < orders tag = 3 in byte-lex order).
   void on_invoice(const invoice_coli_t::Key&, const invoice_coli_t& i) {
      open_due.consume_invoice(i);
   }

   // First on_order call: finalise threshold_ok (open_due is complete).
   // Subsequent calls: flush the previous order register, open a new one
   // only when both the date filter and the threshold pass.
   void on_order(const orders_coli_t::Key& k, const orders_coli_t& o) {
      if (!threshold_ok) {
         // First order for this custkey — evaluate threshold now that all
         // invoices have been consumed.
         threshold_ok = (open_due.value > params.threshold);
         if (!threshold_ok) return;  // skip entire OL sub-hierarchy
      } else {
         flush_order();  // close previous order before opening a new one
      }
      if (o.o_orderdate >= params.orderdate) return;  // single-table date filter
      have_open_order  = true;
      cur_orderkey     = k.orderkey;
      cur_orderdate    = o.o_orderdate;
      cur_shippriority = o.o_shippriority;
   }

   // Accumulate revenue; no-op when threshold failed or order was filtered.
   void on_lineitem(const lineitem_coli_t::Key&, const lineitem_coli_t& l) {
      if (!threshold_ok || !have_open_order) return;
      rev.consume(l, params);
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
   // All remaining Visitor fields have in-class default initializers; only
   // params and out lack defaults so they are named explicitly.
   COLIGroupWalkVisitor v{.params = params, .out = out};
   coli_group_walk<Backend>(coli.merged_adapter(), v);
   // Apply top-10 ordered by revenue DESC outside the pipeline
   // (OPERATORS.md §3 op 8–9).
   apply_topN(out, 10, [](const q3i_agg_row_t& a, const q3i_agg_row_t& b) {
      return a.revenue > b.revenue;
   });
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_base(std::vector<q3i_agg_row_t>& out)
{
   // S1: BinaryMergeJoin over custkey-sorted split COLI indexes.
   //
   // Phase 2 deliverable — reuses CustomerOpenDueAccumulator and
   // LineitemRevenueAccumulator (declared above) over the custkey-sorted
   // split scanners exposed by COLIPipeline.
   //
   // OPERATORS.md §3 op 4 (S1): pre-build cust_open_due via stream scan
   // of split_invoice (custkey-sorted); BinaryMergeJoin(OL) over
   // split_orders / split_lineitem; CUSTOMER hash lookup.
   out.clear();
   return 0;  // TODO Phase 2
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_view(std::vector<q3i_agg_row_t>& out)
{
   // S2: scan materialized pipeline view (joined_ol_t rows, unfiltered).
   //
   // Phase 2 deliverable — apply q3i_predicate_joined post-scan; look up
   // cust_open_due from a pre-built invoice map (OPERATORS.md §3 op 4 S2).
   out.clear();
   return 0;  // TODO Phase 2
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_hash(std::vector<q3i_agg_row_t>& out)
{
   // S4: HashJoin baseline.
   //
   // Phase 2 deliverable — pre-build cust_open_due hashmap from INVOICE;
   // HashJoin(OL) + CUSTOMER hash lookup; reuses CustomerOpenDueAccumulator
   // and LineitemRevenueAccumulator (OPERATORS.md §3 op 4 S4 baseline).
   out.clear();
   return 0;  // TODO Phase 2
}

}  // namespace tpch::q3i
