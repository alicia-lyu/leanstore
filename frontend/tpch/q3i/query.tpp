// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Template method bodies for Q3IWorkload<Backend> query methods,
// Params::defaults(), q3i_agg_row_t::print(), and predicate implementations.

#pragma once

#include <ostream>
#include <string_view>

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
// Q3IWorkload query methods

template <typename Backend>
long Q3IWorkload<Backend>::query_by_merged(std::vector<q3i_agg_row_t>& out)
{
   // S3: COLIGroupWalk visitor over the 4-table COLI MI.
   //
   // Byte-lex order within a custkey group (after tag reorder in views_coli.hpp):
   //   customer → invoice* → (orders → lineitems*)+
   //
   // This means cust_open_due is fully accumulated before any on_order fires,
   // enabling a single streaming pass with no buffering or hashmaps.
   //
   // The Visitor delegates consume_invoice / consume to the standalone
   // accumulators above. Phase 2's S1 path reuses the same accumulators.
   // Outside-pipeline threshold filter applied at flush_order time
   // (OPERATORS.md §3 op 7).

   struct Visitor {
      const Params& params;
      std::vector<q3i_agg_row_t>& out;

      // Inside-pipeline accumulators (factored — S1 reuses these in Phase 2).
      CustomerOpenDueAccumulator open_due;
      LineitemRevenueAccumulator rev;

      // Per-custkey gate: set by on_customer, cleared by on_group_end.
      bool mktsegment_ok = false;

      // Current open order register: reset per order, flushed at next order
      // or at on_group_end. Lineitems are co-located under their parent order
      // in COLI byte order, so a single register suffices.
      bool      have_open_order = false;
      Integer   cur_orderkey;
      Timestamp cur_orderdate;
      Integer   cur_shippriority;

      // Emit the current order if it passes the outside-pipeline threshold
      // filter (OPERATORS.md §3 op 7) and reset per-order state.
      void flush_order() {
         if (!have_open_order) return;
         if (open_due.value > params.threshold) {
            out.push_back({cur_orderkey, rev.revenue, cur_orderdate,
                           cur_shippriority, open_due.value});
         }
         have_open_order = false;
         rev.reset();
      }

      // Gate predicate: return false to suppress on_invoice / on_order /
      // on_lineitem for the entire group (on_group_end is still called).
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

      // Single-table filter on o_orderdate; open the per-order register.
      void on_order(const orders_coli_t::Key& k, const orders_coli_t& o) {
         flush_order();
         if (o.o_orderdate >= params.orderdate) return;  // single-table filter
         have_open_order  = true;
         cur_orderkey     = k.orderkey;
         cur_orderdate    = o.o_orderdate;
         cur_shippriority = o.o_shippriority;
      }

      // Accumulate revenue for the open order; silently skip if no open order
      // (the parent order was filtered out by o_orderdate).
      void on_lineitem(const lineitem_coli_t::Key&, const lineitem_coli_t& l) {
         if (!have_open_order) return;
         rev.consume(l, params);
      }

      // Emit last open order for this group, then reset per-group state.
      void on_group_end(Integer /*ck*/) {
         flush_order();
         mktsegment_ok = false;
         open_due.reset();
      }
   };

   out.clear();
   // All remaining Visitor fields have in-class default initializers; only
   // params and out lack defaults so they are named explicitly.
   Visitor v{.params = params, .out = out};
   coli_group_walk<Backend>(coli.merged_adapter(), v);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3IWorkload<Backend>::query_by_base(std::vector<q3i_agg_row_t>& out)
{
   // S1: BinaryMergeJoin over custkey-sorted secondary indexes.
   //
   // Phase 2 deliverable — reuses CustomerOpenDueAccumulator and
   // LineitemRevenueAccumulator (declared above) over custkey-sorted
   // secondary scanners.
   //
   // OPERATORS.md §3 op 4 (S1): pre-build cust_open_due via stream scan
   // of invoice_secondary sorted by custkey; BinaryMergeJoin(OL) over
   // orders_secondary / lineitem_secondary; CUSTOMER hash lookup.
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
