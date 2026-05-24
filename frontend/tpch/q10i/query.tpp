// Template method bodies for Q10IWorkload<Backend> query methods,
// Params::defaults(), q10i_pipeline_view_t::print(), q10i_agg_row_t::print(),
// and predicate implementations.
// Operator-translation reference: see ../OPERATORS.md §3.
//
// Phase 1: all query_by_* return empty results (XOR digest = 0x0).
// Real bodies land in Phase 4 §7.1 (S3), §7.2 (S1), §7.3 (S2), §7.5 (S4).

#pragma once

#include <gflags/gflags.h>
#include <ostream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../operators.hpp"
#include "../tpch_tables.hpp"
#include "../tpch_family/walk_action.hpp"
#include "../tpchi_family/coli_pipeline.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q10i
{

// ---------------------------------------------------------------------------
// Q10I SUBSTITUTION-PARAMETER rotation table — 24 entries, byte-identical
// to Q10's PARAM_TABLE (both queries share the TPC-H §2.4.10 domain).
//
// Domain: first day of a month between 1993-02-01 and 1995-01-01 inclusive
// (24 valid month starts). Validation default = 1993-10-01 (first entry).
//
// Days since 1970-01-01. DATE_1994_01_01 = 8766 and DATE_1995_01_01 = 9131
// are defined in tpch_tables.hpp. 1993-01-01 = DATE_1994_01_01 − 365.
// All three years (1993, 1994, 1995) are non-leap; month offsets follow
// Jan(31)/Feb(28)/Mar(31)/Apr(30)/May(31)/Jun(30)/Jul(31)/Aug(31)/
// Sep(30)/Oct(31)/Nov(30)/Dec(31).
//
// Validation default first so iter=0 is always spec-valid. Rationale
// (PLAYBOOK §3.6): a fixed-param loop hid Q3I's pre_revenue bug for weeks;
// domain-full rotation surfaces param-bake regressions within one helper.run().

static constexpr Timestamp Q10I_DATE_1993_01_01 = DATE_1994_01_01 - 365;  // 8401

static constexpr Timestamp PARAM_TABLE[] = {
    Q10I_DATE_1993_01_01 + 273,  // 1993-10-01 = 8674  [validation default]
    Q10I_DATE_1993_01_01 +  31,  // 1993-02-01
    Q10I_DATE_1993_01_01 +  59,  // 1993-03-01
    Q10I_DATE_1993_01_01 +  90,  // 1993-04-01
    Q10I_DATE_1993_01_01 + 120,  // 1993-05-01
    Q10I_DATE_1993_01_01 + 151,  // 1993-06-01
    Q10I_DATE_1993_01_01 + 181,  // 1993-07-01
    Q10I_DATE_1993_01_01 + 212,  // 1993-08-01
    Q10I_DATE_1993_01_01 + 243,  // 1993-09-01
    Q10I_DATE_1993_01_01 + 304,  // 1993-11-01
    Q10I_DATE_1993_01_01 + 334,  // 1993-12-01
    DATE_1994_01_01,             // 1994-01-01 = 8766
    DATE_1994_01_01 +  31,       // 1994-02-01
    DATE_1994_01_01 +  59,       // 1994-03-01
    DATE_1994_01_01 +  90,       // 1994-04-01
    DATE_1994_01_01 + 120,       // 1994-05-01
    DATE_1994_01_01 + 151,       // 1994-06-01
    DATE_1994_01_01 + 181,       // 1994-07-01
    DATE_1994_01_01 + 212,       // 1994-08-01
    DATE_1994_01_01 + 243,       // 1994-09-01
    DATE_1994_01_01 + 273,       // 1994-10-01
    DATE_1994_01_01 + 304,       // 1994-11-01
    DATE_1994_01_01 + 334,       // 1994-12-01
    DATE_1995_01_01,             // 1995-01-01 = 9131
};

static constexpr long PARAM_TABLE_SIZE =
    static_cast<long>(sizeof(PARAM_TABLE) / sizeof(PARAM_TABLE[0]));
static_assert(PARAM_TABLE_SIZE == 24,
              "Q10I PARAM_TABLE must cover all 24 valid month starts in [1993-02-01, 1995-01-01]");

// ---------------------------------------------------------------------------

inline Params Params::defaults()
{
   return Params{PARAM_TABLE[0]};
}

// Rotate through the substitution-parameter table so each TX iteration
// exercises a distinct date. Surfaces param-bake regressions that a
// fixed-default loop would miss (PLAYBOOK §3.6 rationale).
template <typename Backend>
void Q10IWorkload<Backend>::set_params_for_iter(long iter)
{
   params.date_lo = PARAM_TABLE[iter % PARAM_TABLE_SIZE];
}

// ---------------------------------------------------------------------------
// Predicate implementations.

inline bool q10i_predicate_orders(const orders_t& o, const Params& p)
{
   // o_orderdate ∈ [date_lo, date_lo + 3 months).
   // 3 months ≈ 90 days on the spec's monthly grid (consistent with Q10).
   return o.o_orderdate >= p.date_lo
       && o.o_orderdate <  p.date_lo + 90;
}

inline bool q10i_predicate_lineitem(const lineitem_coli_t& l, const Params& /*p*/)
{
   // l_returnflag = 'R' (spec-hardcoded; kept live for view reusability —
   // not baked into any secondary so the view serves any returnflag variant).
   return l.l_returnflag.data[0] == 'R';
}

// ---------------------------------------------------------------------------
// Output formatting.

inline void q10i_pipeline_view_t::print(std::ostream& os) const
{
   os << l_extendedprice << '\t' << l_discount   << '\t'
      << l_returnflag    << '\t' << o_orderdate   << '\t'
      << i_status        << '\t'
      << c_name          << '\t' << c_nationkey   << '\n';
}

inline void q10i_agg_row_t::print(std::ostream& os) const
{
   os << c_custkey    << '\t' << c_name      << '\t'
      << paid_returns << '\t' << open_returns << '\t' << late_returns << '\t'
      << revenue      << '\t'
      << c_acctbal    << '\t' << n_name      << '\t'
      << c_address    << '\t' << c_phone     << '\t' << c_comment << '\n';
}

// ---------------------------------------------------------------------------
// Stub query bodies — Phase 1 commit 1.
// Real bodies for S2/S1/S4 land in Phase 4 §7.3/§7.2/§7.5.

// ---------------------------------------------------------------------------
// Shared aggregator: per-custkey bucket of paid/open/late/revenue plus the
// customer payload needed for the post-pipeline emit. Used by S1/S2/S4.
//
// S3 (query_by_merged) emits inline via the streaming visitor; using a map
// here for S1/S2/S4 gives identical TopN output because the comparator
// (revenue DESC, c_custkey ASC) is deterministic — order of offers to the
// sink does not affect the final top-20.
namespace q10i_detail
{

struct CustBucket {
   Numeric      paid     = 0;
   Numeric      open     = 0;
   Numeric      late     = 0;
   Numeric      revenue  = 0;
   Varchar<25>  c_name        = {};
   Varchar<40>  c_address     = {};
   Integer      c_nationkey   = 0;
   Varchar<15>  c_phone       = {};
   Numeric      c_acctbal     = 0;
   Varchar<117> c_comment     = {};
   bool         cust_loaded   = false;
};

inline void route_revenue(CustBucket& b, Numeric rev, char status)
{
   if      (status == 'P') b.paid += rev;
   else if (status == 'O') b.open += rev;
   else if (status == 'L') b.late += rev;
   b.revenue += rev;
}

template <typename NationAdapter>
inline void drain_to_sink(
    std::unordered_map<Integer, CustBucket>& by_cust,
    NationAdapter& nation,
    TopNSink<q10i_agg_row_t,
              decltype(&q10::q10_agg_row_t::cmp)>& sink)
{
   for (auto& [ck, b] : by_cust) {
      if (b.revenue <= 0) continue;
      Varchar<25> n_name_val;
      nation.lookup1(
          typename nation_t::Key{b.c_nationkey},
          [&](const nation_t& n) { n_name_val = n.n_name; });
      q10i_agg_row_t row;
      row.c_custkey    = ck;
      row.revenue      = b.revenue;
      row.c_name       = b.c_name;
      row.c_acctbal    = b.c_acctbal;
      row.n_name       = n_name_val;
      row.c_address    = b.c_address;
      row.c_phone      = b.c_phone;
      row.c_comment    = b.c_comment;
      row.paid_returns = b.paid;
      row.open_returns = b.open;
      row.late_returns = b.late;
      sink.offer(std::move(row));
   }
}

}  // namespace q10i_detail

// ---------------------------------------------------------------------------
// S1: chain-scan over the custkey-sorted COLI split secondaries.
//
// The split secondaries are already (custkey, orderkey, ...) sorted by
// construction (populate_split copies the COLI MI's order). Walk
// split_orders in custkey order; for each order: filter by the parameterised
// orderdate window, then scan split_lineitem at prefix (custkey, orderkey)
// for the order's lineitems with returnflag = 'R'; per surviving lineitem
// recover i_status via point lookup into split_invoice on
// (custkey, invoicekey); recover the customer payload from the base
// `customer` adapter via a single PK lookup per (new) custkey. Routes
// revenue into the per-custkey bucket.
//
// Equivalent to a 3-way BMJ chain (D12) collapsed to seek-driven scans
// over the already-sorted secondaries; produces the same join semantics
// without the BMJ-template machinery and matches Q5I/Q3I's S1 idiom in
// spirit (custkey-anchored, per-emit invoice recovery).
template <typename Backend>
long Q10IWorkload<Backend>::query_by_base(std::vector<q10i_agg_row_t>& out)
{
   out.clear();
   std::unordered_map<Integer, q10i_detail::CustBucket> by_cust;

   const Timestamp date_lo = params.date_lo;
   const Timestamp date_hi = params.date_lo + 90;

   auto ord_sc = coli.split_orders().getScanner();
   auto lin_sc = coli.split_lineitem().getScanner();
   auto inv_sc = coli.split_invoice().getScanner();

   while (auto okv = ord_sc->next()) {
      if (stats) stats->orders_scanned++;
      const auto& ok = okv->first;
      const auto& ov = okv->second;
      if (ov.o_orderdate < date_lo || ov.o_orderdate >= date_hi) continue;
      if (stats) stats->orders_passing_date++;

      typename lineitem_coli_t::Key lkey_seek{ok.custkey, ok.orderkey, 0, 0};
      lin_sc->seek(lkey_seek);
      while (auto lkv = lin_sc->next()) {
         if (lkv->first.custkey != ok.custkey
             || lkv->first.orderkey != ok.orderkey) break;
         if (stats) stats->lineitems_scanned++;
         if (!q10i_predicate_lineitem(lkv->second, params)) continue;
         if (stats) stats->lineitems_passing_returnflag++;

         // INL i_status from split_invoice on (custkey, invoicekey).
         typename invoice_coli_t::Key ikey_seek{ok.custkey, lkv->first.invoicekey};
         inv_sc->seek(ikey_seek);
         auto ikv = inv_sc->next();
         if (!ikv || ikv->first.custkey != ok.custkey
             || ikv->first.invoicekey != lkv->first.invoicekey) {
            throw std::runtime_error("Q10I S1: split_invoice FK miss");
         }
         const Varchar<1> i_status = ikv->second.i_status;

         auto& b = by_cust[ok.custkey];
         if (!b.cust_loaded) {
            customer.lookup1(
                typename customerh_t::Key{ok.custkey},
                [&](const customerh_t& c) {
                   b.c_name       = c.c_name;
                   b.c_address    = c.c_address;
                   b.c_nationkey  = c.c_nationkey;
                   b.c_phone      = c.c_phone;
                   b.c_acctbal    = c.c_acctbal;
                   b.c_comment    = c.c_comment;
                });
            b.cust_loaded = true;
         }
         const Numeric rev = lkv->second.l_extendedprice * (1 - lkv->second.l_discount);
         q10i_detail::route_revenue(b, rev, i_status.data[0]);
      }
   }

   TopNSink<q10i_agg_row_t, decltype(&q10::q10_agg_row_t::cmp)>
       sink(20, &q10::q10_agg_row_t::cmp);
   q10i_detail::drain_to_sink(by_cust, nation, sink);
   sink.drain_sorted(out);
   return static_cast<long>(out.size());
}

// ---------------------------------------------------------------------------
// S2: sequential scan of the q10i_pipeline_view secondary.
//
// The view rows are custkey-sorted and FD-attach the per-lineitem
// i_status (resolved at view-load time) plus the customer payload. Per-row:
// apply the parameterised orderdate window + returnflag filter, route
// revenue into the per-custkey bucket. At end, drain into the top-20
// sink with per-customer NATION INL.
template <typename Backend>
long Q10IWorkload<Backend>::query_by_view(std::vector<q10i_agg_row_t>& out)
{
   out.clear();
   std::unordered_map<Integer, q10i_detail::CustBucket> by_cust;

   const Timestamp date_lo = params.date_lo;
   const Timestamp date_hi = params.date_lo + 90;

   auto sc = pipeline_view.getScanner();
   while (auto kv = sc->next()) {
      if (stats) stats->view_rows_scanned++;
      const auto& v = kv->second;
      if (v.o_orderdate < date_lo || v.o_orderdate >= date_hi) continue;
      if (v.l_returnflag.data[0] != 'R') continue;

      auto& b = by_cust[kv->first.custkey];
      if (!b.cust_loaded) {
         b.c_name       = v.c_name;
         b.c_address    = v.c_address;
         b.c_nationkey  = v.c_nationkey;
         b.c_phone      = v.c_phone;
         b.c_acctbal    = v.c_acctbal;
         b.c_comment    = v.c_comment;
         b.cust_loaded  = true;
      }
      const Numeric rev = v.l_extendedprice * (1 - v.l_discount);
      q10i_detail::route_revenue(b, rev, v.i_status.data[0]);
   }

   TopNSink<q10i_agg_row_t, decltype(&q10::q10_agg_row_t::cmp)>
       sink(20, &q10::q10_agg_row_t::cmp);
   q10i_detail::drain_to_sink(by_cust, nation, sink);
   sink.drain_sorted(out);
   return static_cast<long>(out.size());
}

// ---------------------------------------------------------------------------
// S3: COLI group walk via bespoke Q10IGroupWalkVisitor (D4–D7).
//
// Per-custkey state:
//   - invoice_buf: unordered_map<invoicekey → i_status> built during
//     on_invoice (Pattern B Rule 10); cleared at on_group_end.
//   - Four running sums: paid_returns, open_returns, late_returns, revenue.
//   - Customer payload fields stashed at on_customer.
//   - skip_order flag: set by on_order when the orderdate predicate fails;
//     cleared at the next on_order call.
//
// NATION join (D9): per-customer INL on NATION PK at on_group_end; no
// in-memory nation map.
//
// Top-20 emit: bounded TopNSink<q10i_agg_row_t> with q10_agg_row_t::cmp
// (revenue DESC, c_custkey ASC tiebreaker). Drained into out at end of walk.

template <typename Backend>
long Q10IWorkload<Backend>::query_by_merged(std::vector<q10i_agg_row_t>& out)
{
   out.clear();

   // Bounded top-20 sink: revenue DESC, c_custkey ASC.
   // q10_agg_row_t::cmp is the comparator ("better" = higher revenue).
   // TopNSink keeps the heap min at top (worst of the top-20), so the
   // heap comparator is the same cmp (standard TopNSink convention).
   TopNSink<q10i_agg_row_t, decltype(&q10::q10_agg_row_t::cmp)>
       sink(20, &q10::q10_agg_row_t::cmp);

   // Bespoke visitor — D6: built directly on coli_group_walk, not on
   // Q3FamilyVisitor. All Q10I-specific logic is self-contained here.
   struct Q10IGroupWalkVisitor {
      // ---- external references ----
      const Params& params;
      Q10IStats*    stats;
      // nation adapter for INL PK lookup at on_group_end (D9).
      typename Backend::template Adapter<nation_t>& nation;
      // top-20 sink filled incrementally.
      TopNSink<q10i_agg_row_t, decltype(&q10::q10_agg_row_t::cmp)>& sink;

      // ---- per-group state ----
      // Stashed customer fields (set at on_customer, used at on_group_end).
      Integer      custkey       = 0;
      Varchar<25>  c_name        = {};
      Varchar<40>  c_address     = {};
      Integer      c_nationkey   = 0;
      Varchar<15>  c_phone       = {};
      Numeric      c_acctbal     = 0;
      Varchar<117> c_comment     = {};

      // Pattern B Rule 10: full invoice_coli_t::Key → i_status map, cleared
      // per group. Populated during on_invoice before any on_lineitem fires
      // (byte-lex order: invoice tag=2 < orders tag=3 < lineitem tag=4).
      std::unordered_map<Integer, Varchar<1>> invoice_buf = {};

      // Four per-customer revenue accumulators (D2).
      Numeric paid_returns = 0;
      Numeric open_returns = 0;
      Numeric late_returns = 0;
      Numeric revenue      = 0;

      // Per-order skip flag: set by on_order when orderdate predicate fails.
      // Reset at the start of each on_order call.
      bool skip_lineitems_this_order = false;

      // on_customer: stash payload fields; no per-customer filter (D9 — no
      // region gate; Q10I scans all customers). Reset revenue accumulators.
      WalkAction on_customer(Integer ck, const customer_coli_t& c)
      {
         if (stats) stats->customers_scanned++;
         custkey     = ck;
         c_name      = c.c_name;
         c_address   = c.c_address;
         c_nationkey = c.c_nationkey;
         c_phone     = c.c_phone;
         c_acctbal   = c.c_acctbal;
         c_comment   = c.c_comment;
         paid_returns = 0;
         open_returns = 0;
         late_returns = 0;
         revenue      = 0;
         skip_lineitems_this_order = false;
         return WalkAction::Continue;
      }

      // on_invoice: Pattern B — buffer full i_status keyed by invoicekey.
      // Field extraction (i_status) happens at on_lineitem, not here.
      WalkAction on_invoice(const invoice_coli_t::Key& k,
                            const invoice_coli_t&      i)
      {
         if (stats) stats->invoices_scanned++;
         invoice_buf.emplace(k.invoicekey, i.i_status);
         return WalkAction::Continue;
      }

      // on_order: apply orderdate window predicate (mirrors q10i_predicate_orders
      // logic inline because that predicate takes const orders_t& while the
      // walker delivers const orders_coli_t&; the check is a two-field compare
      // and is cleaner inlined). If the order fails, return SkipOrder so the
      // walker skips this order's lineitems automatically.
      WalkAction on_order(const orders_coli_t::Key& /*k*/,
                          const orders_coli_t&      o)
      {
         if (stats) stats->orders_scanned++;
         if (o.o_orderdate < params.date_lo
             || o.o_orderdate >= params.date_lo + 90) {
            return WalkAction::SkipOrder;
         }
         if (stats) stats->orders_passing_date++;
         return WalkAction::Continue;
      }

      // on_lineitem: apply returnflag predicate; route revenue into buckets.
      WalkAction on_lineitem(const lineitem_coli_t::Key& k,
                             const lineitem_coli_t&      l)
      {
         if (stats) stats->lineitems_scanned++;
         if (!q10i_predicate_lineitem(l, params))
            return WalkAction::Continue;
         if (stats) stats->lineitems_passing_returnflag++;

         // Revenue contribution for this lineitem.
         const Numeric rev = l.l_extendedprice * (1 - l.l_discount);

         // Route into paid/open/late via invoice_buf lookup (Pattern B).
         auto it = invoice_buf.find(k.invoicekey);
         if (it == invoice_buf.end()) {
            throw std::runtime_error(
                "Q10IGroupWalkVisitor::on_lineitem: invoice missing from "
                "buffer (FK violation); custkey-group invariant broken");
         }
         const char status = it->second.data[0];
         if      (status == 'P') paid_returns += rev;
         else if (status == 'O') open_returns += rev;
         else if (status == 'L') late_returns += rev;
         // Unknown status: skip silently (defensive; generator only emits P/O/L).
         revenue += rev;
         return WalkAction::Continue;
      }

      // on_group_end: if return_revenue > 0, emit to the top-20 sink.
      // Resolves n_name via NATION INL on PK (D9).
      void on_group_end(Integer /*ck*/)
      {
         if (stats) stats->mi_records_visited++;  // count group boundary
         if (revenue > 0) {
            if (stats) stats->aggregator_rows_out++;

            // Resolve n_name via per-customer INL on NATION PK (D9).
            Varchar<25> n_name_val;
            nation.lookup1(
                typename nation_t::Key{c_nationkey},
                [&](const nation_t& n) { n_name_val = n.n_name; });

            q10i_agg_row_t row;
            // Base fields (q10_agg_row_t).
            row.c_custkey = custkey;
            row.revenue   = revenue;
            row.c_name    = c_name;
            row.c_acctbal = c_acctbal;
            row.n_name    = n_name_val;
            row.c_address = c_address;
            row.c_phone   = c_phone;
            row.c_comment = c_comment;
            // Q10I extension fields.
            row.paid_returns = paid_returns;
            row.open_returns = open_returns;
            row.late_returns = late_returns;

            if (stats) stats->topn_offers++;
            const std::size_t before = sink.size();
            sink.offer(std::move(row));
            if (stats && sink.size() < before + 1) stats->topn_evictions++;
         }
         // Reset per-group state.
         invoice_buf.clear();
         paid_returns = 0;
         open_returns = 0;
         late_returns = 0;
         revenue      = 0;
         skip_lineitems_this_order = false;
      }
   };

   Q10IGroupWalkVisitor visitor{params, stats, nation, sink};
   coli_group_walk<Backend>(coli.merged_adapter(), visitor);

   sink.drain_sorted(out);

   // XOR-fold each emitted row into the running digest (test harness reads
   // this via the workload's stats; we accumulate inline here instead).
   // The parity gate in the harness XORs the returned rows' bytes itself —
   // no additional work needed here.

   return static_cast<long>(out.size());
}

// ---------------------------------------------------------------------------
// S4: HashJoin chain on BASE tables only (D11).
//
// Q10I has no region/nation filter, so no cust_set is needed (every
// customer is eligible). Build:
//   - ord_set     <- ORDERS scan, gated by orderdate window. PK-only
//                    (Rule 4); o_custkey recovered later via INL.
//   - invoice_set <- INVOICE scan, PK-only. i_status recovered via INL
//                    (Rule 13 — INL substitution; build adds no filtering
//                    benefit, only the per-probe B-tree cost is paid).
//
// Probe: scan LINEITEM with returnflag = 'R' filter, probe ord_set on
// orderkey. On hit, INL recover o_custkey (orders.lookup1) + customer
// payload (customer.lookup1), cached per orderkey-transition. Probe
// invoice on l_invoicekey, recover i_status. Route revenue into the
// per-custkey bucket.
template <typename Backend>
long Q10IWorkload<Backend>::query_by_hash(std::vector<q10i_agg_row_t>& out)
{
   out.clear();
   std::unordered_map<Integer, q10i_detail::CustBucket> by_cust;

   const Timestamp date_lo = params.date_lo;
   const Timestamp date_hi = params.date_lo + 90;

   // (#1) ord_set: ORDERS scan, PK-only, gated by date window.
   std::unordered_set<Integer> ord_set;
   {
      auto sc = orders.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->orders_scanned++;
         if (kv->second.o_orderdate < date_lo
             || kv->second.o_orderdate >= date_hi) continue;
         if (stats) stats->orders_passing_date++;
         ord_set.insert(kv->first.o_orderkey);
      }
   }

   // (#2) LINEITEM streaming probe with returnflag filter; per-lineitem
   // INL into ORDERS (custkey) + CUSTOMER (payload) + INVOICE (i_status),
   // with per-orderkey-transition caching of the customer recovery.
   auto sc = lineitem.getScanner();
   Integer  cached_ok        = -1;
   Integer  cached_custkey   = 0;

   while (auto kv = sc->next()) {
      if (stats) stats->lineitems_scanned++;
      if (kv->second.l_returnflag.data[0] != 'R') continue;
      if (stats) stats->lineitems_passing_returnflag++;

      const Integer ok = kv->first.l_orderkey;
      if (ord_set.count(ok) == 0) continue;

      if (ok != cached_ok) {
         orders.lookup1(typename orders_t::Key{ok},
                        [&](const orders_t& o) { cached_custkey = o.o_custkey; });
         cached_ok = ok;
      }

      auto& b = by_cust[cached_custkey];
      if (!b.cust_loaded) {
         customer.lookup1(
             typename customerh_t::Key{cached_custkey},
             [&](const customerh_t& c) {
                b.c_name       = c.c_name;
                b.c_address    = c.c_address;
                b.c_nationkey  = c.c_nationkey;
                b.c_phone      = c.c_phone;
                b.c_acctbal    = c.c_acctbal;
                b.c_comment    = c.c_comment;
             });
         b.cust_loaded = true;
      }

      Varchar<1> i_status;
      invoice.lookup1(typename invoice_t::Key{kv->second.l_invoicekey},
                      [&](const invoice_t& inv) { i_status = inv.i_status; });

      const Numeric rev = kv->second.l_extendedprice * (1 - kv->second.l_discount);
      q10i_detail::route_revenue(b, rev, i_status.data[0]);
   }

   TopNSink<q10i_agg_row_t, decltype(&q10::q10_agg_row_t::cmp)>
       sink(20, &q10::q10_agg_row_t::cmp);
   q10i_detail::drain_to_sink(by_cust, nation, sink);
   sink.drain_sorted(out);
   return static_cast<long>(out.size());
}

}  // namespace tpch::q10i
