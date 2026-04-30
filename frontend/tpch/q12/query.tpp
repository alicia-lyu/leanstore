// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §3 op 4 (load vs query)            — keep query-time joins on shared JoinState
//   §5 (Q12 worked example)            — inline sketches for query_by_*
//   §6 (comparison-integrity rules)    — read before changing join strategy
//
// Template method bodies for Q12Workload<Backend> query methods,
// Params::defaults(), q12_agg_row_t::print(), and predicate stubs.
//
// Per-structure wrappers (BaseQ12 / ViewQ12 / ...) are aliases to the shared
// tpch::BaseStructure / ViewStructure / ... templates defined in
// frontend/tpch/per_structure_workload.hpp, whose method bodies forward
// directly to w.query_by_*() and w.get_size() — no extra definitions needed.

#pragma once

#include <algorithm>
#include <ostream>
#include <string_view>

#include "../../shared/merge-join/binary_merge_join.hpp"
#include "../../shared/merge-join/hash_join.hpp"
#include "../../shared/merge-join/premerged_join.hpp"

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// Params::defaults — TPC-H §2.4.12 validation values.

inline Params Params::defaults()
{
   return {
       Varchar<10>("MAIL"),
       Varchar<10>("SHIP"),
       DATE_1994_01_01,
       DATE_1995_01_01,
   };
}

// ---------------------------------------------------------------------------
// q12_agg_row_t::print — tab-separated: shipmode, high_line_count, low_line_count.

inline void q12_agg_row_t::print(std::ostream& os) const
{
   os << std::string_view(l_shipmode.data, l_shipmode.length) << "\t"
      << high_line_count << "\t"
      << low_line_count << "\n";
}

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to a raw lineitem before joining (structures 1 and 4).
// See: OPERATORS.md §5 Q12; q12/CLAUDE.md §Q12-Specific Operator Configurations.
inline bool q12_predicate_lineitem(const lineitem_t& l, const Params& p)
{
   auto sm  = std::string_view(l.l_shipmode.data, l.l_shipmode.length);
   auto sm1 = std::string_view(p.shipmode1.data, p.shipmode1.length);
   auto sm2 = std::string_view(p.shipmode2.data, p.shipmode2.length);
   return (sm == sm1 || sm == sm2)
       && l.l_shipdate    < l.l_commitdate
       && l.l_commitdate  < l.l_receiptdate
       && l.l_receiptdate >= p.receiptdate_lo
       && l.l_receiptdate <  p.receiptdate_hi;
}

// Applied to a fully-assembled joined_ol_t (structures 2 and 3).
// Delegates to q12_predicate_lineitem to keep both paths semantically identical
// (defends OPERATORS.md §6.1: same predicate across S1–S4).
inline bool q12_predicate_joined(const joined_ol_t& j, const Params& p)
{
   return q12_predicate_lineitem(j.line(), p);
}

// ---------------------------------------------------------------------------
// Shared helpers

// Returns true for the two "high priority" order priorities in Q12.
static inline bool is_high_priority(const Varchar<15>& p)
{
   auto s = std::string_view(p.data, p.length);
   return s == "1-URGENT" || s == "2-HIGH";
}

// ---------------------------------------------------------------------------
// Q12Workload query methods

template <typename Backend>
long Q12Workload<Backend>::query_by_base(std::vector<q12_agg_row_t>& out)
{
   // S1: BinaryMergeJoin over base ORDERS and LINEITEM scanners.
   // Filter-pushdown on the LINEITEM side: the fetch lambda skips rows that
   // fail q12_predicate_lineitem so the join callback never sees them.
   // OPERATORS.md §3 op 4 (S1), §5 Q12.

   struct slot { Varchar<10> mode; Integer high = 0, low = 0; };
   slot a{params.shipmode1}, b{params.shipmode2};

   auto bump = [&](const orders_t& o, const lineitem_t& l) {
      auto sm   = std::string_view(l.l_shipmode.data, l.l_shipmode.length);
      auto a_sv = std::string_view(a.mode.data, a.mode.length);
      auto b_sv = std::string_view(b.mode.data, b.mode.length);
      slot* s = (sm == a_sv) ? &a : (sm == b_sv) ? &b : nullptr;
      if (!s) return;
      if (is_high_priority(o.o_orderpriority)) s->high += 1;
      else                                     s->low  += 1;
   };

   auto emit_and_sort = [&]() -> long {
      out.clear();
      out.push_back({a.mode, a.high, a.low});
      out.push_back({b.mode, b.high, b.low});
      std::sort(out.begin(), out.end(), [](const auto& x, const auto& y) {
         return std::string_view(x.l_shipmode.data, x.l_shipmode.length)
              < std::string_view(y.l_shipmode.data, y.l_shipmode.length);
      });
      if (stats) stats->aggregator_rows_out = static_cast<long>(out.size());
      return static_cast<long>(out.size());
   };

   auto os = orders.getScanner();
   auto ls = lineitem.getScanner();

   auto fetch_orders = [&]() { return os->next(); };
   auto fetch_lineitem = [&]() -> std::optional<std::pair<lineitem_t::Key, lineitem_t>> {
      while (auto kv = ls->next()) {
         if (stats) stats->lineitems_scanned++;
         if (q12_predicate_lineitem(kv->second, params)) {
            if (stats) stats->lineitems_passed++;
            return kv;
         }
      }
      return std::nullopt;
   };

   BinaryMergeJoin<ol_sort_key_t, joined_ol_t, orders_t, lineitem_t>
       joiner(fetch_orders, fetch_lineitem,
              [&](const joined_ol_t::Key&, const joined_ol_t& jr) {
                 if (stats) stats->join_callbacks++;
                 bump(jr.order(), jr.line());
              });
   joiner.run();
   return emit_and_sort();
}

template <typename Backend>
long Q12Workload<Backend>::query_by_view(std::vector<q12_agg_row_t>& out)
{
   // S2: scan the materialized pipeline view (unfiltered joined_ol_t rows).
   // The view stores unfiltered rows (predicate hoisting per OPERATORS.md §4
   // Q12 bullet); apply q12_predicate_joined post-scan.
   // OPERATORS.md §3 op 4 (S2).

   struct slot { Varchar<10> mode; Integer high = 0, low = 0; };
   slot a{params.shipmode1}, b{params.shipmode2};

   auto bump = [&](const orders_t& o, const lineitem_t& l) {
      auto sm   = std::string_view(l.l_shipmode.data, l.l_shipmode.length);
      auto a_sv = std::string_view(a.mode.data, a.mode.length);
      auto b_sv = std::string_view(b.mode.data, b.mode.length);
      slot* s = (sm == a_sv) ? &a : (sm == b_sv) ? &b : nullptr;
      if (!s) return;
      if (is_high_priority(o.o_orderpriority)) s->high += 1;
      else                                     s->low  += 1;
   };

   auto emit_and_sort = [&]() -> long {
      out.clear();
      out.push_back({a.mode, a.high, a.low});
      out.push_back({b.mode, b.high, b.low});
      std::sort(out.begin(), out.end(), [](const auto& x, const auto& y) {
         return std::string_view(x.l_shipmode.data, x.l_shipmode.length)
              < std::string_view(y.l_shipmode.data, y.l_shipmode.length);
      });
      if (stats) stats->aggregator_rows_out = static_cast<long>(out.size());
      return static_cast<long>(out.size());
   };

   auto vs = pipeline_view.getScanner();
   while (auto kv = vs->next()) {
      if (stats) stats->lineitems_scanned++;
      const joined_ol_t& jr = kv->second;
      if (!q12_predicate_joined(jr, params)) continue;
      if (stats) { stats->lineitems_passed++; stats->join_callbacks++; }
      bump(jr.order(), jr.line());
   }
   return emit_and_sort();
}

template <typename Backend>
long Q12Workload<Backend>::query_by_merged(std::vector<q12_agg_row_t>& out)
{
   // S3: PremergedJoin over MI[0] (merged ORDERS x LINEITEM index).
   // Post-join filter via q12_predicate_joined.
   // OPERATORS.md §3 op 4 (S3), §5 Q12.

   struct slot { Varchar<10> mode; Integer high = 0, low = 0; };
   slot a{params.shipmode1}, b{params.shipmode2};

   auto bump = [&](const orders_t& o, const lineitem_t& l) {
      auto sm   = std::string_view(l.l_shipmode.data, l.l_shipmode.length);
      auto a_sv = std::string_view(a.mode.data, a.mode.length);
      auto b_sv = std::string_view(b.mode.data, b.mode.length);
      slot* s = (sm == a_sv) ? &a : (sm == b_sv) ? &b : nullptr;
      if (!s) return;
      if (is_high_priority(o.o_orderpriority)) s->high += 1;
      else                                     s->low  += 1;
   };

   auto emit_and_sort = [&]() -> long {
      out.clear();
      out.push_back({a.mode, a.high, a.low});
      out.push_back({b.mode, b.high, b.low});
      std::sort(out.begin(), out.end(), [](const auto& x, const auto& y) {
         return std::string_view(x.l_shipmode.data, x.l_shipmode.length)
              < std::string_view(y.l_shipmode.data, y.l_shipmode.length);
      });
      if (stats) stats->aggregator_rows_out = static_cast<long>(out.size());
      return static_cast<long>(out.size());
   };

   auto scanner = ol.merged_scanner();
   using PJ = PremergedJoin<decltype(*scanner), ol_sort_key_t, joined_ol_t,
                            orders_t, lineitem_t>;

   // Predicate pushdown into the scanner: drops non-matching lineitems
   // *before* they enter records_to_join, so the per-orderkey cartesian
   // product fires for ~25 lineitems instead of ~6000. This puts S3 on
   // equal footing with S1/S4, which both filter lineitems in their fetch
   // lambdas (OPERATORS.md §6.1: same predicate across S1–S4).
   //
   // Orders are admitted unconditionally; the filter targets the lineitem
   // side. An order whose lineitems all get rejected sits in records_to_join
   // until the next JK transition, where refresh() clears it via a
   // zero-cardinality cartesian product (early-out in JoinState).
   auto admit_lineitem = [this](const typename PJ::K&, const typename PJ::V& v) -> bool {
      if (stats) stats->variants_scanned++;
      if (std::holds_alternative<lineitem_t>(v)) {
         if (stats) stats->lineitems_scanned++;
         bool ok = q12_predicate_lineitem(std::get<lineitem_t>(v), params);
         if (ok && stats) { stats->lineitems_passed++; stats->lineitems_admitted++; }
         return ok;
      }
      return true;  // admit orders unconditionally
   };

   PJ joiner(*scanner,
             [&](const joined_ol_t::Key&, const joined_ol_t& jr) {
                if (stats) stats->join_callbacks++;
                // Predicate already enforced by admit_lineitem; no post-filter.
                bump(jr.order(), jr.line());
             },
             admit_lineitem);
   joiner.run();
   return emit_and_sort();
}

template <typename Backend>
long Q12Workload<Backend>::query_by_hash(std::vector<q12_agg_row_t>& out)
{
   // S4: HashJoin baseline. Build side = ORDERS; probe side = filtered LINEITEM.
   // Same filter pushdown as S1 (OPERATORS.md §6.1: same predicate across S1–S4).
   // OPERATORS.md §3 op 4 (S4 baseline), §5 Q12.

   struct slot { Varchar<10> mode; Integer high = 0, low = 0; };
   slot a{params.shipmode1}, b{params.shipmode2};

   auto bump = [&](const orders_t& o, const lineitem_t& l) {
      auto sm   = std::string_view(l.l_shipmode.data, l.l_shipmode.length);
      auto a_sv = std::string_view(a.mode.data, a.mode.length);
      auto b_sv = std::string_view(b.mode.data, b.mode.length);
      slot* s = (sm == a_sv) ? &a : (sm == b_sv) ? &b : nullptr;
      if (!s) return;
      if (is_high_priority(o.o_orderpriority)) s->high += 1;
      else                                     s->low  += 1;
   };

   auto emit_and_sort = [&]() -> long {
      out.clear();
      out.push_back({a.mode, a.high, a.low});
      out.push_back({b.mode, b.high, b.low});
      std::sort(out.begin(), out.end(), [](const auto& x, const auto& y) {
         return std::string_view(x.l_shipmode.data, x.l_shipmode.length)
              < std::string_view(y.l_shipmode.data, y.l_shipmode.length);
      });
      if (stats) stats->aggregator_rows_out = static_cast<long>(out.size());
      return static_cast<long>(out.size());
   };

   auto os = orders.getScanner();
   auto ls = lineitem.getScanner();

   auto fetch_orders = [&]() {
      auto kv = os->next();
      if (kv && stats) stats->orders_built++;
      return kv;
   };
   auto fetch_lineitem = [&]() -> std::optional<std::pair<lineitem_t::Key, lineitem_t>> {
      while (auto kv = ls->next()) {
         if (stats) stats->lineitems_scanned++;
         if (q12_predicate_lineitem(kv->second, params)) {
            if (stats) stats->lineitems_passed++;
            return kv;
         }
      }
      return std::nullopt;
   };

   HashJoin<ol_sort_key_t, joined_ol_t, orders_t, lineitem_t>
       joiner(fetch_orders, fetch_lineitem, ol_sort_key_t::max(),
              [&](const joined_ol_t::Key&, const joined_ol_t& jr) {
                 if (stats) stats->join_callbacks++;
                 bump(jr.order(), jr.line());
              });
   joiner.run();
   return emit_and_sort();
}

}  // namespace tpch::q12
