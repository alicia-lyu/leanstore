// Template method bodies for Q5IWorkload<Backend> query methods,
// Params::defaults(), q5i_agg_row_t::print(), and predicate implementations.
// Operator-translation reference: see ../OPERATORS.md §3.
//
// Phase 0.5: all query_by_* return empty results (XOR digest = 0x0).
// Real bodies land in Phase 1+.

#pragma once

#include <algorithm>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../../shared/merge-join/binary_merge_join.hpp"
// q5/workload.hpp must precede q5/side_tables.hpp: side_tables.hpp's
// template body references a forward-declared q5::Params and won't
// parse without the Params definition ahead. visitor.hpp + out_class.hpp
// follow once both are in scope.
#include "../q5/workload.hpp"
#include "../q5/side_tables.hpp"
#include "../tpch_tables.hpp"
#include "../tpchi_family/coli_pipeline.hpp"
#include "out_class.hpp"
#include "visitor.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q5i
{

// ---------------------------------------------------------------------------
// Date constants for the 1-year orderdate window.
// DATE_1994_01_01 and DATE_1995_01_01 ship in tpch_tables.hpp.
// Local constants for the remaining years in the 5-year domain.
static constexpr Timestamp Q5I_DATE_1993_01_01 = DATE_1994_01_01 - 365;
static constexpr Timestamp Q5I_DATE_1996_01_01 = DATE_1995_01_01 + 365;
static constexpr Timestamp Q5I_DATE_1997_01_01 = DATE_1995_01_01 + 365 + 366;

struct ParamEntry {
   const char* region;
   Timestamp   date;
};

static constexpr ParamEntry PARAM_TABLE[] = {
    {"ASIA",        DATE_1994_01_01},    // validation default first
    {"AFRICA",      Q5I_DATE_1993_01_01},
    {"AMERICA",     DATE_1995_01_01},
    {"EUROPE",      Q5I_DATE_1996_01_01},
    {"MIDDLE EAST", Q5I_DATE_1997_01_01},
    {"AFRICA",      DATE_1994_01_01},
    {"AMERICA",     Q5I_DATE_1993_01_01},
    {"EUROPE",      Q5I_DATE_1997_01_01},
    {"MIDDLE EAST", DATE_1994_01_01},
    {"ASIA",        Q5I_DATE_1996_01_01},
};
static constexpr int PARAM_TABLE_SIZE =
    static_cast<int>(sizeof(PARAM_TABLE) / sizeof(PARAM_TABLE[0]));

// ---------------------------------------------------------------------------

inline Params Params::defaults()
{
   return {Varchar<25>("ASIA"), DATE_1994_01_01};
}

// Rotate through the substitution-parameter table so each TX iteration
// exercises a distinct (region, date) combination.
template <typename Backend>
void Q5IWorkload<Backend>::set_params_for_iter(long iter)
{
   const auto& e = PARAM_TABLE[iter % PARAM_TABLE_SIZE];
   params.region       = Varchar<25>(e.region);
   params.orderdate_lo = e.date;
}

// ---------------------------------------------------------------------------
// Predicate implementations.

inline bool q5i_predicate_orders(const orders_t& o, const Params& p)
{
   return o.o_orderdate >= p.orderdate_lo
       && o.o_orderdate <  p.orderdate_lo + 365;
}

// ---------------------------------------------------------------------------
// Output formatting.

inline void q5i_pipeline_view_t::print(std::ostream& os) const
{
   os << l_extendedprice << '\t' << l_discount << '\t'
      << l_suppkey << '\t' << c_nationkey << '\t'
      << o_orderdate << '\t' << i_status << '\n';
}

inline void q5i_agg_row_t::print(std::ostream& os) const
{
   os << n_name << '\t'
      << nominal_revenue   << '\t'
      << realised_revenue  << '\t'
      << open_revenue      << '\t'
      << late_revenue      << '\n';
}

// ---------------------------------------------------------------------------
// Shared sort comparator (post-OutClass step 4): nominal_revenue DESC,
// tie-break by n_name ASC for deterministic ordering across runs.

inline auto q5i_sort_cmp = [](const q5i_agg_row_t& a, const q5i_agg_row_t& b) {
   if (a.nominal_revenue != b.nominal_revenue)
      return a.nominal_revenue > b.nominal_revenue;
   return a.n_name < b.n_name;
};

// Convert q5i::Params to q5::Params for build_q5_side_tables. The two
// share the same field shape (region + orderdate_lo); no semantic
// translation needed.
inline q5::Params q5i_to_q5_params(const Params& p)
{
   return q5::Params{p.region, p.orderdate_lo};
}

// ---------------------------------------------------------------------------
// S1: 2-BMJ chain over custkey-sorted COLI split indexes.
//
//   BMJ #1: customerh_t ⋈ orders_coli_t   on custkey            → q5i_jr1_t
//   BMJ #2: q5i_jr1_t   ⋈ lineitem_coli_t on (custkey, orderkey) → q5i_jr2_t
//
// Per-emit: a per-custkey invoice buffer (advanced in lockstep with jr2's
// custkey transitions) supplies i_status by invoicekey lookup. The
// invoice scanner walks coli.split_invoice() once forward, never seeks.
//
// Pattern B (CONVENTIONS Rule 10): full invoice rows are buffered per
// custkey, i_status extracted at lineitem-assembly time.

namespace q5i_detail
{

// Per-custkey invoice buffer over the custkey-sorted split-invoice
// secondary. Advances forward in lockstep with the BMJ chain's
// custkey transitions; at each new custkey, drains that custkey's
// invoice rows into an unordered_map<invoicekey, invoice_coli_t>.
template <typename InvoiceScanner>
struct CustkeyInvoiceBuffer {
   InvoiceScanner scanner;
   std::optional<std::pair<invoice_coli_t::Key, invoice_coli_t>> peeked;
   Integer current_custkey = std::numeric_limits<Integer>::min();
   bool    primed          = false;
   std::unordered_map<Integer, invoice_coli_t> current;

   explicit CustkeyInvoiceBuffer(InvoiceScanner sc) : scanner(std::move(sc)) {}

   void advance_peek() { peeked = scanner->next(); }

   void ensure_custkey(Integer ck)
   {
      if (primed && ck == current_custkey) return;
      if (!primed) { advance_peek(); primed = true; }
      current.clear();
      // Skip forward over any custkeys < ck (BMJ may emit non-contiguous
      // custkeys when customer rows don't survive).
      while (peeked && peeked->first.custkey < ck) advance_peek();
      while (peeked && peeked->first.custkey == ck) {
         current.emplace(peeked->first.invoicekey, peeked->second);
         advance_peek();
      }
      current_custkey = ck;
   }
};

}  // namespace q5i_detail

template <typename Backend>
long Q5IWorkload<Backend>::query_by_base(std::vector<q5i_agg_row_t>& out)
{
   q5::Q5SideTables sides;
   const auto q5p = q5i_to_q5_params(params);
   q5::build_q5_side_tables<Backend>(region_table, nation, supplier,
                                      q5p, sides);

   Q5IOutClass<q5::Q5SideTables> outclass(sides);

   auto cust_sc = customer.getScanner();
   auto ord_sc  = coli.split_orders().getScanner();
   auto lin_sc  = coli.split_lineitem().getScanner();

   using InvSc = decltype(coli.split_invoice().getScanner());
   q5i_detail::CustkeyInvoiceBuffer<InvSc> inv_buf{coli.split_invoice().getScanner()};

   // fetch_cust: gate by c_nationkey ∈ nation_set inline.
   auto fetch_cust = [&]() -> std::optional<std::pair<customerh_t::Key, customerh_t>> {
      while (auto kv = cust_sc->next()) {
         if (stats) stats->customers_scanned++;
         if (sides.nation_set.count(kv->second.c_nationkey) == 0) continue;
         if (stats) stats->customers_passing_nation++;
         return kv;
      }
      return std::nullopt;
   };

   // fetch_ord: gate by orderdate window inline.
   const Timestamp date_lo = params.orderdate_lo;
   const Timestamp date_hi = params.orderdate_lo + 365;
   auto fetch_ord = [&]() -> std::optional<std::pair<orders_coli_t::Key, orders_coli_t>> {
      while (auto kv = ord_sc->next()) {
         if (stats) stats->orders_scanned++;
         if (kv->second.o_orderdate < date_lo || kv->second.o_orderdate >= date_hi)
            continue;
         if (stats) stats->orders_passing_date++;
         return kv;
      }
      return std::nullopt;
   };

   // BMJ #1: customer ⋈ orders on custkey → jr1.
   BinaryMergeJoin<q5i_cust_jk_t::Key, q5i_jr1_t, customerh_t, orders_coli_t>
       bmj1(fetch_cust, fetch_ord);

   auto fetch_jr1 = [&]() -> std::optional<std::pair<q5i_jr1_t::Key, q5i_jr1_t>> {
      return bmj1.next();
   };

   auto fetch_lin = [&]() -> std::optional<std::pair<lineitem_coli_t::Key, lineitem_coli_t>> {
      auto kv = lin_sc->next();
      if (kv && stats) stats->lineitems_scanned++;
      return kv;
   };

   // BMJ #2: jr1 ⋈ lineitem on (custkey, orderkey) → jr2.
   // Emit callback: look up i_status from the per-custkey invoice buffer.
   // The lineitem's invoicekey is in the constituent lineitem_coli_t::Key
   // (slot 1 of jk.keys), not in the payload.
   BinaryMergeJoin<q5i_co_jk_t::Key, q5i_jr2_t, q5i_jr1_t, lineitem_coli_t>
       bmj2(fetch_jr1, fetch_lin,
            [&](const q5i_jr2_t::Key& jk, const q5i_jr2_t& jr2) {
               const auto& l = jr2.lineitem();
               const lineitem_coli_t::Key& lk = std::get<1>(jk.keys);
               inv_buf.ensure_custkey(lk.custkey);
               auto it = inv_buf.current.find(lk.invoicekey);
               if (it == inv_buf.current.end()) {
                  throw std::runtime_error("Q5I S1: invoice FK miss");
               }
               q5i_pipeline_out_t row{
                   /*c_nationkey     */ jr2.jr1().cust().c_nationkey,
                   /*l_suppkey       */ l.l_suppkey,
                   /*l_extendedprice */ l.l_extendedprice,
                   /*l_discount      */ l.l_discount,
                   /*i_status        */ it->second.i_status,
               };
               outclass.emit(row);
            });
   bmj2.run();

   q5i_resolve_n_names(outclass, nation, out);
   std::sort(out.begin(), out.end(), q5i_sort_cmp);
   return static_cast<long>(out.size());
}

// ---------------------------------------------------------------------------
// S4: HashJoin chain on BASE tables only.
//
// Five logical joins (see q5i/CLAUDE.md + plans/baseline_s4.dot):
//   #1 REGION ⋈ NATION  — folded into build_q5_side_tables (→ nation_set).
//   #2 nation_set probed by CUSTOMER → cust_set (PK-only).
//   #3 cust_set + date filter probed by ORDERS → ord_set (PK-only).
//   #4 ord_set probed by LINEITEM (streaming).
//   #5 LINEITEM ⋈ INVOICE — substituted with index-nested-loop:
//      direct invoice.lookup1(l_invoicekey) per surviving lineitem.
//      An invoice_set hashset would never filter (FK guarantee), so the
//      build pass + memory cost buys nothing. INL pays only the per-
//      lineitem B-tree probe.
//
// Late materialisation: build payloads are id-list only. Per surviving
// lineitem, c_nationkey is recovered via two B-tree PK lookups
// (orders.lookup → o_custkey, customer.lookup → c_nationkey), cached
// at the orderkey-transition boundary (~4 lineitems per orderkey).

template <typename Backend>
long Q5IWorkload<Backend>::query_by_hash(std::vector<q5i_agg_row_t>& out)
{
   q5::Q5SideTables sides;
   const auto q5p = q5i_to_q5_params(params);
   q5::build_q5_side_tables<Backend>(region_table, nation, supplier,
                                      q5p, sides);

   Q5IOutClass<q5::Q5SideTables> outclass(sides);

   // (#2) cust_set: CUSTOMER scan filtered by nation_set, PK only.
   std::unordered_set<Integer> cust_set;
   {
      auto sc = customer.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->customers_scanned++;
         if (sides.nation_set.count(kv->second.c_nationkey) == 0) continue;
         if (stats) stats->customers_passing_nation++;
         cust_set.insert(kv->first.c_custkey);
      }
   }

   // (#3) ord_set: ORDERS scan, gated by date window + cust_set membership.
   const Timestamp date_lo = params.orderdate_lo;
   const Timestamp date_hi = params.orderdate_lo + 365;
   std::unordered_set<Integer> ord_set;
   {
      auto sc = orders.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->orders_scanned++;
         const Timestamp od = kv->second.o_orderdate;
         if (od < date_lo || od >= date_hi) continue;
         if (cust_set.find(kv->second.o_custkey) == cust_set.end()) continue;
         if (stats) stats->orders_passing_date++;
         ord_set.insert(kv->first.o_orderkey);
      }
   }

   // (#4 + #5) LINEITEM streaming probe with seek-on-miss; per-lineitem
   // INL into INVOICE for i_status; per-orderkey-transition customer
   // recovery via two B-tree PK lookups.
   {
      auto sc = lineitem.getScanner();
      Integer cached_ok = -1;
      Integer cached_c_nationkey = 0;

      sc->seek(typename lineitem_i_t::Key{1, 0});

      while (auto kv = sc->next()) {
         if (stats) stats->lineitems_scanned++;
         const Integer ok = kv->first.l_orderkey;

         if (ord_set.count(ok) == 0) {
            sc->seek(typename lineitem_i_t::Key{ok + 1, 0});
            continue;
         }

         if (ok != cached_ok) {
            Integer custkey = 0;
            orders.lookup1(typename orders_t::Key{ok},
                           [&](const orders_t& o) { custkey = o.o_custkey; });
            // custkey ∈ cust_set is guaranteed by the ord_set build,
            // so the customer lookup must succeed.
            Integer nk = 0;
            customer.lookup1(typename customerh_t::Key{custkey},
                             [&](const customerh_t& c) { nk = c.c_nationkey; });
            cached_ok          = ok;
            cached_c_nationkey = nk;
         }

         // INL into INVOICE — direct PK lookup, no build set.
         Varchar<1> i_status;
         invoice.lookup1(typename invoice_t::Key{kv->second.l_invoicekey},
                         [&](const invoice_t& inv) { i_status = inv.i_status; });

         q5i_pipeline_out_t row{
             /*c_nationkey     */ cached_c_nationkey,
             /*l_suppkey       */ kv->second.l_suppkey,
             /*l_extendedprice */ kv->second.l_extendedprice,
             /*l_discount      */ kv->second.l_discount,
             /*i_status        */ i_status,
         };
         outclass.emit(row);
      }
   }

   q5i_resolve_n_names(outclass, nation, out);
   std::sort(out.begin(), out.end(), q5i_sort_cmp);
   return static_cast<long>(out.size());
}

// ---------------------------------------------------------------------------
// S2: sequential scan of pipeline_view.
//
// Shared with S3: build sides → Q5IOutClass → q5i_resolve_n_names → sort.
// In-pipeline operator unique to S2: per-row inline custkey-transition
// nation gate + orderdate window. On nation miss we physically seek the
// view scanner past the current custkey (mirrors S3 SkipGroup), closing
// the S2/S3 access-pattern gap.

template <typename Backend>
long Q5IWorkload<Backend>::query_by_view(std::vector<q5i_agg_row_t>& out)
{
   q5::Q5SideTables sides;
   const auto q5p = q5i_to_q5_params(params);
   q5::build_q5_side_tables<Backend>(region_table, nation, supplier,
                                      q5p, sides);

   Q5IOutClass<q5::Q5SideTables> outclass(sides);

   auto sc = pipeline_view.getScanner();
   Integer cur_custkey      = std::numeric_limits<Integer>::min();
   bool    cur_custkey_ok   = false;
   while (true) {
      auto kv = sc->next();
      if (!kv) break;
      const auto& k = kv->first;
      const auto& v = kv->second;
      if (stats) stats->view_rows_scanned++;

      if (k.custkey != cur_custkey) {
         cur_custkey    = k.custkey;
         cur_custkey_ok = sides.nation_set.count(v.c_nationkey) != 0;
         if (!cur_custkey_ok) {
            // SkipGroup analogue: physical seek past this custkey.
            typename q5i_pipeline_view_t::Key skip{k.custkey + 1, 0, 0, 0};
            sc->seek(skip);
            continue;
         }
      } else if (!cur_custkey_ok) {
         continue;
      }

      if (v.o_orderdate < params.orderdate_lo
          || v.o_orderdate >= params.orderdate_lo + 365) {
         continue;
      }

      q5i_pipeline_out_t row{
          /*c_nationkey     */ v.c_nationkey,
          /*l_suppkey       */ v.l_suppkey,
          /*l_extendedprice */ v.l_extendedprice,
          /*l_discount      */ v.l_discount,
          /*i_status        */ v.i_status,
      };
      outclass.emit(row);
   }

   q5i_resolve_n_names(outclass, nation, out);
   std::sort(out.begin(), out.end(), q5i_sort_cmp);
   return static_cast<long>(out.size());
}

// ---------------------------------------------------------------------------
// S3: COLI group walk via Q5IGroupWalkVisitor in Query mode.

template <typename Backend>
long Q5IWorkload<Backend>::query_by_merged(std::vector<q5i_agg_row_t>& out)
{
   q5::Q5SideTables sides;
   const auto q5p = q5i_to_q5_params(params);
   q5::build_q5_side_tables<Backend>(region_table, nation, supplier,
                                      q5p, sides);

   Q5IOutClass<q5::Q5SideTables> outclass(sides);
   Q5IGroupWalkVisitor<q5::Q5SideTables,
                        Q5IOutClass<q5::Q5SideTables>,
                        Q5IFilterMode::Query>
       visitor{params, sides, outclass, stats};
   coli_group_walk<Backend>(coli.merged_adapter(), visitor);

   q5i_resolve_n_names(outclass, nation, out);
   std::sort(out.begin(), out.end(), q5i_sort_cmp);
   return static_cast<long>(out.size());
}

}  // namespace tpch::q5i
