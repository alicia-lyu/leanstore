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
#include <string_view>
#include <vector>

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
// Stub query bodies (S1 / S4 — wired in Phase 4b).

template <typename Backend>
long Q5IWorkload<Backend>::query_by_base(std::vector<q5i_agg_row_t>& out)
{
   (void)out;
   return 0;
}

template <typename Backend>
long Q5IWorkload<Backend>::query_by_hash(std::vector<q5i_agg_row_t>& out)
{
   (void)out;
   return 0;
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
