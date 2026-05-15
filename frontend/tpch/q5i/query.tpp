// Template method bodies for Q5IWorkload<Backend> query methods,
// Params::defaults(), q5i_agg_row_t::print(), and predicate implementations.
// Operator-translation reference: see ../OPERATORS.md §3.
//
// Phase 0.5: all query_by_* return empty results (XOR digest = 0x0).
// Real bodies land in Phase 1+.

#pragma once

#include <ostream>
#include <string_view>
#include <vector>

#include "../tpch_tables.hpp"

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
      << n_name << '\t' << o_orderdate << '\t' << i_status << '\n';
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
// Stub query bodies (Phase 0.5 — real bodies land in Phase 1+).

template <typename Backend>
long Q5IWorkload<Backend>::query_by_base(std::vector<q5i_agg_row_t>& out)
{
   (void)out;
   return 0;
}

template <typename Backend>
long Q5IWorkload<Backend>::query_by_view(std::vector<q5i_agg_row_t>& out)
{
   (void)out;
   return 0;
}

template <typename Backend>
long Q5IWorkload<Backend>::query_by_merged(std::vector<q5i_agg_row_t>& out)
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

}  // namespace tpch::q5i
