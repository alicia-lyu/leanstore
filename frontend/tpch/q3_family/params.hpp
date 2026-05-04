#pragma once

// Shared SUBSTITUTION-PARAMETER rotation table for the Q3 / Q3I query family.
//
// TPC-H §2.4.3 defines the domain for Q3's two substitution parameters:
//   SEGMENT: one of {AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY}
//   DATE:    March 15 of a year in [1993, 1997] (drives both o_orderdate
//            upper bound and l_shipdate lower bound).
//
// The rotation table below covers the full DATE domain in 10 iterations by
// pairing all 5 segments with the most discriminating years.  The validation
// defaults (BUILDING / 1995-03-15) appear first so the first iteration is
// always spec-valid.
//
// Usage (from a per-query workload's set_params_for_iter):
//
//   const auto& e = tpch::q3_family::PARAM_TABLE[iter % PARAM_TABLE_SIZE];
//   params.mktsegment = Varchar<10>(e.segment);
//   params.orderdate  = e.date;
//   params.shipdate   = e.date;
//
// Q3I additionally sets params.threshold = Numeric(0) (unchanged across
// iterations per spec validation convention).

#include "../tpch_tables.hpp"

namespace tpch::q3_family
{

// Date constants (days since 1970-01-01).
//   DATE_1995_03_15 = 9204  (canonical; defined in tpch_tables.hpp)
//   1993-03-15 = 9204 - 365 - 365 = 8474  (1994 and 1995 are not leap years)
//   1994-03-15 = 9204 - 365       = 8839
//   1996-03-15 = 9204 + 366       = 9570  (1996 is a leap year)
//   1997-03-15 = 9204 + 366 + 365 = 9935
static constexpr Timestamp Q3_DATE_1993_03_15 = DATE_1995_03_15 - 365 - 365;
static constexpr Timestamp Q3_DATE_1994_03_15 = DATE_1995_03_15 - 365;
static constexpr Timestamp Q3_DATE_1996_03_15 = DATE_1995_03_15 + 366;
static constexpr Timestamp Q3_DATE_1997_03_15 = DATE_1995_03_15 + 366 + 365;

struct ParamEntry {
   const char* segment;
   Timestamp   date;
};

// 10 entries: all 5 segments × the two most discriminating years
// (1993 vs 1997) plus the validation year (1995) in the middle, and
// 1994/1996 flanking — covers the full DATE domain in 10 iterations.
static constexpr ParamEntry PARAM_TABLE[] = {
    {"BUILDING",   DATE_1995_03_15},   // validation defaults (first)
    {"AUTOMOBILE", Q3_DATE_1994_03_15},
    {"FURNITURE",  Q3_DATE_1993_03_15},
    {"HOUSEHOLD",  Q3_DATE_1996_03_15},
    {"MACHINERY",  Q3_DATE_1997_03_15},
    {"AUTOMOBILE", DATE_1995_03_15},
    {"BUILDING",   Q3_DATE_1993_03_15},
    {"FURNITURE",  Q3_DATE_1997_03_15},
    {"HOUSEHOLD",  Q3_DATE_1994_03_15},
    {"MACHINERY",  Q3_DATE_1996_03_15},
};

static constexpr long PARAM_TABLE_SIZE =
    static_cast<long>(sizeof(PARAM_TABLE) / sizeof(PARAM_TABLE[0]));

}  // namespace tpch::q3_family
