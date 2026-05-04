#pragma once

// Base aggregate-output row shared by Q3 and Q3I.
//
// q3_agg_row_base_t holds the four fields that both queries produce:
//   o_orderkey, revenue, o_orderdate, o_shippriority.
//
// Q3 aliases this directly.  Q3I derives q3i_agg_row_t from it and adds
// the invoice-specific cust_open_due field.
//
// No SKBuilder / MergedAdapter specialization is needed: q3_agg_row_base_t
// (and its Q3I subclass) is an in-memory result type, never stored in a
// B-tree or RocksDB column family.

#include <ostream>

#include "../tpch_tables.hpp"

namespace tpch::q3_family
{

// ---------------------------------------------------------------------------
// Final aggregate output row for the Q3 query family.
//
// Fields:
//   o_orderkey     — TPC-H ORDERS primary key
//   revenue        — SUM(l_extendedprice * (1 - l_discount)) per order
//   o_orderdate    — ORDER BY key (ASC tiebreaker after revenue DESC)
//   o_shippriority — output column; no ORDER BY role per TPC-H spec

struct q3_agg_row_base_t {
   Integer   o_orderkey;
   Numeric   revenue;
   Timestamp o_orderdate;
   Integer   o_shippriority;

   // Print the four base columns as tab-separated values, no trailing newline.
   // Subclasses call this first, then append their own columns.
   void print_base(std::ostream& os) const
   {
      os << o_orderkey << "\t" << revenue << "\t" << o_orderdate << "\t"
         << o_shippriority;
   }

   // print() emits a full row followed by a newline.  Q3 uses this directly;
   // Q3I overrides it to append cust_open_due.
   void print(std::ostream& os) const
   {
      print_base(os);
      os << "\n";
   }

   // Comparator for apply_topN: revenue DESC, o_orderdate ASC, o_orderkey ASC.
   // Same tiebreaker used by all Q3-family query_by_* paths so partial_sort
   // produces a deterministic order regardless of the input permutation.
   static bool cmp(const q3_agg_row_base_t& a, const q3_agg_row_base_t& b)
   {
      if (a.revenue     != b.revenue)     return a.revenue     > b.revenue;
      if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
      return a.o_orderkey < b.o_orderkey;
   }
};

}  // namespace tpch::q3_family
