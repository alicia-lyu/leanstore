#pragma once

// Shared accumulator structs for the Q3 / Q3I query family.
//
// Both accumulators read only the fields `l_extendedprice`, `l_discount`,
// and `l_shipdate`, which are present on lineitem_t, lineitem_coli_t, and
// lineitem_acoli_t.  The overloaded consume() / consume_invoice() methods
// provide duck-typed dispatch without requiring a common base class.
//
// Sharing these structs across Q3 and Q3I keeps the per-record arithmetic
// identical regardless of storage structure (OPERATORS.md §6.1
// comparison-integrity).

#include "../tpch_tables.hpp"
#include "../views_coli.hpp"

namespace tpch::q3_family
{

// LineitemRevenueAccumulator — per-(custkey, orderkey) revenue sum.
//
// Accumulates SUM(l_extendedprice * (1 - l_discount)) for all lineitems
// that pass the shipdate filter (l_shipdate > params.shipdate).  Used by
// Q3 and Q3I across all storage structures (S1 / S2 / S3 / S5).
//
// Template parameter P must be a Params-like struct with a `shipdate`
// Timestamp field.  Concrete Params types live in each per-query
// workload.hpp; this accumulator depends only on the common field name.
template <typename P>
struct LineitemRevenueAccumulator {
   Numeric revenue = 0;

   // base lineitem_t (used by Q3 S1/S3/S4 and Q3I S4).
   bool consume(const lineitem_t& l, const P& p) {
      if (l.l_shipdate <= p.shipdate) return false;
      revenue += l.l_extendedprice * (Numeric(1) - l.l_discount);
      return true;
   }

   // COLI / aCOLI lineitem variants (used by Q3I S1/S3/S5).
   bool consume(const lineitem_coli_t& l, const P& p) {
      if (l.l_shipdate <= p.shipdate) return false;
      revenue += l.l_extendedprice * (Numeric(1) - l.l_discount);
      return true;
   }

   bool consume(const lineitem_acoli_t& l, const P& p) {
      if (l.l_shipdate <= p.shipdate) return false;
      revenue += l.l_extendedprice * (Numeric(1) - l.l_discount);
      return true;
   }

   void reset() { revenue = 0; }
};

}  // namespace tpch::q3_family
