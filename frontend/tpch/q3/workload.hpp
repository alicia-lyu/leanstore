#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §3 op 4 (load vs query)            — keep query-time joins on shared JoinState
//   §3 op 6 (SortedAggregate)          — Q3's LineitemRevenueAccumulator wrapper
//   §5 (Q3 worked example)             — inline sketch
//   §6 (comparison-integrity rules)    — read before changing join strategy
//
// Q3Workload<Backend>: holds all adapters needed by Q3 and declares the
// per-structure query / load / size methods.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + binary merge join  -> query_by_base
//   2 = intermediate pipeline view               -> query_by_view
//   3 = MI[COL] only (col_group_walk)            -> query_by_merged
//   4 = traditional indexes + hash join          -> query_by_hash
//
// Q3 uses the COL 3-table merged index (CustomerOrdersLineitemPipeline).
// All four storage structures are covered; S5 is omitted (see q3/CLAUDE.md).

#include <vector>

#include "../backend.hpp"
#include "../tpch_family/col_pipeline.hpp"
#include "../q3_family/stats.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.3).
// Defaults: SEGMENT=BUILDING, DATE=1995-03-15.

struct Params {
   Varchar<10> mktsegment;  // default "BUILDING"
   Timestamp   orderdate;   // default DATE '1995-03-15' — orders before this date
   Timestamp   shipdate;    // default DATE '1995-03-15' — lineitems shipped after

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to a customer row: c_mktsegment == params.mktsegment.
bool q3_predicate_customer(const customerh_t& c, const Params& p);

// Applied to an orders row: o_orderdate < params.orderdate.
bool q3_predicate_orders(const orders_t& o, const Params& p);

// Applied to a raw lineitem: l_shipdate > params.shipdate.
bool q3_predicate_lineitem(const lineitem_t& l, const Params& p);

// Applied to a pipeline view row: checks mktsegment + orderdate + shipdate.
bool q3_predicate_view(const q3_pipeline_view_t& v, const Params& p);

// ---------------------------------------------------------------------------
// Stats: Q3 uses the shared Q3FamilyStats directly (no invoice extension).

using Stats = q3_family::Q3FamilyStats;

// ---------------------------------------------------------------------------

template <typename Backend>
class Q3Workload
{
   // The full TPC-H base-table workload (owns all 8 base tables).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Direct references to the base adapters needed at query time.
   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_t>&   lineitem;

   // COL 3-table pipeline: owns MI(customer_coli_t, orders_coli_t, lineitem_col_t).
   CustomerOrdersLineitemPipeline<Backend> col;

   // Structure 2: intermediate pipeline view (per-lineitem rows, unfiltered).
   typename Backend::template Adapter<q3_pipeline_view_t>& pipeline_view;

  public:
   Params params;
   Stats* stats = nullptr;

   // Expose the COL pipeline so test harnesses can call populate_split() and
   // populate_merged() directly without routing through load()'s switch.
   CustomerOrdersLineitemPipeline<Backend>& col_pipeline() { return col; }

   Q3Workload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_t>&   lineitem,
       typename Backend::template Adapter<q3_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_col_t>& merged_col,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_col_t>& split_lineitem);

   // ------------------------------------------------------------------
   // Param cycling: rotate through the shared Q3-family substitution-
   // parameter table so each TX iteration exercises a distinct
   // (segment, date) combination.
   // ------------------------------------------------------------------

   void set_params_for_iter(long iter);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Returns the number of result rows (at most 10, LIMIT 10 by revenue).
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q3_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q3_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q3_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q3_agg_row_t>& out);  // structure 4

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   // Loads only the per-query secondaries (col split, view, col merged).
   // The shared TPCHWorkload base tables must be loaded separately via
   // tpch.load() before this call. Used by family loaders that share one
   // tpch.load() across multiple per-query workloads.
   void   populate_secondaries();
   // Populates only the per-query pipeline view (S2). Family-shared
   // structures (col.populate_split, col.populate_merged) are NOT
   // populated. Used by family loaders so the family-shared secondaries
   // are written exactly once (writing them twice hits LeanStore
   // duplicate-key errors; LSM tolerates it via upsert semantics).
   void   populate_view_only();
   // Convenience: tpch.load() + populate_secondaries(). Used by single-query
   // executables that don't compose with a family loader.
   void   load();
   double get_size() const;
};

}  // namespace tpch::q3

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
