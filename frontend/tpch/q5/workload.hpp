#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §5 (Q3 worked example)             — analogous structure for Q5
//   §6 (comparison-integrity rules)    — read before changing join strategy
//
// Q5Workload<Backend>: holds all adapters needed by Q5 and declares the
// per-structure query / load / size methods.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + binary merge join  -> query_by_base
//   2 = intermediate pipeline view               -> query_by_view
//   3 = MI[COL] only (col_group_walk)            -> query_by_merged
//   4 = traditional indexes + hash join          -> query_by_hash
//
// Q5 uses the COL 3-table merged index (CustomerOrdersLineitemPipeline),
// reusing customer_coli_t, orders_coli_t, lineitem_col_t from Q3 verbatim.
// Three additional dimension tables (REGION, NATION, SUPPLIER) are probed
// at query time via in-process hashmaps — they are NOT stored as secondaries.
// S5 is omitted (Q5 has no parameter-independent aggregate to bake).

#include <vector>

#include "../backend.hpp"
#include "../tpch_family/col_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q5
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.5).
// Defaults: REGION = ASIA, DATE = 1994-01-01.
//
// The query selects orders where:
//   o_orderdate >= orderdate_lo  AND  o_orderdate < orderdate_lo + 1 year

struct Params {
   Varchar<25> region;       // region name filter (e.g. "ASIA")
   Timestamp   orderdate_lo; // inclusive lower bound on o_orderdate

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp (stubbed to true for now).

// Applied to a customer row: c_nationkey is in the in-region nation_set.
// Phase 4: receives pre-built nation_set; Phase 0.5: returns true.
bool q5_predicate_customer(const customerh_t& c, const Params& p);

// Applied to an orders row: orderdate in [orderdate_lo, orderdate_lo + 1y).
// Phase 4: real date-range check; Phase 0.5: returns true.
bool q5_predicate_orders(const orders_t& o, const Params& p);

// Applied to a raw lineitem: always true for Q5 (no per-lineitem filter
// before the SUPPLIER probe; cross-equality fires after the probe).
// Exists for symmetry with Q3's predicate shape.
bool q5_predicate_lineitem(const lineitem_t& l, const Params& p);

// ---------------------------------------------------------------------------
// Q5Stats: cardinality counters for all four query paths.
// Mirrors Q3FamilyStats shape; Q5-specific names where needed.
// Declared here so all four query_by_* bodies can increment when stats != nullptr.

struct Q5Stats {
   long customers_visited  = 0;
   long orders_visited     = 0;
   long lineitems_visited  = 0;
   long lineitems_admitted = 0;  // passed SUPPLIER probe + cross-equality
   long agg_buckets        = 0;  // distinct n_name buckets in the aggregate
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q5Workload
{
   // The full TPC-H base-table workload (owns all 8 base tables).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Direct references to the base adapters needed at query time.
   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_t>&   lineitem;
   typename Backend::template Adapter<supplier_t>&   supplier;
   typename Backend::template Adapter<nation_t>&     nation;
   typename Backend::template Adapter<region_t>&     region;

   // COL 3-table pipeline: owns MI(customer_coli_t, orders_coli_t, lineitem_col_t).
   // Reused verbatim from Q3 — no Q5-specific modifications.
   CustomerOrdersLineitemPipeline<Backend> col;

   // Structure 2: intermediate pipeline view (per-lineitem rows, unfiltered).
   typename Backend::template Adapter<q5_pipeline_view_t>& pipeline_view;

  public:
   Params   params;
   Q5Stats* stats = nullptr;

   // Expose the COL pipeline so test harnesses can call populate_split() and
   // populate_merged() directly without routing through load()'s switch.
   CustomerOrdersLineitemPipeline<Backend>& col_pipeline() { return col; }

   Q5Workload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_t>&   lineitem,
       typename Backend::template Adapter<supplier_t>&   supplier,
       typename Backend::template Adapter<nation_t>&     nation,
       typename Backend::template Adapter<region_t>&     region,
       typename Backend::template Adapter<q5_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_col_t>& merged_col,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_col_t>& split_lineitem);

   // ------------------------------------------------------------------
   // Param cycling: Phase 0.5 always uses defaults.
   // Phase 5 will rotate through REGION × DATE combinations so the
   // executable exercises multiple param sets per run.
   // ------------------------------------------------------------------

   void set_params_for_iter(long iter) { params = Params::defaults(); }

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Returns the number of result rows (~5, one per in-region nation;
   // no LIMIT — cardinality bounded by |nation_set|).
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q5_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q5_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q5_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q5_agg_row_t>& out);  // structure 4

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   void   load();
   double get_size() const;
};

}  // namespace tpch::q5

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
