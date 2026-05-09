#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §3 op 4 (load vs query)            — keep query-time joins on shared JoinState
//   §5 (Q12 worked example)            — inline sketches
//   §6 (comparison-integrity rules)    — read before changing join strategy
//
// Q12Workload<Backend>: holds all adapters needed by Q12 and declares the
// per-structure query / load / size methods.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join  -> query_by_base
//   2 = intermediate pipeline view        -> query_by_view
//   3 = MI[0] only (PremergedJoin)        -> query_by_merged
//   4 = traditional indexes + hash join   -> query_by_hash
//
// The shared ORDERS x LINEITEM join drivers live in OrdersLineitemPipeline;
// this class composes them with Q12-specific filter/project/aggregate logic
// supplied as lambdas (monolithic post-join style per q12/CLAUDE.md
// §Execution Style).

#include <vector>

#include "../backend.hpp"
#include "../tpch_family/ol_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.12).
// Defaults: SHIPMODE1=MAIL, SHIPMODE2=SHIP, DATE=1994-01-01.

struct Params {
   Varchar<10> shipmode1;    // default "MAIL"
   Varchar<10> shipmode2;    // default "SHIP"
   Timestamp receiptdate_lo; // default 1994-01-01 (days since epoch)
   Timestamp receiptdate_hi; // default 1995-01-01 (days since epoch)

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to a raw lineitem before joining (structures 1 and 4).
// Encodes: l_shipmode IN (p.shipmode1, p.shipmode2)
//          AND l_shipdate < l_commitdate < l_receiptdate
//          AND l_receiptdate IN [p.receiptdate_lo, p.receiptdate_hi).
bool q12_predicate_lineitem(const lineitem_t& l, const Params& p);

// Applied to a fully-assembled joined_ol_t (structure 3 — PremergedJoin).
// Same five conditions expressed over joined_ol_t fields.
bool q12_predicate_joined(const joined_ol_t& j, const Params& p);

// Applied to a projected pipeline-view row (structure 2). Same five
// conditions, expressed over the projected fields directly.
bool q12_predicate_view(const q12_pipeline_view_t& v, const Params& p);

// ---------------------------------------------------------------------------
// Optional per-query intermediate cardinality counters. When non-null on the
// Q12Workload, each query_by_* path bumps the relevant fields. Used by the
// test harness to report and sanity-check pre-join / post-filter cardinalities
// alongside timings. Default null in production paths.

struct Q12Stats {
   long lineitems_scanned   = 0;  // total LINEITEM rows seen by fetch lambda (S1/S4) / view scan (S2)
   long lineitems_passed    = 0;  // rows passing the predicate at the filter site
   long variants_scanned    = 0;  // S3 only: total variant records from MI[0] scan
   long lineitems_admitted  = 0;  // S3 only: lineitems admitted by F1 admission filter
   long orders_built        = 0;  // S4 only: orders inserted into hash build side
   long join_callbacks      = 0;  // join callback invocations (assembled rows handed to bump)
   long aggregator_rows_out = 0;  // rows emitted by emit_and_sort

   void reset() { *this = Q12Stats{}; }
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q12Workload
{
   // The full TPC-H base-table workload (owns part, supplier, etc.).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Direct references to the two base adapters needed for view loading.
   typename Backend::template Adapter<orders_t>&   orders;
   typename Backend::template Adapter<lineitem_t>& lineitem;

   // Shared ORDERS x LINEITEM pipeline: wraps the three join drivers.
   OrdersLineitemPipeline<Backend> ol;

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   typename Backend::template Adapter<q12_pipeline_view_t>& pipeline_view;

  public:
   // Substitution parameters; populated by ctor from gflags / defaults.
   Params params;

   // Optional cardinality counters (test harness sets non-null to enable).
   Q12Stats* stats = nullptr;


   Q12Workload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template Adapter<q12_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol);

   // ------------------------------------------------------------------
   // Param cycling — rotates through TPC-H §2.4.12 substitution-parameter
   // sets deterministically so that S2/S5 secondaries that incorrectly
   // bake parameterised filters diverge visibly from S1/S3/S4.
   // Called by TpchExecutableHelper::tput_tx before each query invocation.
   // ------------------------------------------------------------------

   void set_params_for_iter(long iter);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Each returns the number of result rows written into `out`
   // (at most 7, the shipmode cardinality).
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q12_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q12_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q12_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q12_agg_row_t>& out);  // structure 4

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   // Loads base tables via tpch.load(), then builds all secondary
   // structures (pipeline view + merged index) unconditionally.
   void load();

   // Returns the size (MiB) of the secondary structure for the active
   // storage structure (0 for structures 1 and 4, which have no extras).
   double get_size() const;
};

}  // namespace tpch::q12

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
