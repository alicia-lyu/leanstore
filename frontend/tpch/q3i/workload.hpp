#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Q3IWorkload<Backend>: Q3 extended with a per-customer outstanding-invoice
// sub-aggregate (cust_open_due = SUM(i_totaldue) WHERE i_status='O').
// Replaces OrdersLineitemPipeline with COLIPipeline (4-table COLI MI).
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join    -> query_by_base
//   2 = intermediate pipeline view          -> query_by_view
//   3 = MI[COLI] only (PremergedJoin)       -> query_by_merged
//   4 = traditional indexes + hash join     -> query_by_hash

#include <vector>

#include "../backend.hpp"
#include "../tpchi_family/coli_pipeline.hpp"
#include "../q3_family/stats.hpp"
#include "../tpchi_family/tpchi_workload.hpp"
#include "views.hpp"

namespace tpch::q3i
{

// ---------------------------------------------------------------------------
// Substitution parameters (Q3I SQL).
// Defaults: SEGMENT=BUILDING, DATE=1995-03-15, THRESHOLD=0.

struct Params {
   Varchar<10> mktsegment;   // default "BUILDING"
   Timestamp   orderdate;    // default DATE '1995-03-15' — orders before this date
   Timestamp   shipdate;     // default DATE '1995-03-15' — lineitems shipped after
   Numeric     threshold;    // default 0 — minimum cust_open_due to include

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to an orders row: o_orderdate < params.orderdate.
bool q3i_predicate_orders(const orders_t& o, const Params& p);

// Applied to a raw lineitem: l_shipdate > params.shipdate.
bool q3i_predicate_lineitem(const lineitem_t& l, const Params& p);

// Applied to a fully-assembled joined_ol_t: checks both order and lineitem
// conditions simultaneously (structures 2 and 3).
bool q3i_predicate_joined(const joined_ol_t& j, const Params& p);

// Applied to an invoice row: i_status = 'O' (for open-due sub-aggregate).
bool q3i_predicate_invoice(const invoice_t& i);

// ---------------------------------------------------------------------------
// Optional per-query intermediate cardinality counters and per-stage timers.
//
// Per-path stage attribution policy (affects stage_us_* interpretation):
//
//   S1 (merge)  — scan + filter + aggregate all run inside the 3-BMJ chain
//                 and inside the scanner-wrapper aggregators. All attributed
//                 to stage_us_join. stage_us_scan_filter and
//                 stage_us_aggregator are zero.
//   S2 (view)   — no query-time aggregator (pre-materialised). Query is a
//                 sequential view scan with per-row filters. Attributed to
//                 stage_us_scan_filter. stage_us_join and stage_us_aggregator
//                 are zero.
//   S3 (merged) — single fused walk; everything attributed to stage_us_join.
//                 stage_us_scan_filter and stage_us_aggregator are zero.
//                 join1/2/3_output_rows are also zero (S1/S4 chain-join
//                 abstraction; not applicable to a fused walk).
//   S4 (hash)   — entire HJ chain (invoice aggregate, customer filter, order
//                 build, lineitem probe) attributed to stage_us_join to match
//                 S1/S3. stage_us_scan_filter and stage_us_aggregator are zero.
//
// When comparing per-stage costs across structures, always use per-query
// averages (total / tx_count), not raw totals — totals reflect different
// TX/s rates over the same wall-clock window. See §Stage attribution in
// executable_{rocksdb,leanstore}.cpp.
//
// Coverage gaps (see test_query_q3i_lsm `[card]` table). Per-path counter
// fidelity differs by operator shape:
//
//   S1 base   — `customers_scanned`, `orders_scanned` are bumped, but
//               `lineitems_scanned` / `invoices_scanned` stay at 0.
//               Reason: lineitems and invoices are consumed inside the
//               scanner-wrapper aggregators (CustomerOpenDueAggregator,
//               LineitemRevenueAggregator) which currently do NOT
//               increment the per-record counters — only the BMJ chain's
//               aggregate-output stages do.
//   S2 view   — all four base counters stay at 0 by design (S2 scans the
//               pipeline view, not base tables).
//   S3 merged — every counter stays at 0. `query_by_merged` runs a
//               `COLIGroupWalkVisitor` that does not have a `Q3IStats*`
//               threaded through. The merged_coli secondary cardinality
//               check in the test harness is what guards S3 against
//               empty-secondary regressions.
//   S4 hash   — full coverage; all four base scans bump their counters.
//
// TODO (richer cardinality reporting, deferred):
//   1. Bump `lineitems_scanned` / `invoices_scanned` from inside the
//      scanner-wrapper aggregators in query.tpp so S1's stat row shows
//      the real per-record cost.
//   2. Plumb `Q3IStats*` into `COLIGroupWalkVisitor` (q3i/query.tpp) so
//      `query_by_merged` reports cust/orders/lineitems/invoices/joins
//      like the other paths. With this in, the test harness's
//      "S3 merged join_callbacks=0 (stats not wired through Visitor)"
//      relax can be removed and S3 can join the regular check_joins
//      gate.

// Q3IStats: Q3-family base counters (via Q3FamilyStats) plus Q3I-specific
// fields (invoice sub-aggregate, per-structure chain-join output rows,
// aCOLI MI scan counts, seek-skip events, and backend perf counters).
//
// Derivation keeps all Q3-shape increment sites (stats->customers_scanned,
// stats->join_callbacks, etc.) unchanged — no call-site churn.
struct Q3IStats : q3_family::Q3FamilyStats {
   // Invoice sub-aggregate counters (Q3I-only: Q3 has no INVOICE table).
   long invoices_scanned        = 0;
   long invoices_passing_filter = 0;  // post i_status='O' filter

   // S1/S4 chain-join intermediate cardinalities.  S3 fuses everything into
   // one walk so these stay at zero for S3 — see §Cardinality structure.
   long join1_output_rows = 0;  // customer ⋈ open_due (or hashmap probe)
   long join2_output_rows = 0;  // (..) ⋈ orders_coli
   long join3_output_rows = 0;  // (..) ⋈ lineitem_agg

   // S5 (aCOLI) counters — 3-type MI: customer + order + lineitem rows.
   long acoli_customers_scanned        = 0;
   long acoli_customers_passing_filter = 0;
   long acoli_orders_scanned           = 0;
   long acoli_orders_emitted           = 0;
   long acoli_lineitems_scanned        = 0;  // lineitem rows visited in aCOLI walk
   long acoli_lineitems_passing        = 0;  // passed l_shipdate filter

   // S3 (mi_coli_walk) — bytes-of-work proxy.
   // `mi_records_visited` and `mi_groups_skipped` are inherited from
   // Q3FamilyStats and bumped by Q3FamilyVisitor::on_record_visited /
   // on_group_skipped during coli_group_walk / col_group_walk.
   // mi_records_visited / |MI rows| ≈ 1.0 means the walker reads the whole MI;
   // a healthy ratio is closer to mktsegment selectivity (~0.2 for BUILDING).

   // S1 / S4 customer-level Seek-skip counters (G1/G2 A/B). Mirror
   // mi_groups_skipped: count custkey groups physically skipped on the
   // secondary scanners after a customer-side gate rejection. Trait-gated
   // by Backend::USE_PHYSICAL_SEEK_SKIP, runtime-overridden by
   // FLAGS_use_seek_skip. See q3i/PERFORMANCE.md §3 A/B-1.
   long bj_groups_skipped = 0;  // S1 BMJ chain — agg_inv (G1 sync skip)
   long bj_ord_skips      = 0;  // S1 BMJ#2 right — ord_scan (G6 deferred)
   long bj_lin_skips      = 0;  // S1 BMJ#3 right — agg_lin (G6 deferred)
   long hj_groups_skipped = 0;  // S4 HJ chain

   // RocksDB PerfContext / IOStatsContext totals (RocksDB backend only).
   // Populated only when --micro_perf=true; zero otherwise.
   // All counters are accumulated across all queries in the run window;
   // divide by tx_count for per-query averages.
   //
   // PerfContext fields (rocksdb/perf_context.h):
   uint64_t pc_user_key_comparison_count = 0;
   uint64_t pc_block_cache_hit_count     = 0;
   uint64_t pc_block_read_count          = 0;
   uint64_t pc_block_read_byte           = 0;
   uint64_t pc_block_read_time           = 0;  // nanoseconds
   uint64_t pc_block_decompress_time     = 0;  // nanoseconds
   uint64_t pc_iter_next_cpu_nanos       = 0;
   uint64_t pc_iter_seek_cpu_nanos       = 0;
   // IOStatsContext fields (rocksdb/iostats_context.h):
   uint64_t ioc_bytes_read  = 0;
   uint64_t ioc_read_nanos  = 0;
   uint64_t ioc_open_nanos  = 0;

   // LeanStore mirror of A1 metrics — populated when --micro_perf=true on
   // the LeanStore backend. Field semantics mapped in PERFORMANCE.md §3 A1
   // (Linux q3i_btree result table).
   //   ls_dt_next_tuple    — Σ WorkerCounters::dt_next_tuple per query
   //                         (tuples advanced; proxy for user_key_cmp/q).
   //   ls_dt_page_reads    — Σ WorkerCounters::dt_page_reads per query
   //                         (pages from SSD; × EFFECTIVE_PAGE_SIZE
   //                         ≈ block_read_byte/q).
   //   ls_hot_hit          — Σ WorkerCounters::dt_resolve_swip_hot per query
   //                         (swizzles resolved hot, i.e. page already in pool).
   //   ls_cold_hit         — Σ WorkerCounters::dt_resolve_swip_cool per query
   //                         (swizzles resolved cool; not yet evicted).
   //   ls_iter_next_ns     — chrono-instrumented Σ next() time per query
   //                         (LeanStore analog of pc_iter_next_cpu_nanos).
   //   ls_iter_next_calls  — count of next() calls timed (sanity check).
   uint64_t ls_dt_next_tuple   = 0;
   uint64_t ls_dt_page_reads   = 0;
   uint64_t ls_hot_hit         = 0;
   uint64_t ls_cold_hit        = 0;
   uint64_t ls_iter_next_ns    = 0;
   uint64_t ls_iter_next_calls = 0;

   void reset() { *this = Q3IStats{}; }
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q3IWorkload
{
   // The invoice-extended TPC-H workload (owns all 8 base tables + invoice).
   TPCHIWorkload<Backend::template Adapter>& tpch;

   // Direct references to the base adapters needed at query time.
   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_i_t>& lineitem;  // FK-bearing variant
   typename Backend::template Adapter<invoice_t>&    invoice;

   // COLI 4-table pipeline: owns MI(customer_coli_t, orders_coli_t,
   // lineitem_coli_t, invoice_coli_t).
   CustomerOrdersLineitemInvoicePipeline<Backend> coli;

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   typename Backend::template Adapter<q3i_pipeline_view_t>& pipeline_view;

  public:
   Params params;
   Q3IStats* stats      = nullptr;
   bool      micro_perf = false;  // set to true when --micro_perf is active

   // Expose the COLI pipeline so test harnesses can call populate_split() and
   // populate_merged() directly without routing through load()'s
   // FLAGS_storage_structure switch.
   CustomerOrdersLineitemInvoicePipeline<Backend>& coli_pipeline() { return coli; }

   Q3IWorkload(
       TPCHIWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_i_t>& lineitem,
       typename Backend::template Adapter<invoice_t>&    invoice,
       typename Backend::template Adapter<q3i_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_coli_t>& split_lineitem,
       typename Backend::template Adapter<invoice_coli_t>&  split_invoice,
       typename Backend::template MergedAdapter<customer_acoli_t, orders_coli_t,
                                                lineitem_acoli_t>& acoli);

   // ------------------------------------------------------------------
   // Param cycling: rotate through a static substitution-parameter table
   // so each TX iteration exercises a distinct (segment, date) combination.
   // Called by the per-structure wrapper's tput_tx before every query TX.
   // ------------------------------------------------------------------

   void set_params_for_iter(long iter);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Returns the number of result rows (at most 10, LIMIT 10 by revenue).
   // ------------------------------------------------------------------

   long query_by_base      (std::vector<q3i_agg_row_t>& out);  // structure 1
   long query_by_view      (std::vector<q3i_agg_row_t>& out);  // structure 2
   long query_by_merged    (std::vector<q3i_agg_row_t>& out);  // structure 3
   long query_by_hash      (std::vector<q3i_agg_row_t>& out);  // structure 4
   long query_by_aggregated(std::vector<q3i_agg_row_t>& out);  // structure 5 (aCOLI)

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   // See Q3Workload::populate_secondaries() for the load-vs-secondaries split
   // rationale (family loader composition).
   void   populate_secondaries();
   void   load();
   double get_size() const;
};

}  // namespace tpch::q3i

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
