#pragma once

// populate_q10_view — Pattern B view loader for Q10.
//
// Reuses Q10GroupWalkVisitor in ViewLoad mode with all parameterised
// filters dropped: per-lineitem emit of q10_pipeline_view_t into the
// pipeline_view adapter. The same walker code that drives S3 drives
// the view loader, so view-vs-walker drift is structurally impossible
// (PLAYBOOK §3.6 Pattern B; PITFALL notes f74b67da / 4dc93ec6).
//
// Run AFTER col.populate_merged() — the loader consumes the COL MI.

#include "../tpch_tables.hpp"
#include "../q10/views.hpp"
#include "../q10/workload.hpp"  // Params, Q10Stats
#include "visitor.hpp"

namespace tpch::q10
{

// Forwarding sink: each lineitem emitted by the visitor in ViewLoad
// mode lands as one (Key, row) insert into the pipeline_view adapter.
template <typename ViewAdapter>
struct Q10ViewLoadSink {
   ViewAdapter& adapter;
   Q10Stats*    stats         = nullptr;
   long         rows_inserted = 0;

   void emit_view(const q10_pipeline_view_t::Key& k,
                  const q10_pipeline_view_t&      v)
   {
      adapter.insert(k, v);
      ++rows_inserted;
      // Q10Stats::view_rows_scanned is already bumped inside the
      // visitor's ViewLoad branch; don't double-count here.
   }
};

// nation_t adapter is unused in ViewLoad mode (NATION INL only fires
// in Query mode at on_group_end). We still need a concrete type for
// the visitor template; reuse the live nation adapter the caller
// already owns. A dummy local would risk binding to a temp.
template <typename Backend>
void populate_q10_view(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<q10_pipeline_view_t>&  pipeline_view,
    typename Backend::template Adapter<nation_t>&             nation,
    Q10Stats* stats = nullptr)
{
   using ViewAdapterT  = typename Backend::template Adapter<q10_pipeline_view_t>;
   using NationAdapter = typename Backend::template Adapter<nation_t>;

   Q10ViewLoadSink<ViewAdapterT> sink{pipeline_view, stats};
   // Empty cache — never touched in ViewLoad mode.
   std::unordered_map<Integer, Varchar<25>> nation_cache;
   Params dummy_params{};  // parameters don't matter — filters dropped
   Q10GroupWalkVisitor<NationAdapter,
                       Q10ViewLoadSink<ViewAdapterT>,
                       Q10FilterMode::ViewLoad>
       visitor{dummy_params, nation, sink, nation_cache, stats};
   ::tpch::col_group_walk<Backend>(merged_col, visitor);
}

// Forwarding sink for the per-order pre-aggregated view (variant B).
template <typename ViewAdapter>
struct Q10ViewPreaggLoadSink {
   ViewAdapter& adapter;
   Q10Stats*    stats         = nullptr;
   long         rows_inserted = 0;

   void emit_view_preagg(const q10_pipeline_view_preagg_t::Key& k,
                         const q10_pipeline_view_preagg_t&      v)
   {
      adapter.insert(k, v);
      ++rows_inserted;
   }
};

// populate_q10_view_preagg — Pattern B loader for the per-order view.
// Same single-walker discipline as populate_q10_view: the S3 walker in
// ViewLoadPreagg mode bakes l_returnflag='R' (sums returned revenue per
// order) and emits one q10_pipeline_view_preagg_t per order with returned
// revenue > 0. The date window stays live (applied at query time).
template <typename Backend>
void populate_q10_view_preagg(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<q10_pipeline_view_preagg_t>& pipeline_view_preagg,
    typename Backend::template Adapter<nation_t>&             nation,
    Q10Stats* stats = nullptr)
{
   using ViewAdapterT  = typename Backend::template Adapter<q10_pipeline_view_preagg_t>;
   using NationAdapter = typename Backend::template Adapter<nation_t>;

   Q10ViewPreaggLoadSink<ViewAdapterT> sink{pipeline_view_preagg, stats};
   std::unordered_map<Integer, Varchar<25>> nation_cache;
   Params dummy_params{};
   Q10GroupWalkVisitor<NationAdapter,
                       Q10ViewPreaggLoadSink<ViewAdapterT>,
                       Q10FilterMode::ViewLoadPreagg>
       visitor{dummy_params, nation, sink, nation_cache, stats};
   ::tpch::col_group_walk<Backend>(merged_col, visitor);
}

// ---------------------------------------------------------------------------
// aCOL MI loader (S5). The aCOL is MergedAdapter<customer_coli_t,
// orders_acol_t>: per custkey, one full customer record + one order-agg record
// per order with returned revenue > 0 (no lineitems). It is the per-order
// pre-aggregated view's data, but co-located by custkey (customer payload
// stored ONCE, not duplicated per order) and walked by a hand-rolled walker.

// Sink that turns the ViewLoadPreagg per-order emit into an orders_acol_t
// insert. The FD customer columns on the view row are ignored — the customer
// record is inserted by the separate customer sub-pass below. (Inserting into
// the aCOL MI while col_group_walk scans the COL MI is the same proven pattern
// as the preagg-view loader, which inserts into its view adapter mid-scan.)
template <typename AcolAdapter>
struct Q10AcolLoadSink {
   AcolAdapter& adapter;
   Q10Stats*    stats         = nullptr;
   long         rows_inserted = 0;

   void emit_view_preagg(const q10_pipeline_view_preagg_t::Key& k,
                         const q10_pipeline_view_preagg_t&      v)
   {
      adapter.template insert<orders_acol_t>(
          orders_acol_t::Key{k.custkey, k.orderkey},
          orders_acol_t{v.o_orderdate, v.returned_revenue});
      ++rows_inserted;
   }
};

// populate_q10_acol — build the aCOL MI in two sub-passes:
//   (1) customer base scan → insert customer_coli_t (full fidelity, all
//       customers — matches S3's customer set; the proven aCOLI pattern);
//   (2) col_group_walk over the S3 COL MI in ViewLoadPreagg mode → the sink
//       inserts one orders_acol_t per order with returned_revenue > 0.
// The per-order returnflag='R' revenue accumulation is reused verbatim from
// the preagg-view loader (Q10FilterMode::ViewLoadPreagg) — only the sink
// differs. Run AFTER col.populate_merged().
template <typename Backend>
void populate_q10_acol(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<customerh_t>&          customer,
    typename Backend::template MergedAdapter<customer_coli_t, orders_acol_t>& acol,
    typename Backend::template Adapter<nation_t>&             nation,
    Q10Stats* stats = nullptr)
{
   // Sub-pass 1: customer records (full fidelity).
   {
      auto sc = customer.getScanner();
      while (auto kv = sc->next()) {
         acol.template insert<customer_coli_t>(
             customer_coli_t::key_from_base(kv->first),
             customer_coli_t::from_base(kv->second));
      }
   }

   // Sub-pass 2: per-order returned-revenue aggregates.
   using AcolAdapterT  = typename Backend::template MergedAdapter<customer_coli_t,
                                                                  orders_acol_t>;
   using NationAdapter = typename Backend::template Adapter<nation_t>;
   Q10AcolLoadSink<AcolAdapterT> sink{acol, stats};
   std::unordered_map<Integer, Varchar<25>> nation_cache;
   Params dummy_params{};
   Q10GroupWalkVisitor<NationAdapter,
                       Q10AcolLoadSink<AcolAdapterT>,
                       Q10FilterMode::ViewLoadPreagg>
       visitor{dummy_params, nation, sink, nation_cache, stats};
   ::tpch::col_group_walk<Backend>(merged_col, visitor);
}

}  // namespace tpch::q10
