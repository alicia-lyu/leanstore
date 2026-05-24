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

}  // namespace tpch::q10
