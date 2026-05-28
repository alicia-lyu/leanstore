#pragma once

// Per-storage-structure dispatch wrappers for Q10.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.
//
// S5 (AggregatedStructure) — aCOL MI, the per-order pre-aggregated COL merged
// index. D8's original "no S5" applied only to per-CUSTOMER pre-agg; the
// per-ORDER grain is sound (orderdate filters at order grain, returnflag is a
// spec constant), so S5 is revived for Q10. See q10/CLAUDE.md §Storage
// Structure Options and q10/PERFORMANCE.md.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q10
{

template <typename Backend>
using BaseQ10   = ::tpch::BaseStructure  <Q10Workload<Backend>, q10_agg_row_t>;

template <typename Backend>
using ViewQ10   = ::tpch::ViewStructure  <Q10Workload<Backend>, q10_agg_row_t>;

template <typename Backend>
using MergedQ10 = ::tpch::MergedStructure<Q10Workload<Backend>, q10_agg_row_t>;

template <typename Backend>
using HashQ10   = ::tpch::HashStructure  <Q10Workload<Backend>, q10_agg_row_t>;

template <typename Backend>
using AggregatedQ10 = ::tpch::AggregatedStructure<Q10Workload<Backend>, q10_agg_row_t>;

// S7: per-order preagg view variant (q10_pipeline_view_preagg_t). Always
// takes the preagg arm; the legacy --q10_view_variant flag continues to
// drive ViewQ10 (S2). Cohort at S=7 uses {ViewQ3, ViewQ5, PreaggViewQ10}.
template <typename Backend>
using PreaggViewQ10 = ::tpch::PreaggViewStructure<Q10Workload<Backend>, q10_agg_row_t>;

// S6: the COL-family shared materialised view (col_shared_view_t).
template <typename Backend>
using SharedViewQ10 = ::tpch::SharedViewStructure<Q10Workload<Backend>, q10_agg_row_t>;

}  // namespace tpch::q10
