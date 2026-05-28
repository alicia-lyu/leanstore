#pragma once

// Per-storage-structure dispatch wrappers for Q10I.
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q10i
{

template <typename Backend>
using BaseQ10I   = ::tpch::BaseStructure  <Q10IWorkload<Backend>, q10i_agg_row_t>;

template <typename Backend>
using ViewQ10I   = ::tpch::ViewStructure  <Q10IWorkload<Backend>, q10i_agg_row_t>;

template <typename Backend>
using MergedQ10I = ::tpch::MergedStructure<Q10IWorkload<Backend>, q10i_agg_row_t>;

template <typename Backend>
using HashQ10I   = ::tpch::HashStructure  <Q10IWorkload<Backend>, q10i_agg_row_t>;

// S5: aCOLI pre-aggregated merged index → query_by_aggregated.
template <typename Backend>
using AggregatedQ10I = ::tpch::AggregatedStructure<Q10IWorkload<Backend>, q10i_agg_row_t>;

// S7: per-order preagg view variant (q10i_pipeline_view_preagg_t). Parallel
// to PreaggViewQ10 (see ../q10/per_structure_workload.hpp). Cohort at S=7
// uses {ViewQ3I, ViewQ5I, PreaggViewQ10I}.
template <typename Backend>
using PreaggViewQ10I = ::tpch::PreaggViewStructure<Q10IWorkload<Backend>, q10i_agg_row_t>;

}  // namespace tpch::q10i
