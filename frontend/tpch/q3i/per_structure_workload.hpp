#pragma once

// Per-storage-structure dispatch wrappers for Q3I.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q3i
{

template <typename Backend>
using BaseQ3I   = ::tpch::BaseStructure  <Q3IWorkload<Backend>, q3i_agg_row_t>;

template <typename Backend>
using ViewQ3I   = ::tpch::ViewStructure  <Q3IWorkload<Backend>, q3i_agg_row_t>;

template <typename Backend>
using MergedQ3I = ::tpch::MergedStructure<Q3IWorkload<Backend>, q3i_agg_row_t>;

template <typename Backend>
using HashQ3I        = ::tpch::HashStructure       <Q3IWorkload<Backend>, q3i_agg_row_t>;

template <typename Backend>
using AggregatedQ3I  = ::tpch::AggregatedStructure <Q3IWorkload<Backend>, q3i_agg_row_t>;

}  // namespace tpch::q3i
