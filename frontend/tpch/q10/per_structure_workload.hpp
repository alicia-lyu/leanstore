#pragma once

// Per-storage-structure dispatch wrappers for Q10.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.
//
// S5 (AggregatedStructure) is deliberately omitted (Decision D8) — Q10 has
// no parameter-independent aggregate to bake; orderdate is parameterised.
// See q10/CLAUDE.md §Storage Structure Options.

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

}  // namespace tpch::q10
