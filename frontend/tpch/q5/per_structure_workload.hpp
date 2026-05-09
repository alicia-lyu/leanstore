#pragma once

// Per-storage-structure dispatch wrappers for Q5.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.
//
// S5 (AggregatedStructure) is deliberately omitted — Q5 has no
// parameter-independent aggregate to bake (every per-row revenue contribution
// is gated by parameterised o_orderdate; see q5/CLAUDE.md §Storage Structure
// Options).

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q5
{

template <typename Backend>
using BaseQ5   = ::tpch::BaseStructure  <Q5Workload<Backend>, q5_agg_row_t>;

template <typename Backend>
using ViewQ5   = ::tpch::ViewStructure  <Q5Workload<Backend>, q5_agg_row_t>;

template <typename Backend>
using MergedQ5 = ::tpch::MergedStructure<Q5Workload<Backend>, q5_agg_row_t>;

template <typename Backend>
using HashQ5   = ::tpch::HashStructure  <Q5Workload<Backend>, q5_agg_row_t>;

}  // namespace tpch::q5
