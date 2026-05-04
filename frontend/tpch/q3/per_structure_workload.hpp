#pragma once

// Per-storage-structure dispatch wrappers for Q3.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.
//
// S5 (AggregatedStructure) is deliberately omitted — Q3 has no invoice table
// and therefore no parameter-independent aggregate to pre-bake.  See
// q3/CLAUDE.md §Open Questions — S5: omit (resolved).

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q3
{

template <typename Backend>
using BaseQ3   = ::tpch::BaseStructure  <Q3Workload<Backend>, q3_agg_row_t>;

template <typename Backend>
using ViewQ3   = ::tpch::ViewStructure  <Q3Workload<Backend>, q3_agg_row_t>;

template <typename Backend>
using MergedQ3 = ::tpch::MergedStructure<Q3Workload<Backend>, q3_agg_row_t>;

template <typename Backend>
using HashQ3   = ::tpch::HashStructure  <Q3Workload<Backend>, q3_agg_row_t>;

}  // namespace tpch::q3
