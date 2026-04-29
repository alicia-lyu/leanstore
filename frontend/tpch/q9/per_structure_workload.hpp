#pragma once

// Per-storage-structure dispatch wrappers for Q9.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q9
{

template <typename Backend>
using BaseQ9   = ::tpch::BaseStructure  <Q9Workload<Backend>, q9_agg_row_t>;

template <typename Backend>
using ViewQ9   = ::tpch::ViewStructure  <Q9Workload<Backend>, q9_agg_row_t>;

template <typename Backend>
using MergedQ9 = ::tpch::MergedStructure<Q9Workload<Backend>, q9_agg_row_t>;

template <typename Backend>
using HashQ9   = ::tpch::HashStructure  <Q9Workload<Backend>, q9_agg_row_t>;

}  // namespace tpch::q9
