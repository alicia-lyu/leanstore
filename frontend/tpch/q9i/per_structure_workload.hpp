#pragma once

// Per-storage-structure dispatch wrappers for Q9I.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q9i
{

template <typename Backend>
using BaseQ9I   = ::tpch::BaseStructure  <Q9IWorkload<Backend>, q9i_agg_row_t>;

template <typename Backend>
using ViewQ9I   = ::tpch::ViewStructure  <Q9IWorkload<Backend>, q9i_agg_row_t>;

template <typename Backend>
using MergedQ9I = ::tpch::MergedStructure<Q9IWorkload<Backend>, q9i_agg_row_t>;

template <typename Backend>
using HashQ9I   = ::tpch::HashStructure  <Q9IWorkload<Backend>, q9i_agg_row_t>;

}  // namespace tpch::q9i
