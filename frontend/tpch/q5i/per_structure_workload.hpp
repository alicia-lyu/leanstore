#pragma once

// Per-storage-structure dispatch wrappers for Q5I.
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q5i
{

template <typename Backend>
using BaseQ5I   = ::tpch::BaseStructure  <Q5IWorkload<Backend>, q5i_agg_row_t>;

template <typename Backend>
using ViewQ5I   = ::tpch::ViewStructure  <Q5IWorkload<Backend>, q5i_agg_row_t>;

template <typename Backend>
using MergedQ5I = ::tpch::MergedStructure<Q5IWorkload<Backend>, q5i_agg_row_t>;

template <typename Backend>
using HashQ5I   = ::tpch::HashStructure  <Q5IWorkload<Backend>, q5i_agg_row_t>;

}  // namespace tpch::q5i
