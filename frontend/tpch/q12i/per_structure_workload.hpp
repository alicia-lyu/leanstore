#pragma once

// Per-storage-structure dispatch wrappers for Q12I.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q12i
{

template <typename Backend>
using BaseQ12I   = ::tpch::BaseStructure  <Q12IWorkload<Backend>, q12i_agg_row_t>;

template <typename Backend>
using ViewQ12I   = ::tpch::ViewStructure  <Q12IWorkload<Backend>, q12i_agg_row_t>;

template <typename Backend>
using MergedQ12I = ::tpch::MergedStructure<Q12IWorkload<Backend>, q12i_agg_row_t>;

template <typename Backend>
using HashQ12I   = ::tpch::HashStructure  <Q12IWorkload<Backend>, q12i_agg_row_t>;

}  // namespace tpch::q12i
