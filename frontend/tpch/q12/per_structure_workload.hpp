#pragma once

// Per-storage-structure dispatch wrappers for Q12.
//
// Aliases to the shared tpch::BaseStructure / ViewStructure / MergedStructure /
// HashStructure templates in frontend/tpch/per_structure_workload.hpp.
// Method bodies (pure forwarders to w.query_by_*) live in that shared header.

#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q12
{

template <typename Backend>
using BaseQ12   = ::tpch::BaseStructure  <Q12Workload<Backend>, q12_agg_row_t>;

template <typename Backend>
using ViewQ12   = ::tpch::ViewStructure  <Q12Workload<Backend>, q12_agg_row_t>;

template <typename Backend>
using MergedQ12 = ::tpch::MergedStructure<Q12Workload<Backend>, q12_agg_row_t>;

template <typename Backend>
using HashQ12   = ::tpch::HashStructure  <Q12Workload<Backend>, q12_agg_row_t>;

}  // namespace tpch::q12
