#pragma once

// Family helper for the TPCHi invoice-extended cohort {Q3I, Q5I}.
//
// Both queries share the COLI pipeline (4-table merged index
// `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t,
// invoice_coli_t>` + custkey-sorted split secondaries + aCOLI MI for S5)
// and the TPCHIWorkload base loader. The two per-query views are
// query-specific (`q3i_pipeline_view_t` and `q5i_pipeline_view_t`). Q10I is
// design-doc only at the time of this header; once it lands it should be
// added here.
//
// Mirrors tpch_vanilla_family.hpp; see that header for the full rationale.

#include <functional>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "../shared/db_traits.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "tpch_executable_helper.hpp"
#include "tpchi_family/tpchi_workload.hpp"

#include "q3i/per_structure_workload.hpp"
#include "q3i/workload.hpp"
#include "q5i/per_structure_workload.hpp"
#include "q5i/workload.hpp"

namespace tpch
{

template <typename Backend>
inline void load_tpchi_family(TPCHIWorkload<Backend::template Adapter>& tpch,
                              tpch::q3i::Q3IWorkload<Backend>& q3i,
                              tpch::q5i::Q5IWorkload<Backend>& q5i)
{
   tpch.load();
   // q3i owns the family-shared coli.populate_{split,merged,aggregated} via
   // its full populate_secondaries() and additionally populates its own
   // pipeline view. q5i only populates its own view so the family-shared
   // adapters (held by reference inside both q3i.coli and q5i.coli) aren't
   // written twice. LeanStore B-tree returns OP_RESULT::DUPLICATE on the
   // second insert; RocksDB silently overwrites and hid this bug.
   q3i.populate_secondaries();
   q5i.populate_view_only();
}

namespace detail::tpchi
{
template <typename Backend, int Structure>
struct TPCHiWrappers;

template <typename Backend>
struct TPCHiWrappers<Backend, 1> {
   tpch::q3i::BaseQ3I<Backend>   q3i;
   tpch::q5i::BaseQ5I<Backend>   q5i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>& q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>& q5i_w)
       : q3i{q3i_w}, q5i{q5i_w} {}
};
template <typename Backend>
struct TPCHiWrappers<Backend, 2> {
   tpch::q3i::ViewQ3I<Backend>   q3i;
   tpch::q5i::ViewQ5I<Backend>   q5i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>& q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>& q5i_w)
       : q3i{q3i_w}, q5i{q5i_w} {}
};
template <typename Backend>
struct TPCHiWrappers<Backend, 3> {
   tpch::q3i::MergedQ3I<Backend> q3i;
   tpch::q5i::MergedQ5I<Backend> q5i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>& q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>& q5i_w)
       : q3i{q3i_w}, q5i{q5i_w} {}
};
template <typename Backend>
struct TPCHiWrappers<Backend, 4> {
   tpch::q3i::HashQ3I<Backend>   q3i;
   tpch::q5i::HashQ5I<Backend>   q5i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>& q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>& q5i_w)
       : q3i{q3i_w}, q5i{q5i_w} {}
};
// S5 is Q3I-only (Q5I has no aCOLI variant — see q5i/CLAUDE.md §S5 deferred).
// When foreground=S5 the bg cohort drops Q5I and includes Q3I only.
template <typename Backend>
struct TPCHiWrappers<Backend, 5> {
   tpch::q3i::AggregatedQ3I<Backend> q3i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>& q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>& /*q5i_w*/)
       : q3i{q3i_w} {}
};
}  // namespace detail::tpchi

template <typename Backend, int Structure>
inline std::vector<BgStepFn> register_tpchi_bg_steps_at(
    DBTraits& db_traits,
    tpch::q3i::Q3IWorkload<Backend>& q3i_workload,
    tpch::q5i::Q5IWorkload<Backend>& q5i_workload)
{
   using namespace detail::tpchi;
   auto wrappers = std::make_shared<TPCHiWrappers<Backend, Structure>>(
       q3i_workload, q5i_workload);

   std::vector<BgStepFn> steps;
   steps.reserve(2);
   steps.emplace_back([wrappers, &db_traits]() {
      std::vector<tpch::q3i::q3i_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q3i.query(out); }, BG_WORKER);
   });
   if constexpr (Structure != 5) {
      steps.emplace_back([wrappers, &db_traits]() {
         std::vector<tpch::q5i::q5i_agg_row_t> out;
         db_traits.run_tx([&]() { wrappers->q5i.query(out); }, BG_WORKER);
      });
   }
   return steps;
}

template <typename Backend>
inline std::vector<BgStepFn> register_tpchi_bg_steps(
    DBTraits& db_traits,
    tpch::q3i::Q3IWorkload<Backend>& q3i_workload,
    tpch::q5i::Q5IWorkload<Backend>& q5i_workload,
    int structure)
{
   switch (structure) {
      case 1: return register_tpchi_bg_steps_at<Backend, 1>(db_traits, q3i_workload, q5i_workload);
      case 2: return register_tpchi_bg_steps_at<Backend, 2>(db_traits, q3i_workload, q5i_workload);
      case 3: return register_tpchi_bg_steps_at<Backend, 3>(db_traits, q3i_workload, q5i_workload);
      case 4: return register_tpchi_bg_steps_at<Backend, 4>(db_traits, q3i_workload, q5i_workload);
      case 5: return register_tpchi_bg_steps_at<Backend, 5>(db_traits, q3i_workload, q5i_workload);
      default: throw std::runtime_error("register_tpchi_bg_steps: invalid storage_structure");
   }
}

}  // namespace tpch
