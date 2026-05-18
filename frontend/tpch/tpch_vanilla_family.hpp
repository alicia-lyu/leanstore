#pragma once

// Family helper for the vanilla TPC-H cohort {Q3, Q5}.
//
// Both queries share the same COL pipeline (3-table merged index
// `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_col_t>` + custkey-
// sorted split secondaries). The two per-query views are query-specific
// (`q3_pipeline_view_t` and `q5_pipeline_view_t`). Q12 is intentionally
// excluded because it uses a different pipeline (OL merged index over
// `orders_t` + `lineitem_t`) and would force the family image to carry
// unrelated secondaries.
//
// Two responsibilities:
//   1. load_vanilla_family(): one tpch.load() followed by every family
//      member's populate_secondaries(). Lets a per-query binary mount the
//      same .json image as any other family member.
//   2. register_vanilla_bg_steps(): build the type-erased BgStepFn vector
//      that TpchExecutableHelper consumes when --bg_query_thread=true.
//      Each step is a single TX of one family query at the supplied
//      foreground --storage_structure, routed through DBTraits on
//      BG_WORKER. The bg thread round-robins these steps for the full TX
//      window.
//
// The family member workload classes are passed in by reference — they
// must outlive the registered steps. Adapter ownership stays in the
// executable's main() (LeanStore CRM lifecycle constraints).

#include <functional>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "../shared/db_traits.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "tpch_executable_helper.hpp"
#include "tpch_workload.hpp"

#include "q3/per_structure_workload.hpp"
#include "q3/workload.hpp"
#include "q5/per_structure_workload.hpp"
#include "q5/workload.hpp"

namespace tpch
{

template <typename Backend>
inline void load_vanilla_family(TPCHWorkload<Backend::template Adapter>& tpch,
                                tpch::q3::Q3Workload<Backend>& q3,
                                tpch::q5::Q5Workload<Backend>& q5)
{
   tpch.load();
   q3.populate_secondaries();
   q5.populate_secondaries();
}

// Per-structure wrapper holders. Owned by the BgStepFn closures (each step
// pins one wrapper instance) so the wrappers outlive the bg thread. The
// vanilla family has two members; the foreground binary picks which
// structure they all run at.
namespace detail::vanilla
{
// One slot per (query, structure). Each slot is constructed lazily when
// register_vanilla_bg_steps() is called for that structure. Lives in a
// shared_ptr so the closure can capture by value without slicing or
// moving the wrapper.
template <typename Backend, int Structure>
struct VanillaWrappers;

template <typename Backend>
struct VanillaWrappers<Backend, 1> {
   tpch::q3::BaseQ3<Backend> q3;
   tpch::q5::BaseQ5<Backend> q5;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>& q3_w,
                   tpch::q5::Q5Workload<Backend>& q5_w)
       : q3{q3_w}, q5{q5_w} {}
};
template <typename Backend>
struct VanillaWrappers<Backend, 2> {
   tpch::q3::ViewQ3<Backend> q3;
   tpch::q5::ViewQ5<Backend> q5;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>& q3_w,
                   tpch::q5::Q5Workload<Backend>& q5_w)
       : q3{q3_w}, q5{q5_w} {}
};
template <typename Backend>
struct VanillaWrappers<Backend, 3> {
   tpch::q3::MergedQ3<Backend> q3;
   tpch::q5::MergedQ5<Backend> q5;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>& q3_w,
                   tpch::q5::Q5Workload<Backend>& q5_w)
       : q3{q3_w}, q5{q5_w} {}
};
template <typename Backend>
struct VanillaWrappers<Backend, 4> {
   tpch::q3::HashQ3<Backend> q3;
   tpch::q5::HashQ5<Backend> q5;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>& q3_w,
                   tpch::q5::Q5Workload<Backend>& q5_w)
       : q3{q3_w}, q5{q5_w} {}
};
}  // namespace detail::vanilla

template <typename Backend, int Structure>
inline std::vector<BgStepFn> register_vanilla_bg_steps_at(
    DBTraits& db_traits,
    tpch::q3::Q3Workload<Backend>& q3_workload,
    tpch::q5::Q5Workload<Backend>& q5_workload)
{
   using namespace detail::vanilla;
   auto wrappers = std::make_shared<VanillaWrappers<Backend, Structure>>(
       q3_workload, q5_workload);

   std::vector<BgStepFn> steps;
   steps.reserve(2);
   // Q3 step: run one TX of Q3 at Structure on BG_WORKER.
   steps.emplace_back([wrappers, &db_traits]() {
      std::vector<tpch::q3::q3_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q3.query(out); }, BG_WORKER);
   });
   // Q5 step: run one TX of Q5 at Structure on BG_WORKER.
   steps.emplace_back([wrappers, &db_traits]() {
      std::vector<tpch::q5::q5_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q5.query(out); }, BG_WORKER);
   });
   return steps;
}

// Convenience entry point that switches on FLAGS_storage_structure at runtime.
template <typename Backend>
inline std::vector<BgStepFn> register_vanilla_bg_steps(
    DBTraits& db_traits,
    tpch::q3::Q3Workload<Backend>& q3_workload,
    tpch::q5::Q5Workload<Backend>& q5_workload,
    int structure)
{
   switch (structure) {
      case 1: return register_vanilla_bg_steps_at<Backend, 1>(db_traits, q3_workload, q5_workload);
      case 2: return register_vanilla_bg_steps_at<Backend, 2>(db_traits, q3_workload, q5_workload);
      case 3: return register_vanilla_bg_steps_at<Backend, 3>(db_traits, q3_workload, q5_workload);
      case 4: return register_vanilla_bg_steps_at<Backend, 4>(db_traits, q3_workload, q5_workload);
      default: throw std::runtime_error("register_vanilla_bg_steps: invalid storage_structure");
   }
}

}  // namespace tpch
