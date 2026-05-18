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
   // q3 owns the family-shared col.populate_{split,merged} via its full
   // populate_secondaries() and additionally populates its own pipeline
   // view. q5 only populates its own view here so the family-shared
   // adapters (merged_col, split_orders, split_lineitem — which both
   // workloads hold references to) aren't written twice. LeanStore
   // B-tree returns OP_RESULT::DUPLICATE on the second insert; RocksDB
   // silently overwrites and hid this bug.
   q3.populate_secondaries();
   q5.populate_view_only();
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

// bg=2 cohort helper: append a heterogeneous point-lookup step that picks a
// random base table and random PK from the loaded vanilla TPC-H set (8
// tables) and calls adapter.lookup1. Tolerates not-found via tryLookup
// (LeanStore lookup1 throws on miss; TPC-H PK ranges are sparse, e.g.
// orderkey populates only 8/32 sequential slots, so random PKs miss often).
// Routed through DBTraits::run_tx on BG_WORKER as one TX per call.
template <typename Backend>
inline BgStepFn make_tpch_point_lookup_step(
    DBTraits& db_traits,
    TPCHWorkload<Backend::template Adapter>& tpch)
{
   return [&db_traits, &tpch]() {
      // 8 vanilla base tables; pick one uniformly per call.
      const Integer pick = urand(0, 7);
      db_traits.run_tx([&]() {
         switch (pick) {
            case 0: {
               part_t::Key k{tpch.getPartID()};
               tpch.part.tryLookup(k, [](const part_t&) {});
               break;
            }
            case 1: {
               supplier_t::Key k{tpch.getSupplierID()};
               tpch.supplier.tryLookup(k, [](const supplier_t&) {});
               break;
            }
            case 2: {
               partsupp_t::Key k{tpch.getPartID(), tpch.getSupplierID()};
               tpch.partsupp.tryLookup(k, [](const partsupp_t&) {});
               break;
            }
            case 3: {
               customerh_t::Key k{tpch.getCustomerID()};
               tpch.customer.tryLookup(k, [](const customerh_t&) {});
               break;
            }
            case 4: {
               orders_t::Key k{tpch.getOrderID()};
               tpch.orders.tryLookup(k, [](const orders_t&) {});
               break;
            }
            case 5: {
               lineitem_t::Key k{tpch.getOrderID(), urand(1, 7)};
               tpch.lineitem.tryLookup(k, [](const lineitem_t&) {});
               break;
            }
            case 6: {
               nation_t::Key k{tpch.getNationID()};
               tpch.nation.tryLookup(k, [](const nation_t&) {});
               break;
            }
            case 7:
            default: {
               region_t::Key k{tpch.getRegionID()};
               tpch.region.tryLookup(k, [](const region_t&) {});
               break;
            }
         }
      }, BG_WORKER);
   };
}

// Convenience entry point that switches on FLAGS_storage_structure at runtime.
// If include_point_lookups is true (bg=2), append the heterogeneous
// point-lookup step to the cohort returned for that structure.
template <typename Backend>
inline std::vector<BgStepFn> register_vanilla_bg_steps(
    DBTraits& db_traits,
    TPCHWorkload<Backend::template Adapter>& tpch,
    tpch::q3::Q3Workload<Backend>& q3_workload,
    tpch::q5::Q5Workload<Backend>& q5_workload,
    int structure,
    bool include_point_lookups)
{
   std::vector<BgStepFn> steps;
   switch (structure) {
      case 1: steps = register_vanilla_bg_steps_at<Backend, 1>(db_traits, q3_workload, q5_workload); break;
      case 2: steps = register_vanilla_bg_steps_at<Backend, 2>(db_traits, q3_workload, q5_workload); break;
      case 3: steps = register_vanilla_bg_steps_at<Backend, 3>(db_traits, q3_workload, q5_workload); break;
      case 4: steps = register_vanilla_bg_steps_at<Backend, 4>(db_traits, q3_workload, q5_workload); break;
      default: throw std::runtime_error("register_vanilla_bg_steps: invalid storage_structure");
   }
   if (include_point_lookups) {
      steps.push_back(make_tpch_point_lookup_step<Backend>(db_traits, tpch));
   }
   return steps;
}

}  // namespace tpch
