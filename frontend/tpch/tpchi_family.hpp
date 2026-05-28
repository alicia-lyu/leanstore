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
   steps.emplace_back([wrappers, &db_traits](u64 worker_id) {
      std::vector<tpch::q3i::q3i_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q3i.query(out); }, worker_id);
   });
   if constexpr (Structure != 5) {
      steps.emplace_back([wrappers, &db_traits](u64 worker_id) {
         std::vector<tpch::q5i::q5i_agg_row_t> out;
         db_traits.run_tx([&]() { wrappers->q5i.query(out); }, worker_id);
      });
   }
   return steps;
}

// Sample a random invoicekey from the loaded invoice range. Lives here
// (not in tpchi_workload.hpp) because tpchi_workload.hpp is in
// LOADING_META_FILES — bumping its mtime invalidates every persisted
// tpchi_*/build/<sf>.json and forces a fresh multi-minute reload.
// tpchi_family.hpp is bg-cohort-only, so changes here don't invalidate
// load images.
//
// Per loadInvoiceAndLinkLineitem(), invoicekeys are dense
// [1 .. 2 * |orders|] (~2 invoices per order). `last_order_id` is the
// sparse last orderkey, which slightly over-estimates the upper bound
// — the sparse mapping multiplies by 32/8. For a contention workload
// that's fine: occasional misses are tolerated by tryLookup.
template <template <typename> class AdapterType>
inline Integer tpchi_random_invoicekey(TPCHIWorkload<AdapterType>& tpch)
{
   return urand(1, std::max(Integer(1), 2 * tpch.last_order_id));
}

// bg=2 cohort helper for the invoice-extended family. Same shape as
// make_tpch_point_lookup_step in tpch_vanilla_family.hpp, with the
// 9-table TPCHi base set (vanilla 8 + invoice). The lineitem adapter
// here is typed on lineitem_i_t (FK-bearing variant); the cohort still
// works on PK ((l_orderkey, l_linenumber)) which is unchanged from
// lineitem_t.
template <typename Backend>
inline BgStepFn make_tpchi_point_lookup_step(
    DBTraits& db_traits,
    TPCHIWorkload<Backend::template Adapter>& tpch)
{
   return [&db_traits, &tpch](u64 worker_id) {
      // 9 invoice-extended base tables; pick one uniformly per call.
      const Integer pick = urand(0, 8);
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
               lineitem_i_t::Key k{tpch.getOrderID(), urand(1, 7)};
               tpch.lineitem.tryLookup(k, [](const lineitem_i_t&) {});
               break;
            }
            case 6: {
               nation_t::Key k{tpch.getNationID()};
               tpch.nation.tryLookup(k, [](const nation_t&) {});
               break;
            }
            case 7: {
               region_t::Key k{tpch.getRegionID()};
               tpch.region.tryLookup(k, [](const region_t&) {});
               break;
            }
            case 8:
            default: {
               invoice_t::Key k{tpchi_random_invoicekey(tpch)};
               tpch.invoice.tryLookup(k, [](const invoice_t&) {});
               break;
            }
         }
      }, worker_id);
   };
}

// Returns the cohort step vector only — point-lookups are owned by the
// helper's dedicated BG_LOOKUP_WORKER thread. TPCHi callers should
// additionally call `helper.set_bg_lookup_step(
// make_tpchi_point_lookup_step<B>(db_traits, tpch))` to include the
// invoice table in the lookup distribution; otherwise the helper falls
// back to its built-in 8-table vanilla lookup. The
// `include_point_lookups` parameter is kept for source-compat but is
// now unused.
template <typename Backend>
inline std::vector<BgStepFn> register_tpchi_bg_steps(
    DBTraits& db_traits,
    TPCHIWorkload<Backend::template Adapter>& /*tpch*/,
    tpch::q3i::Q3IWorkload<Backend>& q3i_workload,
    tpch::q5i::Q5IWorkload<Backend>& q5i_workload,
    int structure,
    bool /*include_point_lookups*/)
{
   std::vector<BgStepFn> steps;
   switch (structure) {
      case 1: steps = register_tpchi_bg_steps_at<Backend, 1>(db_traits, q3i_workload, q5i_workload); break;
      case 2: steps = register_tpchi_bg_steps_at<Backend, 2>(db_traits, q3i_workload, q5i_workload); break;
      case 3: steps = register_tpchi_bg_steps_at<Backend, 3>(db_traits, q3i_workload, q5i_workload); break;
      case 4: steps = register_tpchi_bg_steps_at<Backend, 4>(db_traits, q3i_workload, q5i_workload); break;
      case 5: steps = register_tpchi_bg_steps_at<Backend, 5>(db_traits, q3i_workload, q5i_workload); break;
      default: throw std::runtime_error("register_tpchi_bg_steps: invalid storage_structure");
   }
   return steps;
}

}  // namespace tpch
