#pragma once

// Family helper for the TPCHi invoice-extended cohort {Q3I, Q5I, Q10I}.
//
// All three queries share the COLI pipeline (4-table merged index
// `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t,
// invoice_coli_t>` + custkey-sorted split secondaries) and the
// TPCHIWorkload base loader. The three per-query views are query-specific
// (`q3i_pipeline_view_t`, `q5i_pipeline_view_t`, `q10i_pipeline_view_t`).
// Q10I additionally has a per-order preagg view
// (`q10i_pipeline_view_preagg_t` — the "S7" path) and a 2-type aCOLI MI
// (`MergedAdapter<customer_coli_t, orders_acoli_q10i_t>` — the "S5" path).
//
// Mirrors tpch_vanilla_family.hpp; see that header for the full rationale,
// including the S5/S7 heterogeneous cohort layout (Q3I/Q5I fall back to
// S3/S2 respectively; Q10I runs its native S5/S7).

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
#include "q10i/per_structure_workload.hpp"
#include "q10i/workload.hpp"

namespace tpch
{

template <typename Backend>
inline void load_tpchi_family(TPCHIWorkload<Backend::template Adapter>& tpch,
                              tpch::q3i::Q3IWorkload<Backend>&   q3i,
                              tpch::q5i::Q5IWorkload<Backend>&   q5i,
                              tpch::q10i::Q10IWorkload<Backend>& q10i)
{
   tpch.load();
   q3i.populate_secondaries();   // shared coli.populate_{split,merged} + Q3I view
   q5i.populate_view_only();     // Q5I view only
   q10i.populate_view_only();    // S2 naive view + S7 preagg view
   q10i.populate_acoli_only();   // S5 aCOLI MI
}

namespace detail::tpchi
{
template <typename Backend, int Structure>
struct TPCHiWrappers;

template <typename Backend>
struct TPCHiWrappers<Backend, 1> {
   tpch::q3i::BaseQ3I<Backend>   q3i;
   tpch::q5i::BaseQ5I<Backend>   q5i;
   tpch::q10i::BaseQ10I<Backend> q10i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>&   q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>&   q5i_w,
                 tpch::q10i::Q10IWorkload<Backend>& q10i_w)
       : q3i{q3i_w}, q5i{q5i_w}, q10i{q10i_w} {}
};
template <typename Backend>
struct TPCHiWrappers<Backend, 2> {
   tpch::q3i::ViewQ3I<Backend>   q3i;
   tpch::q5i::ViewQ5I<Backend>   q5i;
   tpch::q10i::ViewQ10I<Backend> q10i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>&   q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>&   q5i_w,
                 tpch::q10i::Q10IWorkload<Backend>& q10i_w)
       : q3i{q3i_w}, q5i{q5i_w}, q10i{q10i_w} {}
};
template <typename Backend>
struct TPCHiWrappers<Backend, 3> {
   tpch::q3i::MergedQ3I<Backend>   q3i;
   tpch::q5i::MergedQ5I<Backend>   q5i;
   tpch::q10i::MergedQ10I<Backend> q10i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>&   q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>&   q5i_w,
                 tpch::q10i::Q10IWorkload<Backend>& q10i_w)
       : q3i{q3i_w}, q5i{q5i_w}, q10i{q10i_w} {}
};
template <typename Backend>
struct TPCHiWrappers<Backend, 4> {
   tpch::q3i::HashQ3I<Backend>   q3i;
   tpch::q5i::HashQ5I<Backend>   q5i;
   tpch::q10i::HashQ10I<Backend> q10i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>&   q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>&   q5i_w,
                 tpch::q10i::Q10IWorkload<Backend>& q10i_w)
       : q3i{q3i_w}, q5i{q5i_w}, q10i{q10i_w} {}
};
// S=5: heterogeneous — Q3I/Q5I fall back to S3 (COLI MI), Q10I runs S5
// aCOLI. The image at S5 is shared with S3.
template <typename Backend>
struct TPCHiWrappers<Backend, 5> {
   tpch::q3i::MergedQ3I<Backend>        q3i;
   tpch::q5i::MergedQ5I<Backend>        q5i;
   tpch::q10i::AggregatedQ10I<Backend>  q10i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>&   q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>&   q5i_w,
                 tpch::q10i::Q10IWorkload<Backend>& q10i_w)
       : q3i{q3i_w}, q5i{q5i_w}, q10i{q10i_w} {}
};
// S=7: heterogeneous — Q3I/Q5I fall back to S2 (naive view), Q10I runs S7
// preagg view. Image at S7 shares the S2 image.
template <typename Backend>
struct TPCHiWrappers<Backend, 7> {
   tpch::q3i::ViewQ3I<Backend>          q3i;
   tpch::q5i::ViewQ5I<Backend>          q5i;
   tpch::q10i::PreaggViewQ10I<Backend>  q10i;
   TPCHiWrappers(tpch::q3i::Q3IWorkload<Backend>&   q3i_w,
                 tpch::q5i::Q5IWorkload<Backend>&   q5i_w,
                 tpch::q10i::Q10IWorkload<Backend>& q10i_w)
       : q3i{q3i_w}, q5i{q5i_w}, q10i{q10i_w} {}
};
}  // namespace detail::tpchi

template <typename Backend, int Structure>
inline std::vector<BgCatalogFn> register_tpchi_bg_catalog_at(
    DBTraits& db_traits,
    tpch::q3i::Q3IWorkload<Backend>&   q3i_workload,
    tpch::q5i::Q5IWorkload<Backend>&   q5i_workload,
    tpch::q10i::Q10IWorkload<Backend>& q10i_workload)
{
   using namespace detail::tpchi;
   auto wrappers = std::make_shared<TPCHiWrappers<Backend, Structure>>(
       q3i_workload, q5i_workload, q10i_workload);

   std::vector<BgCatalogFn> catalog;
   catalog.reserve(3);
   catalog.emplace_back([wrappers, &db_traits](u64 worker_id) {
      std::vector<tpch::q3i::q3i_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q3i.query(out); }, worker_id);
   });
   catalog.emplace_back([wrappers, &db_traits](u64 worker_id) {
      std::vector<tpch::q5i::q5i_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q5i.query(out); }, worker_id);
   });
   catalog.emplace_back([wrappers, &db_traits](u64 worker_id) {
      std::vector<tpch::q10i::q10i_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q10i.query(out); }, worker_id);
   });
   return catalog;
}

// Sample a random invoicekey from the loaded invoice range. Lives here
// (not in tpchi_workload.hpp) because tpchi_workload.hpp is in
// LOADING_META_FILES — bumping its mtime invalidates every persisted
// tpchi_*/build/<sf>.json and forces a fresh multi-minute reload.
// tpchi_family.hpp is bg-cohort-only, so changes here don't invalidate
// load images.
template <template <typename> class AdapterType>
inline Integer tpchi_random_invoicekey(TPCHIWorkload<AdapterType>& tpch)
{
   return urand(1, std::max(Integer(1), 2 * tpch.last_order_id));
}

// bg=2 cohort helper for the invoice-extended family. Same shape as
// make_tpch_point_lookup_step in tpch_vanilla_family.hpp, with the
// 9-table TPCHi base set (vanilla 8 + invoice).
template <typename Backend>
inline BgCatalogFn make_tpchi_point_lookup_step(
    DBTraits& db_traits,
    TPCHIWorkload<Backend::template Adapter>& tpch)
{
   return [&db_traits, &tpch](u64 worker_id) {
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

template <typename Backend>
inline std::vector<BgCatalogFn> register_tpchi_bg_catalog(
    DBTraits& db_traits,
    TPCHIWorkload<Backend::template Adapter>& /*tpch*/,
    tpch::q3i::Q3IWorkload<Backend>&   q3i_workload,
    tpch::q5i::Q5IWorkload<Backend>&   q5i_workload,
    tpch::q10i::Q10IWorkload<Backend>& q10i_workload,
    int structure,
    bool /*include_point_lookups*/)
{
   std::vector<BgCatalogFn> catalog;
   switch (structure) {
      case 1: catalog = register_tpchi_bg_catalog_at<Backend, 1>(db_traits, q3i_workload, q5i_workload, q10i_workload); break;
      case 2: catalog = register_tpchi_bg_catalog_at<Backend, 2>(db_traits, q3i_workload, q5i_workload, q10i_workload); break;
      case 3: catalog = register_tpchi_bg_catalog_at<Backend, 3>(db_traits, q3i_workload, q5i_workload, q10i_workload); break;
      case 4: catalog = register_tpchi_bg_catalog_at<Backend, 4>(db_traits, q3i_workload, q5i_workload, q10i_workload); break;
      case 5: catalog = register_tpchi_bg_catalog_at<Backend, 5>(db_traits, q3i_workload, q5i_workload, q10i_workload); break;
      case 7: catalog = register_tpchi_bg_catalog_at<Backend, 7>(db_traits, q3i_workload, q5i_workload, q10i_workload); break;
      default: throw std::runtime_error("register_tpchi_bg_catalog: invalid storage_structure");
   }
   return catalog;
}

}  // namespace tpch
