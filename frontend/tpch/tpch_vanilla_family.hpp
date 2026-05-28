#pragma once

// Family helper for the vanilla TPC-H cohort {Q3, Q5, Q10}.
//
// All three queries share the COL pipeline (3-table merged index
// `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_col_t>` + custkey-
// sorted split secondaries). The three per-query views are query-specific
// (`q3_pipeline_view_t`, `q5_pipeline_view_t`, `q10_pipeline_view_t`); Q10
// additionally has a per-order preagg view (`q10_pipeline_view_preagg_t`,
// the "S7" path) and an aCOL MI (`MergedAdapter<customer_coli_t,
// orders_acol_t>`, the "S5" path). Q12 is intentionally excluded because
// it uses a different pipeline (OL merged index over `orders_t` +
// `lineitem_t`) and would force the family image to carry unrelated
// secondaries.
//
// Two responsibilities:
//   1. load_vanilla_family(): one tpch.load() followed by every family
//      member's secondaries. Lets a per-query binary mount the same .json
//      image as any other family member.
//   2. register_vanilla_bg_catalog(): build the type-erased BgCatalogFn
//      vector that TpchExecutableHelper consumes when
//      --bg_query_thread=true. Each entry is a single TX of one family
//      query at the supplied foreground --storage_structure, routed
//      through DBTraits on a dedicated CRM worker.
//
// S5 / S7 cohort fallback shape: only Q10 has a native S5 (aCOL MI) or S7
// (per-order preagg view). At those Sx the cohort still rotates all three
// queries — Q3/Q5 fall back to a sibling path that's present in the same
// recovered image:
//   S5 cohort = {Q3@S3 (COL MI), Q5@S3 (COL MI),  Q10@S5 (aCOL)}
//   S7 cohort = {Q3@S2 (view),   Q5@S2 (view),    Q10@S7 (preagg view)}
//
// The family member workload classes are passed in by reference — they
// must outlive the registered cohort. Adapter ownership stays in the
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
#include "q10/per_structure_workload.hpp"
#include "q10/workload.hpp"

namespace tpch
{

// Per-Sx load dispatch for the vanilla family.
//
// Each Sx has its own image directory (tpch_<backend>_S<N>/$(scale)), so
// loading only the structures for that Sx avoids growing every image to
// the full Cartesian footprint. S5 and S7 share images with S3 and S2
// respectively, so loading at S=3 covers both the S3 binary and the
// S=5 binary (Q10's aCOL co-resident with the COL MI); same for S=2/S=7.
//
// storage_structure values:
//   1 → S1 split secondaries only
//   2 → S2 views (Q3/Q5/Q10 naive + Q10 preagg) — same image serves S=7
//   3 → S3 COL MI + Q10 S5 aCOL — same image serves S=5
//   4 → base tables only (hash join needs no extra secondary)
//   5 → equivalent to 3 (S5 image == S3 image; loader still routes here)
//   6 → S6 col_shared_view (NOT yet wired into per-Sx; deferred — Q10's
//        S6 still in its standalone image)
//   7 → equivalent to 2 (S7 image == S2 image)
//   <0 → load everything (legacy monolithic image fallback)
template <typename Backend>
inline void load_vanilla_family(int storage_structure,
                                TPCHWorkload<Backend::template Adapter>& tpch,
                                tpch::q3::Q3Workload<Backend>&  q3,
                                tpch::q5::Q5Workload<Backend>&  q5,
                                tpch::q10::Q10Workload<Backend>& q10)
{
   tpch.load();
   // Normalize S5 → S3, S7 → S2 (image-sharing).
   const int sx = (storage_structure == 5) ? 3
                : (storage_structure == 7) ? 2
                : storage_structure;
   const bool all = (sx < 0);
   if (all || sx == 1) {
      // S1 split secondaries (col.populate_split is shared; q3 owns it).
      q3.col_pipeline().populate_split();
   }
   if (all || sx == 2) {
      // S2 views — Q3/Q5/Q10 naive + Q10 preagg. The view loaders all
      // walk the COL MI, so populate_merged must run first within the
      // same image. (At S=2 we don't want the MI in the final image;
      // however, the MI is consumed transiently by the loaders. For
      // RocksDB we let the merged-col data sit in the image
      // unused; for LeanStore it's the same. The disk cost is small
      // relative to the views themselves.)
      q3.col_pipeline().populate_merged();
      q3.populate_view_only();
      q5.populate_view_only();
      q10.populate_view_only();
   }
   if (all || sx == 3) {
      q3.col_pipeline().populate_merged();   // S3 COL MI
      q10.populate_acol_only();              // S5 aCOL MI (co-resident)
   }
   // S4: base tables only — nothing extra.
   // S6: deferred — Q10's col_shared_view stays in q10's standalone image.
}

// Per-structure wrapper holders. Owned by the BgCatalogFn closures (each
// catalog entry pins one wrapper instance) so the wrappers outlive their
// bg thread. The vanilla family has three members; the foreground binary
// picks which structure they all run at.
namespace detail::vanilla
{
template <typename Backend, int Structure>
struct VanillaWrappers;

template <typename Backend>
struct VanillaWrappers<Backend, 1> {
   tpch::q3::BaseQ3<Backend>   q3;
   tpch::q5::BaseQ5<Backend>   q5;
   tpch::q10::BaseQ10<Backend> q10;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>&  q3_w,
                   tpch::q5::Q5Workload<Backend>&  q5_w,
                   tpch::q10::Q10Workload<Backend>& q10_w)
       : q3{q3_w}, q5{q5_w}, q10{q10_w} {}
};
template <typename Backend>
struct VanillaWrappers<Backend, 2> {
   tpch::q3::ViewQ3<Backend>   q3;
   tpch::q5::ViewQ5<Backend>   q5;
   tpch::q10::ViewQ10<Backend> q10;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>&  q3_w,
                   tpch::q5::Q5Workload<Backend>&  q5_w,
                   tpch::q10::Q10Workload<Backend>& q10_w)
       : q3{q3_w}, q5{q5_w}, q10{q10_w} {}
};
template <typename Backend>
struct VanillaWrappers<Backend, 3> {
   tpch::q3::MergedQ3<Backend>   q3;
   tpch::q5::MergedQ5<Backend>   q5;
   tpch::q10::MergedQ10<Backend> q10;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>&  q3_w,
                   tpch::q5::Q5Workload<Backend>&  q5_w,
                   tpch::q10::Q10Workload<Backend>& q10_w)
       : q3{q3_w}, q5{q5_w}, q10{q10_w} {}
};
template <typename Backend>
struct VanillaWrappers<Backend, 4> {
   tpch::q3::HashQ3<Backend>   q3;
   tpch::q5::HashQ5<Backend>   q5;
   tpch::q10::HashQ10<Backend> q10;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>&  q3_w,
                   tpch::q5::Q5Workload<Backend>&  q5_w,
                   tpch::q10::Q10Workload<Backend>& q10_w)
       : q3{q3_w}, q5{q5_w}, q10{q10_w} {}
};
// S=5: heterogeneous — Q3/Q5 fall back to S3 (COL MI), Q10 runs S5 aCOL.
// The image at S5 is shared with S3 (per Linux disk-layout convention).
template <typename Backend>
struct VanillaWrappers<Backend, 5> {
   tpch::q3::MergedQ3<Backend>         q3;
   tpch::q5::MergedQ5<Backend>         q5;
   tpch::q10::AggregatedQ10<Backend>   q10;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>&  q3_w,
                   tpch::q5::Q5Workload<Backend>&  q5_w,
                   tpch::q10::Q10Workload<Backend>& q10_w)
       : q3{q3_w}, q5{q5_w}, q10{q10_w} {}
};
template <typename Backend>
struct VanillaWrappers<Backend, 6> {
   tpch::q3::SharedViewQ3<Backend>   q3;
   tpch::q5::SharedViewQ5<Backend>   q5;
   tpch::q10::SharedViewQ10<Backend> q10;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>&  q3_w,
                   tpch::q5::Q5Workload<Backend>&  q5_w,
                   tpch::q10::Q10Workload<Backend>& q10_w)
       : q3{q3_w}, q5{q5_w}, q10{q10_w} {}
};
// S=7: heterogeneous — Q3/Q5 fall back to S2 (naive view), Q10 runs S7
// preagg view. Image at S7 shares the S2 image.
template <typename Backend>
struct VanillaWrappers<Backend, 7> {
   tpch::q3::ViewQ3<Backend>          q3;
   tpch::q5::ViewQ5<Backend>          q5;
   tpch::q10::PreaggViewQ10<Backend>  q10;
   VanillaWrappers(tpch::q3::Q3Workload<Backend>&  q3_w,
                   tpch::q5::Q5Workload<Backend>&  q5_w,
                   tpch::q10::Q10Workload<Backend>& q10_w)
       : q3{q3_w}, q5{q5_w}, q10{q10_w} {}
};
}  // namespace detail::vanilla

template <typename Backend, int Structure>
inline std::vector<BgCatalogFn> register_vanilla_bg_catalog_at(
    DBTraits& db_traits,
    tpch::q3::Q3Workload<Backend>&  q3_workload,
    tpch::q5::Q5Workload<Backend>&  q5_workload,
    tpch::q10::Q10Workload<Backend>& q10_workload)
{
   using namespace detail::vanilla;
   auto wrappers = std::make_shared<VanillaWrappers<Backend, Structure>>(
       q3_workload, q5_workload, q10_workload);

   std::vector<BgCatalogFn> catalog;
   catalog.reserve(3);
   catalog.emplace_back([wrappers, &db_traits](u64 worker_id) {
      std::vector<tpch::q3::q3_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q3.query(out); }, worker_id);
   });
   catalog.emplace_back([wrappers, &db_traits](u64 worker_id) {
      std::vector<tpch::q5::q5_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q5.query(out); }, worker_id);
   });
   catalog.emplace_back([wrappers, &db_traits](u64 worker_id) {
      std::vector<tpch::q10::q10_agg_row_t> out;
      db_traits.run_tx([&]() { wrappers->q10.query(out); }, worker_id);
   });
   return catalog;
}

// Heterogeneous point-lookup closure over the 8 vanilla TPC-H base tables.
// Intended for `helper.set_bg_lookup_step(...)` so it runs on the dedicated
// BG_LOOKUP_WORKER (decoupled from the cohort on BG_WORKER + ...).
// Tolerates not-found via tryLookup (LeanStore lookup1 throws on miss;
// TPC-H PK ranges are sparse, e.g. orderkey populates only 8/32 sequential
// slots, so random PKs miss often). Identical to the helper's built-in
// bg_point_lookup() — kept as a free function for explicit setter use.
template <typename Backend>
inline BgCatalogFn make_tpch_point_lookup_step(
    DBTraits& db_traits,
    TPCHWorkload<Backend::template Adapter>& tpch)
{
   return [&db_traits, &tpch](u64 worker_id) {
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
      }, worker_id);
   };
}

// Convenience entry point that switches on FLAGS_storage_structure at runtime.
// Returns the cohort catalog vector only — point-lookups are owned by the
// helper's dedicated BG_LOOKUP_WORKER thread (see TpchExecutableHelper).
// The `include_point_lookups` parameter is kept for source-compat with
// existing callers but is now unused: pass any value.
template <typename Backend>
inline std::vector<BgCatalogFn> register_vanilla_bg_catalog(
    DBTraits& db_traits,
    TPCHWorkload<Backend::template Adapter>& /*tpch*/,
    tpch::q3::Q3Workload<Backend>&  q3_workload,
    tpch::q5::Q5Workload<Backend>&  q5_workload,
    tpch::q10::Q10Workload<Backend>& q10_workload,
    int structure,
    bool /*include_point_lookups*/)
{
   std::vector<BgCatalogFn> catalog;
   switch (structure) {
      case 1: catalog = register_vanilla_bg_catalog_at<Backend, 1>(db_traits, q3_workload, q5_workload, q10_workload); break;
      case 2: catalog = register_vanilla_bg_catalog_at<Backend, 2>(db_traits, q3_workload, q5_workload, q10_workload); break;
      case 3: catalog = register_vanilla_bg_catalog_at<Backend, 3>(db_traits, q3_workload, q5_workload, q10_workload); break;
      case 4: catalog = register_vanilla_bg_catalog_at<Backend, 4>(db_traits, q3_workload, q5_workload, q10_workload); break;
      case 5: catalog = register_vanilla_bg_catalog_at<Backend, 5>(db_traits, q3_workload, q5_workload, q10_workload); break;
      case 6: catalog = register_vanilla_bg_catalog_at<Backend, 6>(db_traits, q3_workload, q5_workload, q10_workload); break;
      case 7: catalog = register_vanilla_bg_catalog_at<Backend, 7>(db_traits, q3_workload, q5_workload, q10_workload); break;
      default: throw std::runtime_error("register_vanilla_bg_catalog: invalid storage_structure");
   }
   return catalog;
}

}  // namespace tpch
