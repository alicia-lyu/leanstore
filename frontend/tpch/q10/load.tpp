// Template method bodies for Q10Workload<Backend>: ctor, load, get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>

#include "../q10_family/view_loaders.hpp"
#include "../tpch_family/shared_view_loader.hpp"
#include "../tpch_family/size_audit.hpp"

DECLARE_int32(storage_structure);
DECLARE_string(q10_view_variant);

namespace tpch::q10
{

template <typename Backend>
Q10Workload<Backend>::Q10Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<customerh_t>&  customer,
    typename Backend::template Adapter<orders_t>&     orders,
    typename Backend::template Adapter<lineitem_t>&   lineitem,
    typename Backend::template Adapter<nation_t>&     nation,
    typename Backend::template Adapter<q10_pipeline_view_t>& pipeline_view,
    typename Backend::template Adapter<q10_pipeline_view_preagg_t>& pipeline_view_preagg,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_col_t>& split_lineitem,
    typename Backend::template MergedAdapter<customer_coli_t, orders_acol_t>& acol,
    typename Backend::template Adapter<col_shared_view_t>& shared_view)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      nation(nation),
      col(customer, orders, lineitem, merged_col, split_orders, split_lineitem),
      pipeline_view(pipeline_view),
      pipeline_view_preagg(pipeline_view_preagg),
      acol(acol),
      shared_view(shared_view),
      params(Params::defaults())
{
}

// Populate every secondary unconditionally so a single DB image can serve
// all four --storage_structure query-time variants (PLAYBOOK §3.6 PITFALL
// — `load()` populating only one secondary).
//
// S2 view loader uses Pattern B (Decision E1): populate_q10_view reuses
// the S3 Q10GroupWalkVisitor in ViewLoad mode with parameterised filters
// dropped and a forwarding sink into pipeline_view. Same walker code
// drives both — view-vs-walker drift is structurally impossible.
// S2 variant A (per-lineitem) + variant B (per-order pre-agg, aka S7). Both
// loaders consume the COL MI; caller must have run col.populate_merged()
// already. One image carries both A/B so --q10_view_variant selects the arm
// at query time for S2; S7 always reads the preagg variant.
template <typename Backend>
void Q10Workload<Backend>::populate_view_only()
{
   populate_q10_view<Backend>(col.merged_adapter(), pipeline_view,
                               nation, stats);
   populate_q10_view_preagg<Backend>(col.merged_adapter(), pipeline_view_preagg,
                                      nation, stats);
}

// S5: aCOL MI (customer_coli_t + orders_acol_t). Built from the COL MI
// (same per-order returned-revenue accumulation as the preagg view) plus
// a customer base scan for the full customer records. Caller must have run
// col.populate_merged() already.
template <typename Backend>
void Q10Workload<Backend>::populate_acol_only()
{
   populate_q10_acol<Backend>(col.merged_adapter(), customer, acol,
                              nation, stats);
}

template <typename Backend>
void Q10Workload<Backend>::load()
{
   tpch.load();
   col.populate_split();    // S1: custkey-sorted split indexes
   col.populate_merged();   // S3: COL merged index
   populate_view_only();    // S2 (naive) + S7 (preagg)
   // S4: base tables only — nothing to populate.
   populate_acol_only();    // S5
   // S6: COL-family shared union view (per-lineitem; same grain as the S2
   // variant-A view). Walks the COL MI just built. q10 keeps a byte-identical
   // copy in its standalone image (the union view is family-shared in concept;
   // its footprint is counted once — see paper-data/scripts/space_table.py).
   tpch::populate_col_shared_view<Backend>(col.merged_adapter(), shared_view);
}

template <typename Backend>
double Q10Workload<Backend>::get_size() const
{
   // Per-adapter sum across what THIS binary sees. Per-Sx image-sharing
   // (S5↔S3, S7↔S2) plus the view loader's transient `populate_merged()`
   // mean the image carries co-resident structures beyond what one switch
   // case naïvely names. Cases below include every co-resident structure
   // Q10 has a handle for. NATION is in the base set (D6).
   double base = customer.size() + orders.size() + lineitem.size()
               + nation.size();
   double sum;
   switch (FLAGS_storage_structure) {
      case 1: sum = base + col.get_split_size(); break;
      // S2 image: base + COL MI + own naive + own preagg + sibling Q3/Q5 views
      // (sibling views invisible — see size_audit). FLAGS_q10_view_variant
      // toggles which arm the FOREGROUND query reads; the image carries both.
      case 2: sum = base + col.get_merged_size()
                    + pipeline_view.size() + pipeline_view_preagg.size();
              break;
      // S3 image: base + COL MI + Q10 aCOL (S5 shared image).
      case 3: sum = base + col.get_merged_size() + acol.size(); break;
      case 4: sum = base; break;
      // S5 shares the S3 image; report both COL MI and aCOL (S5's COL MI is
      // walked by bg cohort Q3/Q5; aCOL is the foreground structure).
      case 5: sum = base + col.get_merged_size() + acol.size(); break;
      case 6: sum = base + shared_view.size(); break;
      // S7 shares the S2 image. Foreground Q10 reads preagg; bg cohort Q3/Q5
      // read their views. Count only the preagg (Q10's working set). The
      // COL MI is co-resident from S2 loader but not consumed at S7 query
      // time — user directive 2026-05-28: S7 size should NOT include COL MI.
      // Sibling Q3/Q5 views are also in the image but invisible to this
      // binary (sanity log catches the residual gap).
      case 7: sum = base + pipeline_view_preagg.size(); break;
      default: throw std::runtime_error("invalid --storage_structure");
   }
   log_size_audit("q10", sum);
   return sum;
}

}  // namespace tpch::q10
