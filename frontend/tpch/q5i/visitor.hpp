#pragma once

// Q5IGroupWalkVisitor — feeds the COLI group walker (S3 query body) and
// the Pattern B view loader (S2 load path) from the same in-pipeline
// implementation. Mode is a compile-time template parameter; dead
// branches drop out at instantiation.
//
// Hook contract: see tpchi_family/coli_pipeline.hpp. Every record-type
// hook returns tpch::WalkAction. Byte-lex order within a custkey group:
// customer → invoice* → (orders → lineitems*)+, so the invoice buffer is
// always complete by the time on_lineitem fires (CONVENTIONS Rule 10
// Pattern B).
//
// The visitor itself knows nothing about the supplier_nation_set probe,
// the per-nationkey aggregator, or n_name resolution — those live
// strictly post-pipeline (Q5IOutClass + q5i_resolve_n_names). The
// visitor's job ends at sink.emit(...).

#include <stdexcept>
#include <unordered_map>

#include "../tpch_family/views_coli.hpp"
#include "../tpch_family/walk_action.hpp"
#include "out_class.hpp"
#include "views.hpp"
#include "workload.hpp"  // Q5IStats, Params

// The visitor is templated on the Sides type so visitor.hpp doesn't
// have to include q5/side_tables.hpp, whose template body references
// a forward-declared q5::Params and won't parse without
// q5/workload.hpp ahead of it. Call sites (load.tpp, query.tpp)
// include q5/workload.hpp + q5/side_tables.hpp themselves and pass
// q5::Q5SideTables as the Sides type at instantiation.

namespace tpch::q5i
{

enum class Q5IFilterMode { Query, ViewLoad };

// Q5IGroupWalkVisitor<Sides, Sink, Mode>
//   Query mode    → Sink = Q5IOutClass<Sides>&, Sides = q5::Q5SideTables
//   ViewLoad mode → Sink = any object exposing emit_view(key, row) that
//                   writes into the pipeline_view adapter. Sides is
//                   ignored at runtime (an empty struct works fine).
template <typename Sides, typename Sink, Q5IFilterMode Mode>
struct Q5IGroupWalkVisitor {
   const Params& params;
   const Sides&  sides;
   Sink&         sink;
   Q5IStats*     stats = nullptr;

   // Per-group state, reset at on_group_end.
   Integer                                       cached_c_nationkey = 0;
   Timestamp                                     cached_o_orderdate = 0;
   std::unordered_map<Integer, invoice_coli_t>   invoice_buf;

   void on_record_visited()
   {
      if (stats) stats->mi_records_visited++;
   }

   void on_group_skipped(Integer /*ck*/)
   {
      if (stats) stats->mi_groups_skipped++;
   }

   // on_customer
   //   Query    : gate c_nationkey ∈ nation_set; SkipGroup on miss.
   //   ViewLoad : admit all customers.
   //   Both     : cache c_nationkey for downstream assembly.
   //   n_name resolution lives strictly post-pipeline (Q5IOutClass).
   ::tpch::WalkAction on_customer(Integer /*ck*/, const customer_coli_t& c)
   {
      if (stats) stats->customers_scanned++;
      if constexpr (Mode == Q5IFilterMode::Query) {
         if (sides.nation_set.count(c.c_nationkey) == 0) {
            return ::tpch::WalkAction::SkipGroup;
         }
      }
      cached_c_nationkey = c.c_nationkey;
      if (stats) stats->customers_passing_nation++;
      return ::tpch::WalkAction::Continue;
   }

   // on_invoice — Rule 10 Pattern B: buffer the **full** record. Field
   // extraction is forbidden inside this hook (CONVENTIONS.md §Rule 10).
   ::tpch::WalkAction on_invoice(const invoice_coli_t::Key& k,
                                 const invoice_coli_t&      i)
   {
      if (stats) stats->invoices_scanned++;
      invoice_buf.emplace(k.invoicekey, i);
      return ::tpch::WalkAction::Continue;
   }

   // on_order
   //   Query    : gate o_orderdate ∈ [lo, lo+365); SkipOrder on miss.
   //   ViewLoad : admit all (the orderdate is stored on the view).
   ::tpch::WalkAction on_order(const orders_coli_t::Key& /*k*/,
                               const orders_coli_t&      o)
   {
      if (stats) stats->orders_scanned++;
      if constexpr (Mode == Q5IFilterMode::Query) {
         if (o.o_orderdate < params.orderdate_lo
             || o.o_orderdate >= params.orderdate_lo + 365) {
            return ::tpch::WalkAction::SkipOrder;
         }
      }
      cached_o_orderdate = o.o_orderdate;
      if (stats) stats->orders_passing_date++;
      return ::tpch::WalkAction::Continue;
   }

   // on_lineitem — assemble the pipeline output record and emit. The
   // i_status is extracted here (at assembly time), not inside on_invoice.
   ::tpch::WalkAction on_lineitem(const lineitem_coli_t::Key& k,
                                  const lineitem_coli_t&      l)
   {
      if (stats) stats->lineitems_scanned++;
      auto it = invoice_buf.find(k.invoicekey);
      if (it == invoice_buf.end()) {
         throw std::runtime_error(
             "Q5IGroupWalkVisitor::on_lineitem: invoice missing from "
             "buffer (FK violation); custkey-group invariant broken");
      }
      const invoice_coli_t& inv = it->second;

      if constexpr (Mode == Q5IFilterMode::Query) {
         q5i_pipeline_out_t row{
             /*c_nationkey     */ cached_c_nationkey,
             /*l_suppkey       */ l.l_suppkey,
             /*l_extendedprice */ l.l_extendedprice,
             /*l_discount      */ l.l_discount,
             /*i_status        */ inv.i_status,
         };
         sink.emit(row);
      } else {
         typename q5i_pipeline_view_t::Key view_key{
             /*custkey   */ k.custkey,
             /*orderkey  */ k.orderkey,
             /*invoicekey*/ k.invoicekey,
             /*linenumber*/ k.linenumber,
         };
         q5i_pipeline_view_t row{
             /*l_extendedprice*/ l.l_extendedprice,
             /*l_discount     */ l.l_discount,
             /*l_suppkey      */ l.l_suppkey,
             /*c_nationkey    */ cached_c_nationkey,
             /*o_orderdate    */ cached_o_orderdate,
             /*i_status       */ inv.i_status,
         };
         sink.emit_view(view_key, row);
      }
      return ::tpch::WalkAction::Continue;
   }

   void on_group_end(Integer /*ck*/)
   {
      cached_c_nationkey = 0;
      cached_o_orderdate = 0;
      invoice_buf.clear();
   }
};

}  // namespace tpch::q5i
