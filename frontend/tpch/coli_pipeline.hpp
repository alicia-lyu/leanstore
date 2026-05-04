#pragma once

// CUSTOMER × ORDERS × LINEITEM × INVOICE merged-index substrate.
//
// This pipeline owns MI(customer_coli_t, orders_coli_t, lineitem_coli_t,
// invoice_coli_t) keyed by Calcite-style tagged keys and exposes:
//
//   populate_merged()      — dual-write replay over the four base adapters
//   get_merged_size()      — estimated size in MiB of merged_coli
//   populate_split()       — build three custkey-sorted split indexes
//                            (orders, lineitem, invoice) for S1 BinaryMergeJoin
//   get_split_size()       — sum of the three split adapter sizes in MiB
//
// COLIGroupWalk driver (coli_group_walk):
//   A pure-dispatch iterator over the merged index parameterized by a Visitor.
//   The walker calls the appropriate Visitor hook for each record and
//   on_group_end at each custkey boundary. All query-specific assembly and
//   aggregation live in the Visitor.
//
// Operator drivers (PremergedJoin / BinaryMergeJoin / HashJoin) and view
// loading are per-query and live in q{N}/query.tpp / q{N}/load.tpp.
//
// Design note: Lineitem is rekeyed from (orderkey, linenumber) to
// (custkey, orderkey, invoicekey, linenumber). The custkey is not stored
// in lineitem's PK, so populate_merged resolves it from the orders base table
// by scanning orders first and building an orderkey → custkey map, then
// replaying lineitems with the resolved custkey.

#include "backend.hpp"
#include "views_coli.hpp"
#include "tpchi_tables.hpp"

namespace tpch
{

// ---------------------------------------------------------------------------
// COLIGroupWalk — pure-dispatch walker over a COLI merged index.
//
// Iterates the merged scanner forward and dispatches each row to the
// corresponding Visitor hook.  Detects custkey group boundaries and calls
// on_group_end(prev_custkey) before advancing into the next group.
//
// Byte-lex order within a custkey group (guaranteed by the coli_domain_tag
// encoding in views_coli.hpp):
//   customer → invoice* → (orders → lineitems*)+ ...
//
// Visitor contract (all methods optional; absent methods are skipped via
// "if constexpr requires"):
//
//   bool on_customer(Integer custkey, const customer_coli_t&)
//       Return false to suppress on_invoice / on_order / on_lineitem for
//       the rest of this group.  on_group_end is still called.
//
//   void on_invoice (const invoice_coli_t::Key&,  const invoice_coli_t&)
//   void on_order   (const orders_coli_t::Key&,   const orders_coli_t&)
//   void on_lineitem(const lineitem_coli_t::Key&, const lineitem_coli_t&)
//   void on_group_end(Integer custkey)
//
// The walker is deliberately narrow: it knows only about byte layout and
// custkey transitions.  All per-query record assembly, projection, and
// aggregation logic lives in the Visitor.

template <typename Backend, typename Visitor>
void coli_group_walk(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>& mi,
    Visitor& visitor);

// A2c fused_emit variant: bypasses std::variant construction, dispatches via
// a tag-byte switch on raw (idx_tag, key_view, value_view) triples.
// Declared separately so callers can use it explicitly; the dispatcher
// coli_group_walk_dispatch selects between the two based on FLAGS_coli_walker_variant.
template <typename Backend, typename Visitor>
void coli_group_walk_fused_emit(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>& mi,
    Visitor& visitor);

// ---------------------------------------------------------------------------
// CustomerOrdersLineitemInvoicePipeline

template <typename Backend>
class CustomerOrdersLineitemInvoicePipeline
{
   // Non-owning references; lifetime managed by the per-query workload.
   typename Backend::template Adapter<customerh_t>&   customer;
   typename Backend::template Adapter<orders_t>&      orders;
   // lineitem_i_t: the FK-bearing variant, provided by TPCHIWorkload after the
   // invoice linking pass has written real invoicekeys into every row.
   typename Backend::template Adapter<lineitem_i_t>&  lineitem;
   typename Backend::template Adapter<invoice_t>&     invoice;
   typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                            lineitem_coli_t, invoice_coli_t>& merged_coli;

   // Custkey-sorted split indexes for S1 (BinaryMergeJoin on custkey).
   // Customer is already custkey-keyed via customerh_t, so no split index needed.
   // These reuse the *_coli_t tagged-key encoding; the same key_from_base
   // factories and custkey prefix apply.
   // Named with _ref suffix to avoid collision with the public accessor methods.
   typename Backend::template Adapter<orders_coli_t>&   split_orders_ref;
   typename Backend::template Adapter<lineitem_coli_t>& split_lineitem_ref;
   typename Backend::template Adapter<invoice_coli_t>&  split_invoice_ref;

   // aCOLI (aggregated COLI) 3-type MI:
   //   customer_acoli_t + orders_coli_t + lineitem_acoli_t.
   // orders_acoli_t was retired (Step 4b) — byte-identical to orders_coli_t
   // after tagged-key adoption, so collapsed into the same type.
   // Invoice rows are collapsed into pre_open_due at load time (parameter-
   // independent: i_status='O' is hardcoded by spec).  Lineitems are stored
   // unaggregated so S5 remains reusable across all DATE param sets.
   typename Backend::template MergedAdapter<customer_acoli_t, orders_coli_t,
                                            lineitem_acoli_t>& acoli_adapter_ref;

  public:
   CustomerOrdersLineitemInvoicePipeline(
       typename Backend::template Adapter<customerh_t>&   customer,
       typename Backend::template Adapter<orders_t>&      orders,
       typename Backend::template Adapter<lineitem_i_t>&  lineitem,
       typename Backend::template Adapter<invoice_t>&     invoice,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_coli_t>& split_lineitem,
       typename Backend::template Adapter<invoice_coli_t>&  split_invoice,
       typename Backend::template MergedAdapter<customer_acoli_t, orders_coli_t,
                                                lineitem_acoli_t>& acoli);

   // Dual-write replay: scans all four base tables and inserts each record
   // into merged_coli using *_coli_t tagged keys. Lineitem records are rekeyed
   // from (orderkey, linenumber) to include custkey resolved from orders.
   void populate_merged();

   // Populate three custkey-sorted split indexes for S1 (BinaryMergeJoin).
   // Mirrors populate_merged's scan order and orderkey→custkey map; each
   // *_coli_t record is inserted into the corresponding single-type split
   // adapter using the same tagged custkey-prefix key as in merged_coli.
   void populate_split();

   // Returns the estimated size of merged_coli in MiB.
   double get_merged_size() const;

   // Returns the sum of the three split adapter sizes in MiB.
   double get_split_size() const;

   // Build the aCOLI MI: two passes over base tables.
   //   Pass A: invoice scan → per-custkey open_due map (i_status='O' fused).
   //   Pass C: customer + orders + lineitem scan → insert all three record types.
   //   (Pass B — lineitem revenue map — removed 2026-05-03; revenue is computed
   //    at query time so S5 is reusable across DATE param sets.)
   void populate_aggregated();

   // Returns estimated size of the aCOLI MI in MiB.
   double get_aggregated_size() const;

   // Expose the merged adapter so per-query drivers can call coli_group_walk.
   typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                            lineitem_coli_t, invoice_coli_t>&
   merged_adapter() { return merged_coli; }

   // Expose the aCOLI 3-type MI for S5 query drivers.
   typename Backend::template MergedAdapter<customer_acoli_t, orders_coli_t,
                                            lineitem_acoli_t>&
   acoli_adapter() { return acoli_adapter_ref; }

   // Expose custkey-sorted split adapters so per-query S1 drivers can scan
   // the single-type indexes directly (parallel to merged_adapter() for S3).
   typename Backend::template Adapter<orders_coli_t>&
   split_orders() { return split_orders_ref; }

   typename Backend::template Adapter<lineitem_coli_t>&
   split_lineitem() { return split_lineitem_ref; }

   typename Backend::template Adapter<invoice_coli_t>&
   split_invoice() { return split_invoice_ref; }
};

}  // namespace tpch

#include "coli_pipeline.tpp"  // IWYU pragma: keep
