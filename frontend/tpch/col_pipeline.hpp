#pragma once

// CUSTOMER × ORDERS × LINEITEM (COL) merged-index substrate.
//
// This pipeline owns MI(customer_coli_t, orders_coli_t, lineitem_col_t)
// keyed by Calcite-style tagged keys and exposes:
//
//   populate_merged()      — 3-pass replay over the three base adapters
//   get_merged_size()      — estimated size in MiB of merged_col
//   populate_split()       — build two custkey-sorted split indexes
//                            (orders, lineitem) for S1 BinaryMergeJoin
//   get_split_size()       — sum of the two split adapter sizes in MiB
//
// COLGroupWalk driver (col_group_walk / col_group_walk_fused_emit):
//   A pure-dispatch iterator over the merged index parameterized by a Visitor.
//   The walker calls the appropriate Visitor hook for each record and
//   on_group_end at each custkey boundary.  All query-specific assembly and
//   aggregation live in the Visitor.
//
// Relationship to COLIPipeline (coli_pipeline.hpp):
//   COL is a strict 3-table subset of COLI minus the invoice arm.
//   customer_coli_t and orders_coli_t are reused verbatim (byte-identical).
//   lineitem_col_t differs from lineitem_coli_t only in lacking the invoicekey
//   key segment.  The walker and populate logic mirror COLI with the invoice
//   pass deleted.
//
// Design note: lineitem is rekeyed from (orderkey, linenumber) to
// (custkey, orderkey, linenumber).  The custkey is not stored in lineitem's PK,
// so populate_merged resolves it via an in-memory orderkey → custkey map built
// during the orders pass.

#include "backend.hpp"
#include "views_col.hpp"
#include "tpch_tables.hpp"

namespace tpch
{

// ---------------------------------------------------------------------------
// COLGroupWalk — pure-dispatch walker over a COL merged index.
//
// Iterates the merged scanner forward and dispatches each row to the
// corresponding Visitor hook.  Detects custkey group boundaries and calls
// on_group_end(prev_custkey) before advancing into the next group.
//
// Byte-lex order within a custkey group (guaranteed by the coli_domain_tag
// encoding in views_col.hpp / views_coli.hpp):
//   customer → (orders → lineitems*)+ ...
//
// Visitor contract (all methods optional; absent methods are skipped via
// "if constexpr requires"):
//
//   bool on_customer(Integer custkey, const customer_coli_t&)
//       Return false to suppress on_order / on_lineitem for the rest of this
//       group.  on_group_end is still called.
//
//   void on_order   (const orders_coli_t::Key&,  const orders_coli_t&)
//   void on_lineitem(const lineitem_col_t::Key&, const lineitem_col_t&)
//   void on_group_end(Integer custkey)
//
// The walker is deliberately narrow: it knows only about byte layout and
// custkey transitions.  All per-query record assembly, projection, and
// aggregation logic lives in the Visitor.

template <typename Backend, typename Visitor>
void col_group_walk(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& mi,
    Visitor& visitor);

// A2c fused_emit variant: bypasses std::variant construction, dispatches via
// a tag-byte switch on raw (idx_tag, key_view, value_view) triples.
template <typename Backend, typename Visitor>
void col_group_walk_fused_emit(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& mi,
    Visitor& visitor);

// ---------------------------------------------------------------------------
// CustomerOrdersLineitemPipeline

template <typename Backend>
class CustomerOrdersLineitemPipeline
{
   // Non-owning references; lifetime managed by the per-query workload.
   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_t>&   lineitem;
   typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                            lineitem_col_t>& merged_col;

   // Custkey-sorted split indexes for S1 (BinaryMergeJoin on custkey).
   // Customer is already custkey-keyed via customerh_t, so no split index for it.
   // Named with _ref suffix to avoid collision with the public accessor methods.
   typename Backend::template Adapter<orders_coli_t>&   split_orders_ref;
   typename Backend::template Adapter<lineitem_col_t>& split_lineitem_ref;

  public:
   CustomerOrdersLineitemPipeline(
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_t>&   lineitem,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_col_t>& merged_col,
       typename Backend::template Adapter<orders_coli_t>&   split_orders,
       typename Backend::template Adapter<lineitem_col_t>& split_lineitem);

   // 3-pass replay: scans the three base tables and inserts each record into
   // merged_col using *_coli_t / lineitem_col_t tagged keys. Lineitem records
   // are rekeyed from (orderkey, linenumber) to include custkey resolved from
   // the orders pass.
   void populate_merged();

   // Populate two custkey-sorted split indexes for S1 (BinaryMergeJoin).
   // Mirrors populate_merged's scan order and orderkey→custkey map; each record
   // is inserted into the corresponding single-type split adapter using the same
   // tagged custkey-prefix key as in merged_col.
   void populate_split();

   // Returns the estimated size of merged_col in MiB.
   double get_merged_size() const;

   // Returns the sum of the two split adapter sizes in MiB.
   double get_split_size() const;

   // Expose the merged adapter so per-query drivers can call col_group_walk.
   typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                            lineitem_col_t>&
   merged_adapter() { return merged_col; }

   // Expose custkey-sorted split adapters so per-query S1 drivers can scan
   // the single-type indexes directly.
   typename Backend::template Adapter<orders_coli_t>&
   split_orders() { return split_orders_ref; }

   typename Backend::template Adapter<lineitem_col_t>&
   split_lineitem() { return split_lineitem_ref; }
};

}  // namespace tpch

#include "col_pipeline.tpp"  // IWYU pragma: keep
