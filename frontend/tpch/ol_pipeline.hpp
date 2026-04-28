#pragma once

// Shared ORDERS x LINEITEM pipeline substrate used by Q12, Q3, and Q9.
//
// This class is one Pipeline implementation. Future pipelines (e.g.,
// OrdersCustomerPipeline, LineitemPartsuppPipeline) follow the same convention
// documented in frontend/tpch/CLAUDE.md §Pipeline Convention.
//
// A Pipeline class owns the secondary structures for one table group and
// exposes four load/size methods (populate_view, populate_merged,
// get_view_size, get_merged_size) plus query-time join drivers. Per-query
// workloads hold pipeline instances as members and dispatch to them from
// their own load() / get_size() based on FLAGS_storage_structure.
//
// The `OnJoin` callback receives a `const joined_ol_t&` representing one
// fully-assembled (order, lineitem) pair. Per-query logic sits entirely
// inside that callback — this is the "monolithic post-join" execution style
// endorsed in frontend/tpch/q12/CLAUDE.md §Execution Style.

#include "backend.hpp"
#include "views_ol.hpp"
#include "tpch_tables.hpp"

namespace tpch
{

template <typename Backend>
class OrdersLineitemPipeline
{
   // References are non-owning; lifetime is managed by the per-query workload.
   typename Backend::template Adapter<orders_t>&                        orders;
   typename Backend::template Adapter<lineitem_t>&                      lineitem;
   typename Backend::template MergedAdapter<orders_t, lineitem_t>&      merged_ol;

  public:
   OrdersLineitemPipeline(
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol);

   // ------------------------------------------------------------------
   // Query drivers
   // Each method calls `on_join` once per fully-assembled joined_ol_t.
   // ------------------------------------------------------------------

   // Structure 3 driver: runs PremergedJoin over merged_ol.
   // See: frontend/shared/merge-join/premerged_join.hpp, PremergedJoin.
   template <typename OnJoin>
   void scan_merged(OnJoin&& on_join);

   // Structure 1 driver: binary merge join over base tables sorted by orderkey.
   // See: frontend/shared/merge-join/binary_merge_join.hpp, BinaryMergeJoin.
   template <typename OnJoin>
   void merge_join_base(OnJoin&& on_join);

   // Structure 4 driver: hash join — build orders in-memory, probe with lineitem scan.
   // See: frontend/shared/merge-join/hash_join.hpp, HashJoin.
   template <typename OnJoin>
   void hash_join_base(OnJoin&& on_join);

   // ------------------------------------------------------------------
   // Pipeline Convention: load / size methods
   // See: frontend/tpch/CLAUDE.md §Pipeline Convention
   // ------------------------------------------------------------------

   // Drives a join over base tables and inserts each joined_ol_t into `view`.
   // ViewAdapter must support view.insert(joined_ol_t::Key, joined_ol_t).
   // The view row type belongs to the per-query workload; this method is
   // templated so each query can pass its own adapter type.
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 3 load.
   template <typename ViewAdapter>
   void populate_view(ViewAdapter& view);

   // Dual-write replay: scans base tables and inserts each orders_t /
   // lineitem_t record into merged_ol as well.
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 4 load.
   void populate_merged();

   // Returns the estimated size of the pipeline view adapter in MiB.
   // See: frontend/shared/adapter-scanner/ ::size() implementations.
   template <typename ViewAdapter>
   double get_view_size(ViewAdapter& view) const;

   // Returns the estimated size of merged_ol in MiB.
   double get_merged_size() const;
};

}  // namespace tpch

#include "ol_pipeline.tpp"  // IWYU pragma: keep
