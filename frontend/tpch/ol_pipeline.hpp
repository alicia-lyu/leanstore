#pragma once

// Shared ORDERS x LINEITEM pipeline substrate used by Q12, Q3, and Q9.
//
// All three Tier 1 queries share a single MergedIndex(orders_t, lineitem_t)
// keyed by orderkey. This class owns references to the three adapters that
// participate in that pipeline and provides the join drivers that each
// per-query workload composes with its own filter/project/aggregate callback.
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
   // Load helpers (called from per-query load() based on storage structure)
   // ------------------------------------------------------------------

   // Drives a join over base tables and calls `sink` for each joined_ol_t,
   // allowing the caller to insert into a pipeline view adapter.
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 3 load.
   template <typename OnJoin>
   void populate_pipeline_view(OnJoin&& sink);

   // Dual-write replay: scans base tables and inserts each orders_t /
   // lineitem_t record into merged_ol as well.
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 4 load.
   void populate_merged_ol();

   // Returns the estimated size of merged_ol in MiB.
   double get_merged_size() const;
};

}  // namespace tpch

#include "ol_pipeline.tpp"  // IWYU pragma: keep
