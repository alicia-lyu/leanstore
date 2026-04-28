// Template method bodies for OrdersLineitemPipeline<Backend>.
// All bodies are TODO stubs. Each comment cites the section of
// frontend/tpch/q12/CLAUDE.md or the shared infrastructure header
// that the implementer should consult.

#pragma once

namespace tpch
{

template <typename Backend>
OrdersLineitemPipeline<Backend>::OrdersLineitemPipeline(
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol)
    : orders(orders), lineitem(lineitem), merged_ol(merged_ol)
{
   // TODO(skeleton): trivial — just bind references. Nothing to implement.
}

template <typename Backend>
template <typename OnJoin>
void OrdersLineitemPipeline<Backend>::scan_merged(OnJoin&& on_join)
{
   // TODO(skeleton): Instantiate PremergedJoin<MergedScanner, ol_sort_key_t,
   // joined_ol_t, orders_t, lineitem_t> over merged_ol.getScanner(), then
   // call joiner.run(on_join). See:
   //   - frontend/shared/merge-join/premerged_join.hpp (PremergedJoin::run)
   //   - frontend/tpch/q12/CLAUDE.md §Execution Style: monolithic post-join
   //   - frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 4 query
}

template <typename Backend>
template <typename OnJoin>
void OrdersLineitemPipeline<Backend>::merge_join_base(OnJoin&& on_join)
{
   // TODO(skeleton): Instantiate BinaryMergeJoin<Scanner<orders_t>,
   // Scanner<lineitem_t>, ol_sort_key_t, joined_ol_t> over base table
   // scanners, then call joiner.run(on_join). See:
   //   - frontend/shared/merge-join/binary_merge_join.hpp (BinaryMergeJoin)
   //   - frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 2 query
}

template <typename Backend>
template <typename OnJoin>
void OrdersLineitemPipeline<Backend>::hash_join_base(OnJoin&& on_join)
{
   // TODO(skeleton): Build an in-memory hash map keyed by orderkey from
   // the orders adapter, then scan lineitem and probe the map, calling
   // on_join for each match. See:
   //   - frontend/shared/merge-join/hash_join.hpp (HashJoin)
   //   - frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 1 query
}

template <typename Backend>
template <typename OnJoin>
void OrdersLineitemPipeline<Backend>::populate_pipeline_view(OnJoin&& sink)
{
   // TODO(skeleton): Drive merge_join_base(sink) so that the per-query
   // caller can insert each joined_ol_t into the pipeline view adapter.
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 3 load.
}

template <typename Backend>
void OrdersLineitemPipeline<Backend>::populate_merged_ol()
{
   // TODO(skeleton): Scan orders adapter, insert each orders_t into
   // merged_ol; scan lineitem adapter, insert each lineitem_t into
   // merged_ol. This is the dual-write replay that builds MI[0].
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Option 4 load.
}

template <typename Backend>
double OrdersLineitemPipeline<Backend>::get_merged_size() const
{
   // TODO(skeleton): Return merged_ol.size(). See:
   //   - frontend/shared/adapter-scanner/RocksDBMergedAdapter.hpp ::size()
   //   - frontend/shared/adapter-scanner/LeanStoreMergedAdapter.hpp ::size()
   return 0;
}

}  // namespace tpch
