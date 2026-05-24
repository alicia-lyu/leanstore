#pragma once

// TPC-H §5.1.2 refresh-function (RF1 + RF2) generator.
//
// RF1 ("New Sales")  — insert new orders + their lineitems ABOVE the loaded
//                       keyspace. Mints orderkeys via orderkey_from_index so
//                       the §4.2.3 sparse-orderkey invariant is preserved.
// RF2 ("Old Sales")  — delete PRE-EXISTING orders + their lineitems from the
//                       BOTTOM of the loaded keyspace, walked via
//                       orderkey_from_index from index 1.
//
// The two cursors are disjoint: RF2 does NOT delete what RF1 inserts. The
// reservoir lives outside the database (two Integer cursors), faithful to the
// TPC-H spec's "exhaustible external delete stream" model. See
// `~/.claude/.../memory/feedback_tpch_rf1_rf2_semantics.md`.
//
// The generator is workload-agnostic. Per-structure maintenance (S1 split,
// S3 merged, S2 view, S4 base) lives in:
//   - `col_pipeline.{hpp,tpp}`  — single-record insert/erase helpers
//   - per-query workloads        — `maintain_rf1` / `erase_rf2` dispatchers

#include <cassert>
#include <gflags/gflags.h>
#include <vector>

#include "../tpch_tables.hpp"
#include "../tpch_workload.hpp"

DECLARE_int32(tpch_scale_factor);

namespace tpch
{

template <template <typename> class AdapterType, class LineitemRecord = lineitem_t>
class RefreshState
{
   TPCHWorkload<AdapterType, LineitemRecord>& tpch;

   // Index counters operate in the loader's sequential index space; each is
   // round-tripped through orderkey_from_index to produce a sparse orderkey.
   //
   // rf1_next_index starts at (loaded count + 1) so RF1 keys land in fresh
   // sparse slots beyond the loaded range.
   // rf2_next_index starts at 1 so RF2 walks the loaded range from the bottom.
   Integer rf1_next_index;
   Integer rf2_next_index;
   Integer loaded_index_max;

  public:
   long rf1_count = 0;
   long rf2_count = 0;

   // Construct after tpch.recover_last_ids() (or tpch.load()) has populated
   // last_*_id. Uses ORDERS_SCALE * tpch_scale_factor as the loader's index
   // upper bound (matches loadOrders' end-index).
   explicit RefreshState(TPCHWorkload<AdapterType, LineitemRecord>& w)
       : tpch(w),
         rf1_next_index(TPCHWorkload<AdapterType, LineitemRecord>::ORDERS_SCALE
                        * FLAGS_tpch_scale_factor + 1),
         rf2_next_index(1),
         loaded_index_max(TPCHWorkload<AdapterType, LineitemRecord>::ORDERS_SCALE
                          * FLAGS_tpch_scale_factor)
   {
      assert(tpch.last_customer_id > 0
             && "RefreshState constructed before recover_last_ids/load");
   }

   struct Rf1Output {
      typename orders_t::Key            key;
      orders_t                          order;
      std::vector<lineitem_t>           lines;
   };

   // Mint one fresh order + K∈[1..7] lineitems with valid partsupp pairs.
   // Picks each partkey urand, then scans partsupp for a valid supplier in
   // the same shape as load_lineitems_1order (tpch_workload.hpp:382).
   Rf1Output next_rf1()
   {
      Rf1Output out;
      Integer orderkey   = tpch.orderkey_from_index(rf1_next_index++);
      Integer custkey    = randutils::urand(1, tpch.last_customer_id);
      Timestamp orderdate = Timestamp(randutils::urand(TPCH_STARTDATE,
                                                       TPCH_ORDERS_ENDDATE));
      tpch.order_dates[orderkey] = orderdate;  // must precede lineitem gen

      Integer lineitem_cnt = randutils::urand(1, 7);
      out.lines.reserve(lineitem_cnt);
      for (Integer j = 1; j <= lineitem_cnt; j++) {
         auto p = randutils::urand(1, tpch.last_part_id);
         auto s = randutils::urand(1, tpch.last_supplier_id);
         auto start_key = partsupp_t::Key{p, s};
         bool found = false;
         tpch.partsupp.scan(
             start_key,
             [&](const partsupp_t::Key& k, const partsupp_t&) {
                p = k.ps_partkey;
                s = k.ps_suppkey;
                found = true;
                return false;
             },
             []() {});
         if (!found) {
            tpch.partsupp.scanDesc(
                start_key,
                [&](const partsupp_t::Key& k, const partsupp_t&) {
                   p = k.ps_partkey;
                   s = k.ps_suppkey;
                   found = true;
                   return false;
                },
                []() {});
         }
         assert(found && "partsupp empty — cannot pick valid (p, s) for RF1");
         lineitem_t l = lineitem_t::generateRandomRecord(
             p, s, orderdate, part_t::computeRetailPrice(p));
         tpch.accumulate_for_order(orderkey, l);
         out.lines.push_back(l);
      }

      auto custkey_gen = [custkey]() { return custkey; };
      out.order = orders_t::generateRandomRecord(custkey_gen, orderdate,
                                                  Varchar<1>("F"), Numeric(0));
      // Overwrite the derived fields with the values accumulated above.
      auto& agg = tpch.order_aggregates[orderkey];
      out.order.o_custkey     = custkey;
      out.order.o_totalprice  = agg.totalprice;
      out.order.o_orderstatus = (agg.ostatus_count == agg.line_count)
                                    ? Varchar<1>("O")
                                    : (agg.ostatus_count == 0 ? Varchar<1>("F")
                                                              : Varchar<1>("P"));
      out.key = orders_t::Key{orderkey};
      ++rf1_count;
      return out;
   }

   struct Rf2Output {
      typename orders_t::Key            key;
      Integer                           custkey;
      std::vector<Integer>              linenumbers;
      bool                              exhausted;
   };

   // Walk the next pre-existing orderkey. Reads orders[K] to resolve custkey
   // (needed for tagged secondary keys); scans lineitem[{K, 1..}] to enumerate
   // the linenumbers to delete.
   Rf2Output next_rf2()
   {
      Rf2Output out{};
      if (rf2_next_index > loaded_index_max) {
         out.exhausted = true;
         return out;
      }
      Integer orderkey = tpch.orderkey_from_index(rf2_next_index++);
      out.key = orders_t::Key{orderkey};
      // Resolve custkey from the live orders row. If the order was already
      // deleted (cursor wrapped), skip silently — callers can re-call.
      bool order_found = false;
      tpch.orders.lookup1(out.key, [&](const orders_t& o) {
         out.custkey  = o.o_custkey;
         order_found  = true;
      });
      if (!order_found) {
         out.exhausted = false;  // hole; let caller advance
         return out;
      }
      // Enumerate linenumbers for this orderkey via prefix scan.
      typename LineitemRecord::Key start{orderkey, 1};
      tpch.lineitem.scan(
          start,
          [&](const typename LineitemRecord::Key& k, const LineitemRecord&) {
             if (k.l_orderkey != orderkey) return false;
             out.linenumbers.push_back(k.l_linenumber);
             return true;
          },
          []() {});
      ++rf2_count;
      return out;
   }

   Integer next_rf1_index() const { return rf1_next_index; }
   Integer next_rf2_index() const { return rf2_next_index; }
   bool    rf2_exhausted()  const { return rf2_next_index > loaded_index_max; }
};

}  // namespace tpch
