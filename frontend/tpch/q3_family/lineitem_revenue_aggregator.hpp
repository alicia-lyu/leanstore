#pragma once

// LineitemRevenueAggregator: scanner-wrapper aggregator (OPERATORS.md §3 op 6).
//
// Drives a custkey-sorted split-lineitem adapter and emits one
// (lineitem_agg_t::Key, lineitem_agg_t) per (custkey, orderkey) group.
// l_shipdate filter is fused at consume time via LineitemRevenueAccumulator.
//
// Templated on:
//   • Backend       — the storage backend traits struct.
//   • LineitemType  — the split-lineitem record type.  Must expose
//                     `LineitemType::Key` aggregate-initialisable from `{}`
//                     and carrying at least a `custkey` field.  Used by
//                     Q3I (lineitem_coli_t) and Q3 (lineitem_col_t).
//   • Params        — per-query Params struct with a `shipdate` field.
//
// Hoisted Phase 4 §7.2 from q3i/query.tpp so Q3 and Q3I share one
// implementation (OPERATORS.md §6.1 comparison-integrity).

#include <optional>
#include <utility>

#include "accumulators.hpp"
#include "lineitem_agg.hpp"

namespace tpch::q3_family
{

template <typename Backend, typename LineitemType, typename Params>
class LineitemRevenueAggregator
{
   using Scanner = decltype(std::declval<typename Backend::template Adapter<LineitemType>>().getScanner());
   Scanner scanner_;
   const Params& params_;

   std::optional<std::pair<lineitem_agg_t::Key, lineitem_agg_t>> pending_;

   Integer cur_custkey_  = -1;
   Integer cur_orderkey_ = -1;
   LineitemRevenueAccumulator<Params> acc_;

   std::optional<std::pair<typename LineitemType::Key, LineitemType>> lookahead_;
   bool exhausted_ = false;

   void flush_group()
   {
      if (cur_custkey_ < 0) return;
      // Emit only groups with non-zero revenue (no revenue means all
      // lineitems were filtered out; suppress so downstream BMJ skips
      // the orderkey).
      if (acc_.revenue > Numeric(0)) {
         pending_ = {lineitem_agg_t::Key{cur_custkey_, cur_orderkey_},
                     lineitem_agg_t{acc_.revenue}};
      }
      acc_.reset();
   }

  public:
   explicit LineitemRevenueAggregator(
       typename Backend::template Adapter<LineitemType>& adapter,
       const Params& params)
       : scanner_(adapter.getScanner()), params_(params)
   {
      lookahead_ = scanner_->next();
      if (!lookahead_) exhausted_ = true;
   }

   // Forward-only seek to the first lineitem with custkey >= K.  Drops any
   // partial-group accumulation for custkeys < K.  Used only by paths that
   // gate on Backend::USE_PHYSICAL_SEEK_SKIP / FLAGS_use_seek_skip.
   bool seek_to_custkey(Integer K)
   {
      if (cur_custkey_ >= K) return false;
      if (lookahead_ && lookahead_->first.custkey >= K) return false;
      pending_ = std::nullopt;
      acc_.reset();
      cur_custkey_  = -1;
      cur_orderkey_ = -1;
      typename LineitemType::Key seek_key{};
      seek_key.custkey = K;
      scanner_->seek(seek_key);
      lookahead_ = scanner_->next();
      if (!lookahead_) exhausted_ = true;
      return true;
   }

   std::optional<std::pair<lineitem_agg_t::Key, lineitem_agg_t>> next()
   {
      if (pending_) {
         auto out = std::move(pending_);
         pending_ = std::nullopt;
         return out;
      }
      if (exhausted_ && cur_custkey_ < 0) return std::nullopt;

      for (;;) {
         if (lookahead_) {
            auto& [k, v] = *lookahead_;
            bool same_group = (k.custkey == cur_custkey_ && k.orderkey == cur_orderkey_);
            if (!same_group) {
               bool had_group = (cur_custkey_ >= 0);
               flush_group();
               cur_custkey_  = k.custkey;
               cur_orderkey_ = k.orderkey;
               acc_.consume(v, params_);
               lookahead_ = scanner_->next();
               if (!lookahead_) exhausted_ = true;
               if (had_group && pending_) {
                  auto out = std::move(pending_);
                  pending_ = std::nullopt;
                  return out;
               }
               continue;
            }
            acc_.consume(v, params_);
            lookahead_ = scanner_->next();
            if (!lookahead_) exhausted_ = true;
         } else {
            if (cur_custkey_ < 0) return std::nullopt;
            flush_group();
            cur_custkey_ = -1;
            if (pending_) {
               auto out = std::move(pending_);
               pending_ = std::nullopt;
               return out;
            }
            return std::nullopt;
         }
      }
   }
};

}  // namespace tpch::q3_family
