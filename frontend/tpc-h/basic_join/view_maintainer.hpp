#pragma once

#include <algorithm>
#include <array>
#include <functional>
#include <tuple>
#include <vector>
#include "../merge.hpp"
#include "../tables.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace basic_join
{

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
class ViewMaintainer
{
  public:
   using WorkloadType = TPCHWorkload<AdapterType>;
   using LineAdapter = AdapterType<sorted_lineitem_t>;
   using ViewAdapter = AdapterType<joinedPPsL_t>;
   using JoinKey = join_key_t;
   using MaintainTemplateTypes = std::function<void(std::function<void(const orders_t::Key&, const orders_t&)>,
                                                    std::function<void(const lineitem_t::Key&, const lineitem_t&)>,
                                                    std::function<void(const part_t::Key&, const part_t&)>,
                                                    std::function<void(const partsupp_t::Key&, const partsupp_t&)>)>;

   ViewMaintainer(MaintainTemplateTypes maintainTemplate, WorkloadType& workload, LineAdapter& sorted_lineitem, ViewAdapter& view)
       : maintainTemplate_(maintainTemplate),
         workload_(workload),
         sorted_lineitem_(sorted_lineitem),
         view_(view),
         scanner_part_(std::move(workload_.part.getScanner())),
         scanner_partsupp_(std::move(workload_.partsupp.getScanner())),
         scanner_sorted_lineitem_(std::move(sorted_lineitem_.getScanner()))
   {
   }

   // Public API: perform view maintenance
   void run()
   {
      collect_deltas();
      sort_deltas();

      // 1. join only deltas
      run_joins({0, 1, 2}, {false, false, false});
      // 2. join two deltas + one base
      run_joins({0, 1, 2}, {false, false, true});  // YES part as base: a part-supplier pair not seen before can use an existing part rather than a
                                                   // new one; new lineitems can use a new part-supplier pair that uses an existing part
      // run_joins({0, 1, 2}, {false, true, false}); // NO partsupp as base: a part not seen before cannot be found in the previous state of the
      // partsupp table run_joins({0, 1, 2}, {true, false, false}); // NO lineitem as base: part & part-supplier pair not seen before cannot appear in
      // the previous state of the lineitem table
      // 3. join one delta + two bases
      run_joins({0, 1, 2}, {false, true, true});  // NO part as delta: new parts cannot appear in the previous state of the partsupp table
      // run_joins({0, 1, 2}, {true, false, true}); // NO partsupp as delta: new part-supplier pair cannot appear in the previous state of the
      // lineitem table run_joins({0, 1, 2}, {true, true, false}); // YES lineitem as delta: new lineitems can use existing part-supplier pairs

      for (auto const& [k, v] : delta_joinedPPsL_) {
         view_.insert(k, v);
      }
   }

  private:
   MaintainTemplateTypes maintainTemplate_;
   WorkloadType& workload_;
   LineAdapter& sorted_lineitem_;
   ViewAdapter& view_;
   std::unique_ptr<ScannerType<part_t>> scanner_part_;
   std::unique_ptr<ScannerType<partsupp_t>> scanner_partsupp_;
   std::unique_ptr<ScannerType<sorted_lineitem_t>> scanner_sorted_lineitem_;

   using PartEntry = std::tuple<part_t::Key, part_t>;
   using SuppEntry = std::tuple<partsupp_t::Key, partsupp_t>;
   using LineEntry = std::tuple<sorted_lineitem_t::Key, sorted_lineitem_t>;
   using JoinedEntry = std::tuple<joinedPPsL_t::Key, joinedPPsL_t>;

   std::vector<PartEntry> delta_parts_;
   std::vector<SuppEntry> delta_partsupps_;
   std::vector<LineEntry> delta_lineitems_;

   std::vector<JoinedEntry> delta_joinedPPsL_;

   part_t::Key start_partkey_;

   void collect_deltas()
   {
      delta_parts_.clear();
      delta_partsupps_.clear();
      delta_lineitems_.clear();

      maintainTemplate_(
          // insert orders
          [this](auto const& k, auto const& v) { workload_.orders.insert(k, v); },
          // insert lineitem + view-side delta
          [this](auto const& k, auto const& v) {
             workload_.lineitem.insert(k, v);
             auto view_key = sorted_lineitem_t::Key{k, v};
             auto view_val = sorted_lineitem_t{v};
             sorted_lineitem_.insert(view_key, view_val);
             delta_lineitems_.emplace_back(view_key, view_val);
          },
          // insert part + delta
          [this](auto const& k, auto const& v) {
             workload_.part.insert(k, v);
             delta_parts_.emplace_back(k, v);
          },
          // insert partsupp + delta
          [this](auto const& k, auto const& v) {
             workload_.partsupp.insert(k, v);
             delta_partsupps_.emplace_back(k, v);
          });
   }

   void sort_deltas()
   {
      auto cmp = [](auto const& a, auto const& b) {
         return SKBuilder<JoinKey>::create(std::get<0>(a), std::get<1>(a)) < SKBuilder<JoinKey>::create(std::get<0>(b), std::get<1>(b));
      };
      std::sort(delta_parts_.begin(), delta_parts_.end(), cmp);
      std::sort(delta_partsupps_.begin(), delta_partsupps_.end(), cmp);
      std::sort(delta_lineitems_.begin(), delta_lineitems_.end(), cmp);

      start_partkey_ = std::get<0>(delta_parts_.front());  // all deltas are about the same partkey
   }

   void run_joins(std::array<int, 3> tables, std::array<bool, 3> base)
   {
      std::function<void(const joinedPPsL_t::Key&, const joinedPPsL_t&)> consume_joined = [this](const joinedPPsL_t::Key& k, const joinedPPsL_t& v) {
         delta_joinedPPsL_.emplace_back(k, v);
      };
      std::vector<std::function<HeapEntry<JoinKey>()>> sources;
      for (int idx : tables) {
         if (base[idx]) {
            sources.push_back(make_base_source(idx));
         } else {
            sources.push_back(make_delta_source(idx));
         }
      }
      MergeJoin<JoinKey, joinedPPsL_t, part_t, partsupp_t, sorted_lineitem_t> merger(consume_joined, sources);
      merger.run();
   }

   // Delta source generator
   std::function<HeapEntry<JoinKey>()> make_delta_source(int table_id)
   {
      switch (table_id) {
         case 0: {
            auto it = delta_parts_.begin();
            return [this, it]() mutable {
               if (it == delta_parts_.end())
                  return HeapEntry<JoinKey>();
               auto [k, v] = *it++;
               auto jk = SKBuilder<JoinKey>::create(k, v);
               return HeapEntry<JoinKey>(jk, part_t::toBytes(k), part_t::toBytes(v), 0);
            };
         }
         case 1: {
            auto it = delta_partsupps_.begin();
            return [this, it]() mutable {
               if (it == delta_partsupps_.end())
                  return HeapEntry<JoinKey>();
               auto [k, v] = *it++;
               auto jk = SKBuilder<JoinKey>::create(k, v);
               return HeapEntry<JoinKey>(jk, partsupp_t::toBytes(k), partsupp_t::toBytes(v), 1);
            };
         }
         default: {
            auto it = delta_lineitems_.begin();
            return [this, it]() mutable {
               if (it == delta_lineitems_.end())
                  return HeapEntry<JoinKey>();
               auto [k, v] = *it++;
               auto jk = SKBuilder<JoinKey>::create(k, v);
               return HeapEntry<JoinKey>(jk, sorted_lineitem_t::toBytes(k), sorted_lineitem_t::toBytes(v), 2);
            };
         }
      }
   }

   // Base source generator
   std::function<HeapEntry<JoinKey>()> make_base_source(int table_id)
   {
      switch (table_id) {
         case 0: {
            scanner_part_->seek(start_partkey_);
            return [this]() mutable {
               if (auto opt = scanner_part_->next()) {
                  auto const& [k, v] = *opt;
                  auto jk = SKBuilder<JoinKey>::create(k, v);
                  return HeapEntry<JoinKey>(jk, part_t::toBytes(k), part_t::toBytes(v), 0);
               }
               return HeapEntry<JoinKey>();
            };
         }
         case 1: {
            scanner_partsupp_->seek(partsupp_t::Key{start_partkey_.p_partkey, 0});
            return [this]() mutable {
               if (auto opt = scanner_partsupp_->next()) {
                  auto const& [k, v] = *opt;
                  auto jk = SKBuilder<JoinKey>::create(k, v);
                  return HeapEntry<JoinKey>(jk, partsupp_t::toBytes(k), partsupp_t::toBytes(v), 1);
               }
               return HeapEntry<JoinKey>();
            };
         }
         default: {
            scanner_sorted_lineitem_->seek(sorted_lineitem_t::Key{join_key_t{start_partkey_.p_partkey, 0}, lineitem_t::Key{}});
            return [this]() mutable {
               if (auto opt = scanner_sorted_lineitem_->next()) {
                  auto const& [k, v] = *opt;
                  auto jk = SKBuilder<JoinKey>::create(k, v);
                  return HeapEntry<JoinKey>(jk, sorted_lineitem_t::toBytes(k), sorted_lineitem_t::toBytes(v), 2);
               }
               return HeapEntry<JoinKey>();
            };
         }
      }
   }
};
}  // namespace basic_join
