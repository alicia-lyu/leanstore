#pragma once
#include <variant>
#include "../shared/MergedScanner.hpp"
#include "Exceptions.hpp"
#include "merge_helpers.hpp"
#include "view_templates.hpp"

// source -> heap -> join_state (consume) -> yield joined records
template <typename JK, typename JR, typename... Rs>
struct MergeJoin {
   HeapMergeHelper<JK, Rs...> heap_merge;
   JoinState<JK, JR, Rs...> join_state;

   template <template <typename> class ScannerType>
   MergeJoin(
       ScannerType<Rs>&... scanners,
       std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})
       : heap_merge(getHeapConsumesToBeJoined(), scanners...), join_state("MergeJoin", consume_joined)
   {
      heap_merge.init();
   }

   MergeJoin(
       std::vector<std::function<HeapEntry<JK>()>>& sources,
       std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})
       : heap_merge(sources, getHeapConsumesToBeJoined()), join_state("MergeJoin", consume_joined)
   {
      heap_merge.init();
   }

   template <template <typename> class AdapterType, template <typename> class ScannerType>
   MergeJoin(AdapterType<JR>& joinedAdapter, ScannerType<Rs>&... scanners)
       : heap_merge(getHeapConsumesToBeJoined(), scanners...),
         join_state("MergeJoin", [&](const auto& k, const auto& v) { joinedAdapter.insert(k, v); })
   {
      heap_merge.init();
   }

   template <typename R, size_t I>
   auto getHeapConsumeToBeJoined()
   {
      return [this](HeapEntry<JK>& entry) {
         // heap_merge.current_entry = entry;
         if (entry.jk != join_state.cached_jk) {
            join_state.refresh(entry.jk);
         }
         join_state.emplace<I>(typename R::Key(R::template fromBytes<typename R::Key>(entry.k)), CurrRec(R::template fromBytes<R>(entry.v)));
      };
   }

   template <size_t... Is>
   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToBeJoined(std::index_sequence<Is...>)
   {
      return {getHeapConsumeToBeJoined<Rs, Is>()...};
   }

   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToBeJoined()
   {
      return getHeapConsumesToBeJoined(std::index_sequence_for<Rs...>{});
   }

   void run() { heap_merge.run(); }

   void next_jk()
   {
      JK start_jk = join_state.cached_jk;
      while (join_state.cached_jk == start_jk && heap_merge.has_next()) {
         heap_merge.next();
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      while (!join_state.has_next() && heap_merge.has_next()) {
         heap_merge.next();
      }
      return join_state.next();
   }
};

template <typename JK, typename JR, typename... Rs>
struct PremergedJoin {
   JoinState<JK, JR, Rs...> join_state;

   MergedScanner<Rs...>& merged_scanner;

   using K = std::variant<typename Rs::Key...>;
   using V = std::variant<Rs...>;

   PremergedJoin(
       MergedScanner<Rs...>& merged_scanner,
       std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
       : merged_scanner(merged_scanner), join_state("PremergedJoin", consume_joined)
   {
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      while (!join_state.has_next()) {
         std::optional<std::pair<K, V>> kv = merged_scanner.next();
         if (!kv) {
            return std::nullopt;
         }
         auto& k = kv->first;
         auto& v = kv->second;
         JK jk;
         std::visit([&](auto& actual_key) -> void { jk = actual_key.get_jk(); }, k);
         // if (current_jk != jk) join the cached records
         if (join_state.cached_jk != jk)
            join_state.refresh(jk);
         // add the new record to the fcache
         join_state.emplate(k, v);
      }
      return join_state.next();
   }

   void next_jk()
   {
      JK start_jk = join_state.cached_jk;
      while (join_state.cached_jk == start_jk) {
         auto ret = next();
         if (!ret.has_value()) {
            break;
         }
      }
   }

   void run()
   {
      while (true) {
         auto ret = next();
         if (!ret.has_value()) {
            break;
         }
      }
   }
};

template <typename JK, typename... Rs>
struct Merge {
   HeapMergeHelper<JK, Rs...> heap_merge;

   template <typename MergedAdapterType, template <typename> class ScannerType, typename... SourceRecords>
   Merge(MergedAdapterType& mergedAdapter, ScannerType<SourceRecords>&... scanners)
       : heap_merge(getHeapConsumesToMerged<MergedAdapterType, SourceRecords...>(mergedAdapter), scanners...)
   {
      heap_merge.init();
   }

   ~Merge()
   {
      if (heap_merge.sifted > 1000)
         std::cout << "\r~Merge: produced " << (double)heap_merge.sifted / 1000 << "k records------------------------------------" << std::endl;
   }

   void printProgress()
   {
      if (heap_merge.current_jk % 10 == 0) {
         double progress = (double)heap_merge.sifted / 1000;
         std::cout << "\rMerge: " << progress << "k records------------------------------------";
      }
   }

   template <typename MergedAdapterType, typename RecordType, typename SourceRecord>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry<JK>& entry) {
         heap_merge.current_entry = entry;
         auto source_k = SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k);
         auto source_v = SourceRecord::template fromBytes<SourceRecord>(entry.v);
         mergedAdapter.insert(typename RecordType::Key(source_k, source_v), RecordType(source_v));
         printProgress();
      };
   }

   template <typename MergedAdapterType, typename... SourceRecords>
   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToMerged(MergedAdapterType& mergedAdapter)
   {
      return {getHeapConsumeToMerged<MergedAdapterType, Rs, SourceRecords>(mergedAdapter)...};
   }

   void run() { heap_merge.run(); }

   void next_jk()
   {
      JK start_jk = heap_merge.current_jk;
      while (heap_merge.current_jk == start_jk && heap_merge.has_next()) {
         heap_merge.next();
      }
   }
};

template <typename JK, typename JR, typename R1, typename R2>
struct BinaryMergeJoin {
   JoinState<JK, JR, R1, R2> join_state;
   std::optional<std::pair<typename R1::Key, R1>> next_r1 = std::nullopt;  // larger than current_jk
   std::optional<std::pair<typename R2::Key, R2>> next_r2 = std::nullopt;  // larger than current_jk

   std::function<std::optional<std::pair<typename R1::Key, R1>>()> next_left_func;
   std::function<std::optional<std::pair<typename R2::Key, R2>>()> next_right_func;

   BinaryMergeJoin(std::function<std::optional<std::pair<typename R1::Key, R1>>()> next_left_func,
                   std::function<std::optional<std::pair<typename R2::Key, R2>>()> next_right_func)
       : join_state("BinaryMergeJoin"), next_left_func(next_left_func), next_right_func(next_right_func)
   {
      next_r1 = next_left_func();
   }

   void join_update_cutoff(std::pair<typename R1::Key, R1>& r1, std::pair<typename R2::Key, R2>& r2)
   {
      // join the cached records
      auto r1_jk = SKBuilder<JK>::create(r1.first, r1.second);
      auto r2_jk = SKBuilder<JK>::create(r2.first, r2.second);
      auto match_ret = r1_jk.match(r2_jk);
      join_state.refresh(match_ret <= 0 ? r1_jk : r2_jk);

      // update cutoff
      if (match_ret < 0) {  // r1_jk < r2_jk
         next_r1 = std::nullopt;
         join_state.emplace<1>(r1.first, r1.second);
         next_r2 = r2;
      } else if (match_ret > 0) {  // r1_jk > r2_jk
         next_r2 = std::nullopt;
         join_state.emplace<2>(r2.first, r2.second);
         next_r1 = r1;
      } else {  // r1_jk == r2_jk
         join_state.emplace<1>(r1.first, r1.second);
         join_state.emplace<2>(r2.first, r2.second);

         auto current_jk = join_state.cached_jk;  // = r1_jk = r2_jk
         // exhaust left branch of current jk
         while (true) {
            auto kv = next_left_func();
            if (!kv) {
               next_r1 = std::nullopt;
               // exhaust right branch of current jk
               while (true) {
                  kv = next_right_func();
                  if (!kv) {
                     next_r2 = std::nullopt;
                     break;  // exhausted both branches
                     if (SKBuilder<JK>::create(kv->first, kv->second).match(current_jk) == 0) {
                        join_state.emplace<2>(kv->first, kv->second);
                     } else {
                        next_r2 = *kv;
                        break;
                     }
                  }
                  break;
               }
               if (SKBuilder<JK>::create(kv->first, kv->second).match(current_jk) == 0) {
                  join_state.emplace<1>(kv->first, kv->second);
               } else {
                  next_r1 = *kv;
                  break;
               }
            }
         }
      }
   }

   // fill cached records until we have seen all of the current jk, in that case, join the cached records
   void next_jk()
   {
      auto current_jk = join_state.cached_jk;
      if (next_r1.has_value()         // exhausted all records of current jk from left
          && !next_r2.has_value()) {  // not exhausted all records of current jk from right
         auto kv = next_right_func();
         if (!kv) {  // Nearing the end, last batch of joins
            next_r1 = std::nullopt;
            next_r2 = std::nullopt;
            return join_state.refresh(SKBuilder<JK>::create(next_r1->first, next_r1->second));
         } else {
            if (SKBuilder<JK>::create(kv->first, kv->second).match(current_jk) == 0) {  // not exhausted all records of current jk from right
               join_state.emplace<2>(kv->first, kv->second);
               return next_jk();
            } else {                                      // exhausted all records of current jk from right
               return join_update_cutoff(*next_r1, *kv);  // with placeholder next_r1 at first, it goes here
            }
         }
      } else if (!next_r1.has_value() && next_r2.has_value()) {
         auto kv = next_left_func();
         if (!kv) {  // last batch of joins
            next_r1 = std::nullopt;
            next_r2 = std::nullopt;
            return join_state.refresh(SKBuilder<JK>::create(next_r2->first, next_r2->second));
         } else {
            if (SKBuilder<JK>::create(kv->first, kv->second).match(current_jk) == 0) {
               join_state.emplace<1>(kv->first, kv->second);
               return next_jk();
            } else {
               return join_update_cutoff(*kv, *next_r2);
            }
         }
      } else if (!next_r1.has_value() && !next_r2.has_value()) {  // both exhausted, which only happens at the end
         return join_state.refresh(JK::max());
      } else {
         UNREACHABLE();
      }
   }

   void run()
   {
      while (true) {
         next_jk();
         if (!join_state.has_next() && !next_r1.has_value() && !next_r2.has_value()) {
            break;  // exhausted all records
         }
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      while (!join_state.has_next()) {
         if (!next_r1.has_value() && !next_r2.has_value()) {
            return std::nullopt;  // exhausted all records
         }
         next_jk();
      }
      return join_state.next();
   }
};