#pragma once
#include "heap.hpp"
#include "join_state.hpp"

// source -> heap -> join_state (consume) -> yield joined records
template <typename JK, typename JR, typename... Rs>
struct MergeJoin {
   Heap<JK, Rs...> heap_merge;
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
         if (entry.jk.match(join_state.jk_to_join) != 0) {
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

   void eager_join() { join_state.eager_join(); }

   bool went_past(const JK& match_jk) const { return join_state.went_past(match_jk); }

   bool has_cached_next() const { return join_state.has_next(); }

   void run()
   {
      join_state.enable_logging();
      heap_merge.run();
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      while (!join_state.has_next() && heap_merge.has_next()) {
         heap_merge.next();
      }
      return join_state.next();
   }

   JK jk_to_join() const { return join_state.jk_to_join; }

   long produced() const { return join_state.get_produced(); }
};