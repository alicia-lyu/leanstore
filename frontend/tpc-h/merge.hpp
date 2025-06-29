#pragma once
#include <cstddef>
#include <variant>
#include "../shared/LeanStoreMergedScanner.hpp"
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
         if (entry.jk.match(join_state.cached_jk) != 0) {
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

   void run()
   {
      join_state.logging = true;
      heap_merge.run();
   }

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

   JK current_jk() const { return join_state.cached_jk; }

   long produced() const { return join_state.joined; }
};

// merged_scanner -> join_state -> yield joined records
template <template <typename...> class MergedScannerType, typename JK, typename JR, typename... Rs>
struct PremergedJoin {
   MergedScannerType<JK, JR, Rs...>& merged_scanner;
   JoinState<JK, JR, Rs...> join_state;
   size_t seek_cnt = 0;
   size_t scan_filter_cnt = 0;
   size_t emplace_cnt = 0;  // not necessarily equal to merged_scanner.produced

   using K = std::variant<typename Rs::Key...>;
   using V = std::variant<Rs...>;

   PremergedJoin(
       MergedScannerType<JK, JR, Rs...>& merged_scanner,
       std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
       : merged_scanner(merged_scanner), join_state("PremergedJoin", consume_joined)
   {
   }

   template <template <typename> class AdapterType>
   PremergedJoin(MergedScannerType<JK, JR, Rs...>& merged_scanner, AdapterType<JR>& joinedAdapter)
       : merged_scanner(merged_scanner), join_state("PremergedJoin", [&](const auto& k, const auto& v) { joinedAdapter.insert(k, v); })
   {
   }

   ~PremergedJoin()
   {
      // if (seek_cnt > 1 || scan_filter_cnt > 0)
      //    std::cout << "\r~PremergedJoin: seek count = " << seek_cnt << ", scan filter count = " << scan_filter_cnt
      //              << ", emplace count = " << emplace_cnt << ", joined = " << join_state.joined << "!";
   }

   bool scan_next()
   {
      std::optional<std::pair<K, V>> kv = merged_scanner.next();
      if (!kv) {
         return false;
      }
      auto& k = kv->first;
      auto& v = kv->second;
      JK jk;
      std::visit([&](auto& actual_key) -> void { jk = actual_key.get_jk(); }, k);
      // if (current_jk != jk) join the cached records
      if (join_state.cached_jk.match(jk) != 0)
         join_state.refresh(jk);
      // add the new record to the fcache
      join_state.emplace(k, v);
      emplace_cnt++;
      return true;
   }

   std::pair<int, int> distance(const JK& to_jk)
   {
      if (join_state.cached_jk == JK::max()) {
         assert(join_state.joined == 0);
         return to_jk.first_diff(JK());
      } else {
         return to_jk.first_diff(join_state.cached_jk);
      }
   }

   template <typename R, size_t I>
   bool get_next(const JK& seek_jk)
   {
      JK jk = SKBuilder<JK>::template get<R>(seek_jk);
      auto [base, dist] = distance(jk);
      if (dist < 0) {          // scanned and passed
         return false;         // no required record (type R)
      } else if (dist == 0) {  // scanned and cached
                               // can happen b/c seeks has to start from the first R, or when the last key has no selection (==0)
         return true;          // skip this R
      } else if (dist <= 1) {  // right next record
         assert(base == 0);
         // go to the end
      } else if (dist <= 15 &&                                                                               // HARDCODED, at most acorss 1 page
                 I == sizeof...(Rs) - 1 &&                                                                   // last R
                 std::is_same_v<MergedScannerType<JK, JR, Rs...>, LeanStoreMergedScanner<JK, JR, Rs...>>) {  // b-tree scanner
         return scan_filter_next(seek_jk);
      } else {  // across multiple pages
         typename R::Key k{seek_jk};
         merged_scanner.template seek<R>(k);
         seek_cnt++;
         // go to the end
      }
      scan_next();
      auto cmp = join_state.cached_jk.match(jk);  // cached_jk is just cached in scan_next()
      assert(cmp >= 0);
      if (cmp > 0) {
         return false;  // no more records to scan, all records are after the seek_jk
      } else {
         return true;  // found the right record, but not yet added to the fcache
      }
   }

   bool scan_filter_next(const JK& seek_jk)
   {
      // scan until we find the first record with the right jk
      scan_filter_cnt++;
      while (true) {
         std::optional<std::pair<K, V>> kv = merged_scanner.next();
         if (!kv) {
            return false;  // no more records to scan
         }
         auto& k = kv->first;
         auto& v = kv->second;
         JK jk;
         std::visit([&](auto& actual_key) -> void { jk = actual_key.get_jk(); }, k);
         int cmp = jk.match(seek_jk);
         if (cmp < 0) {
            continue;  // skip this record, it is before the seek_jk
         } else if (cmp > 0) {
            return false;  // no more records to scan, all records are after the seek_jk
         } else {          // cmp == 0, found the right record
            if (join_state.cached_jk.match(jk) != 0)
               join_state.refresh(jk);
            // add the new record to the fcache
            join_state.emplace(k, v);
            emplace_cnt++;
            return true;
         }  // else continue scanning
      }
   }

   template <size_t... Is>
   bool get_next_all(const JK& seek_jk, std::index_sequence<Is...>)
   {
      return ((get_next<Rs, Is>(seek_jk) && ...));
   }

   std::optional<std::pair<typename JR::Key, JR>> next(const JK& seek_jk = JK::max())
   {
      bool start = true;
      while (!join_state.has_next()) {
         bool ret;
         if (seek_jk != JK::max() && start) {
            ret = get_next_all(seek_jk, std::index_sequence_for<Rs...>{});
            start = false;  // if still no next, scan
         } else {
            ret = scan_next();
         }
         if (!ret) {
            return std::nullopt;  // no more records to scan
         }
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
      join_state.logging = true;
      while (true) {
         auto ret = next();
         if (!ret.has_value()) {
            break;
         }
      }
   }

   JK current_jk() const { return join_state.cached_jk; }

   long produced() const { return join_state.joined; }
};

template <typename JK, typename... Rs>
struct Merge {
   HeapMergeHelper<JK, Rs...> heap_merge;
   bool logging = false;

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

   void run()
   {
      logging = true;
      heap_merge.run();
   }

   void next_jk()
   {
      JK start_jk = heap_merge.current_jk;
      while (heap_merge.current_jk == start_jk && heap_merge.has_next()) {
         heap_merge.next();
      }
   }

   JK current_jk() const { return heap_merge.current_jk; }

   long produced() const { return heap_merge.sifted; }
};

// sources -> join_state -> yield joined records
template <typename JK, typename JR, typename R1, typename R2>
struct BinaryMergeJoin {
   JoinState<JK, JR, R1, R2> join_state;
   std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left;
   std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right;
   std::optional<std::pair<typename R1::Key, R1>> next_left;
   std::optional<std::pair<typename R2::Key, R2>> next_right;

   BinaryMergeJoin(std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left_func,
                   std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right_func)
       : join_state("BinaryMergeJoin"),
         fetch_left(std::move(fetch_left_func)),
         fetch_right(std::move(fetch_right_func)),
         next_left(fetch_left()),
         next_right(fetch_right())
   {
   }

   void refill_current_key()
   {
      auto curr_jk = join_state.cached_jk;
      while (next_left && SKBuilder<JK>::create(next_left->first, next_left->second).match(curr_jk) == 0) {
         join_state.template emplace<R1, 0>(next_left->first, next_left->second);
         next_left = fetch_left();
      }
      while (next_right && SKBuilder<JK>::create(next_right->first, next_right->second).match(curr_jk) == 0) {
         join_state.template emplace<R2, 1>(next_right->first, next_right->second);
         next_right = fetch_right();
      }
   }

   void next_jk()
   {
      refill_current_key();
      JK left_jk = next_left ? SKBuilder<JK>::create(next_left->first, next_left->second) : JK::max();
      JK right_jk = next_right ? SKBuilder<JK>::create(next_right->first, next_right->second) : JK::max();

      join_state.refresh(left_jk < right_jk ? left_jk : right_jk);
   }

   void run()
   {
      join_state.logging = true;
      while (next_left || next_right) {
         next();
      }
      join_state.refresh(JK::max());
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      while (!join_state.has_next() && (next_left || next_right)) {
         next_jk();
      }
      return join_state.next();
   }

   JK current_jk() const { return join_state.cached_jk; }
   long produced() const { return join_state.joined; }
};