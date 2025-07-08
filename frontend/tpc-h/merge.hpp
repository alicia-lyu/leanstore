#pragma once
#include <cstddef>
#include <fstream>
#include <variant>
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

   void eager_join() { join_state.join_current(); }

   bool went_past(const JK& match_jk) const { return join_state.went_past(match_jk); }

   bool has_cached_next() const { return join_state.has_next(); }

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
   size_t right_next_cnt = 0;
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
      int cached_not_joined = 0;
      std::apply([&](auto&... records) { ((cached_not_joined += records.size()), ...); }, join_state.cached_records);
      std::filesystem::path premerged_join_log_path = std::filesystem::path(FLAGS_csv_path) / "premerged_join.csv";
      bool print_header = !std::filesystem::exists(premerged_join_log_path);
      std::ofstream premerged_join_log(premerged_join_log_path, std::ios::app);
      if (print_header) {
         premerged_join_log << "record_type_cnt,seek_cnt,scan_filter_cnt,right_next_cnt,emplace_cnt,cached_not_joined,joined,joined_not_produced\n";
      }
      premerged_join_log << sizeof...(Rs) << "," << seek_cnt << "," << scan_filter_cnt << "," << right_next_cnt << "," << emplace_cnt << ","
                         << cached_not_joined << "," << join_state.joined << "," << join_state.cached_joined_records.size() << std::endl;
      premerged_join_log.close();
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
      emplace(k, v, jk);
      return true;
   }

   void eager_join() { join_state.join_current(); }

   bool went_past(const JK& match_jk) const { return join_state.went_past(match_jk); }

   bool has_cached_next() const { return join_state.has_next(); }

   std::tuple<int, int, int> distance(const JK& to_jk)
   {
      assert(join_state.cached_jk != JK::max());
      return to_jk.first_diff(join_state.cached_jk);
   }

   template <typename R>
   bool seek_and_scan_next(const JK& seek_jk)
   {
      typename R::Key k{seek_jk};
      merged_scanner.template seek<R>(k);
      seek_cnt++;
      scan_next();
      int cmp = join_state.cached_jk.match(seek_jk);  // cached_jk is just cached in scan_next()
      assert(cmp >= 0);
      return cmp == 0;  // true if the cached record matches the seek_jk
   }

   void emplace(const K& k, const V& v, const JK& jk)
   {
      // if (current_jk != jk) join the cached records
      if (join_state.cached_jk.match(jk) != 0)
         join_state.refresh(jk);  // previous records are joined
      // add the new record to the cache
      join_state.emplace(k, v);
      emplace_cnt++;
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
            continue;           // skip this record, it is before the seek_jk
         } else if (cmp > 0) {  // no records matches seek_jk, all records are after the seek_jk
            emplace(k, v, jk);  // emplace anyway, otherwise we need to scan this record again later
            return false;
         } else {  // cmp == 0, found the right record
            emplace(k, v, jk);
            return true;
         }  // else continue scanning
      }
   }

   template <typename R, size_t I>
   bool get_next(const JK& seek_jk, bool& info_exhausted)
   {
      if (info_exhausted) {  // no more seeks needed
         return true;
      }
      JK jk_r = SKBuilder<JK>::template get<R>(seek_jk);
      info_exhausted =
          info_exhausted || jk_r == seek_jk;  // seek_jk has more info only when there is additional non-zero fields, i.e., seek_jk > jk_r
      // if info_exhausted, stop getting the next R
      // 1. seek first record at all times
      if (join_state.cached_jk == JK::max()) {
         return seek_and_scan_next<R>(seek_jk);
      }
      // 2. have sought before...
      auto [_, base, dist] = distance(jk_r);
      assert(dist >= 0);  // unexhausted info
      // 2.1 scanned and cached, or no selection at this field
      if (dist == 0) {  // can happen b/c seeks has to start from the first R, or when the last key has no selection (==0)
         return true;   // no scan or seek needed
      }
      // 2.2 right the next record
      if (base == 0 && dist == 1) {
         right_next_cnt++;
         scan_next();
         int cmp = join_state.cached_jk.match(seek_jk);  // cached_jk is just cached in scan_next()
         assert(cmp >= 0);
         return cmp == 0;  // true if the cached record matches the seek_jk
      }
      // decide between scan_filter_next() and seek
      auto last_kv_in_page = merged_scanner.last_in_page();
      // 2.3 if the last record in the page is larger than the seek_jk, we scan the page
      if (last_kv_in_page.has_value()) {
         auto [k, v] = last_kv_in_page.value();
         JK last_jk = SKBuilder<JK>::create(k, v);
         if (last_jk > jk_r) {  // required jk in page
            return scan_filter_next(seek_jk);
         } else if (last_jk == jk_r) {  // The right record is the very last record in the page
            scan_filter_cnt++;
            emplace(k, v, last_jk);
            merged_scanner.go_to_last_in_page();  // last_in_page returns to the previous cursor position
            return true;                          // no scan or seek needed
         }
      }
      // 2.4 if the last record in the page is smaller than the seek_jk, we need to seek
      return seek_and_scan_next<R>(seek_jk);  // then scan_next();
   }

   template <size_t... Is>
   bool get_next_all(const JK& seek_jk, std::index_sequence<Is...>)
   {
      bool info_exhausted = false;
      return (get_next<Rs, Is>(seek_jk, info_exhausted) && ...);
   }

   std::optional<std::pair<typename JR::Key, JR>> next(const JK& seek_jk = JK::max())
   {
      while (!join_state.has_next()) {
         if (join_state.cached_jk == JK::max() &&  // no record scanned
             seek_jk != JK::max()) {
            bool all_records_exists_for_seek_jk = get_next_all(seek_jk, std::index_sequence_for<Rs...>{});
            assert(join_state.cached_jk != JK::max());
            if (all_records_exists_for_seek_jk) {  // go to scan_next() in the next iteration, yielding a record == seek_jk
               assert(!join_state.has_next());     // previous records would only yield a record < seek_jk
            }  // else go to scan_next() in the next iteration, yielding a record > seek_jk
         } else {
            bool not_exhausted = scan_next();
            if (!not_exhausted) {
               return std::nullopt;  // only return std::nullopt if the joiner reaches the end
            }
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
         std::cout << "~Merge: produced " << (double)heap_merge.sifted / 1000 << "k records------------------------------------" << std::endl;
   }

   void printProgress()
   {
      if (heap_merge.current_jk % 10 == 0 && FLAGS_log_progress) {
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
      // assert(next_left || next_right);  // at least one source must be available
      refresh_join_state();  // initialize the join state with the smallest JK
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

   void refresh_join_state()
   {
      JK left_jk = next_left ? SKBuilder<JK>::create(next_left->first, next_left->second) : JK::max();
      JK right_jk = next_right ? SKBuilder<JK>::create(next_right->first, next_right->second) : JK::max();

      int comp = left_jk.match(right_jk);

      if (comp != 0) {  // cache the smaller one
         join_state.refresh(comp < 0 ? left_jk : right_jk);
      } else {
         if (left_jk == JK::max())
            return;
         // matched but may not be equal, cache the most specific one
         join_state.refresh(left_jk > right_jk ? left_jk : right_jk);
      }
   }

   void eager_join() { join_state.join_current(); }

   bool went_past(const JK& match_jk) const { return join_state.went_past(match_jk); }

   bool has_cached_next() const { return join_state.has_next(); }

   void next_jk()
   {
      refill_current_key();
      refresh_join_state();
   }

   void run()
   {
      join_state.logging = true;
      while (next_left || next_right) {
         next();
      }
      while (join_state.has_next()) {
         join_state.next();  // consume the remaining records
      }
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