#pragma once
#include <filesystem>
#include <fstream>
#include "join_state.hpp"

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
      std::filesystem::path premerged_join_log_path = std::filesystem::path(FLAGS_csv_path) / "premerged_join.csv";
      bool print_header = !std::filesystem::exists(premerged_join_log_path);
      std::ofstream premerged_join_log(premerged_join_log_path, std::ios::app);
      if (print_header) {
         premerged_join_log << "record_type_cnt,seek_cnt,scan_filter_cnt,right_next_cnt,emplace_cnt,remaining_records_to_join(,produced\n";
      }
      premerged_join_log << sizeof...(Rs) << "," << seek_cnt << "," << scan_filter_cnt << "," << right_next_cnt << "," << emplace_cnt << ","
                         << join_state.get_remaining_records_to_join() << "," << join_state.get_produced() << std::endl;
      premerged_join_log.close();
   }

   void eager_join() { join_state.eager_join(); }

   bool went_past(const JK& match_jk) const { return join_state.went_past(match_jk); }

   bool has_cached_next() const { return join_state.has_next(); }

   void emplace(const K& k, const V& v, const JK& jk)
   {
      // if (current_jk != jk) join the cached records
      if (join_state.jk_to_join.match(jk) != 0)
         join_state.refresh(jk);  // previous records are joined
      // add the new record to the cache
      join_state.emplace(k, v);
      emplace_cnt++;
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

   std::tuple<int, int, int> distance(const JK& to_jk)
   {
      assert(join_state.jk_to_join != JK::max());
      return to_jk.first_diff(join_state.jk_to_join);
   }

   template <typename R>
   bool seek_next(const JK& seek_jk)
   {
      typename R::Key k{seek_jk};
      merged_scanner.template seek<R>(k);
      seek_cnt++;
      scan_next();
      int cmp = join_state.jk_to_join.match(seek_jk);  // cached_jk is just cached in scan_next()
      assert(cmp >= 0);
      return cmp == 0;  // true if the cached record matches the seek_jk
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
         }
         emplace(k, v, jk);  // emplace the record, it is either the right one or the first one after the seek_jk, which will be needed later
         return cmp == 0;
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
      if (join_state.jk_to_join == JK::max()) {
         return seek_next<R>(seek_jk);
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
         int cmp = join_state.jk_to_join.match(seek_jk);  // cached_jk is just cached in scan_next()
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
      return seek_next<R>(seek_jk);  // then scan_next();
   }

   template <size_t... Is>
   bool get_next_all(const JK& seek_jk, std::index_sequence<Is...>)
   {
      bool info_exhausted = false;
      return (get_next<Rs, Is>(seek_jk, info_exhausted) && ...);
   }

   std::optional<std::pair<typename JR::Key, JR>> next(const JK& seek_jk = JK::max())
   {
      if (join_state.jk_to_join == JK::max() &&  // no record scanned
          seek_jk != JK::max()) {                // seek required
         bool all_records_exists_for_seek_jk = get_next_all(seek_jk, std::index_sequence_for<Rs...>{});
         assert(join_state.jk_to_join != JK::max());
         if (all_records_exists_for_seek_jk) {  // go to scan_next() in the next iteration, yielding a record == seek_jk
            assert(!join_state.has_next());     // previous records would only yield a record < seek_jk
         }  // else go to scan_next() in the next iteration, yielding a record > seek_jk
      } else if (join_state.jk_to_join != JK::max() && seek_jk != JK::max()) {
         std::cerr << "PremergedJoin::next() call with meaningful seek_jk should only happen when join_state.jk_to_join == JK::max(), i.e., "
                      "immediately after PremergedJoin construction."
                   << std::endl;
      }

      while (!join_state.has_next()) {
         bool not_exhausted = scan_next();
         if (!not_exhausted) {
            return std::nullopt;  // only return std::nullopt if the joiner reaches the end
         }
      }
      return join_state.next();
   }

   void run()
   {
      join_state.enable_logging();
      while (true) {
         auto ret = next();
         if (!ret.has_value()) {
            break;
         }
      }
   }

   JK jk_to_join() const { return join_state.jk_to_join; }

   long produced() const { return join_state.get_produced(); }
};