#pragma once
#include <gflags/gflags_declare.h>
#include <algorithm>
#include <fstream>
#include <functional>
#include <mutex>
#include <variant>
#include "join_state.hpp"

DECLARE_int32(tentative_skip_bytes);

struct PremergedJoinStats; // Forward declaration

// Centralized logger class
class PremergedJoinLogger
{
  public:
   // This is the main interface for logging stats
   static void log(const PremergedJoinStats& stats, size_t remaining_records_to_join, long produced);
   // Flushes any pending stats to the log file.
   static void flush();

  private:
   // All members are private and static to create a singleton-like logger
   PremergedJoinLogger() = default;  // Prevent instantiation
   static void init();
   static void write_row();

   inline static std::ofstream log_file;
   static PremergedJoinStats last_stats;
   inline static long long remaining_records_to_join_accumulated = 0;
   inline static long long produced_accumulated = 0;
   inline static size_t repeat_count = 0;
   inline static bool is_initialized = false;
   inline static std::mutex mtx;
};

// Forward declaration
struct PremergedJoinStats {
   // Removed static members for logging, they now live in PremergedJoinLogger
   size_t record_type_count;
   size_t seek_cnt = 0;
   size_t right_next_cnt = 0;
   size_t scan_filter_success = 0;
   size_t scan_filter_fail = 0;
   size_t emplace_cnt = 0;

   // Default constructor for static initialization
   PremergedJoinStats() : record_type_count(0) {}
   PremergedJoinStats(size_t record_type_count) : record_type_count(record_type_count) {}

   // Copy assignment operator is now correct without the const member
   PremergedJoinStats& operator=(const PremergedJoinStats& other) = default;

   bool operator==(const PremergedJoinStats& other) const
   {
      return record_type_count == other.record_type_count && seek_cnt == other.seek_cnt && right_next_cnt == other.right_next_cnt &&
             scan_filter_success == other.scan_filter_success && scan_filter_fail == other.scan_filter_fail && emplace_cnt == other.emplace_cnt;
   }

   bool operator!=(const PremergedJoinStats& other) const { return !(*this == other); }
};

// merged_scanner -> join_state -> yield joined records
template <typename MergedScannerType, typename JK, typename JR, typename... Rs>
struct PremergedJoin {
   JK seek_jk = JK::max();
   MergedScannerType& merged_scanner;
   JoinState<JK, JR, Rs...> join_state;
   PremergedJoinStats stats{sizeof...(Rs)};

   using K = std::variant<typename Rs::Key...>;
   using V = std::variant<Rs...>;

   PremergedJoin(
       MergedScannerType& merged_scanner,
       std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
       : merged_scanner(merged_scanner), join_state("PremergedJoin", consume_joined)
   {
   }

   template <template <typename> class AdapterType>
   PremergedJoin(MergedScannerType& merged_scanner, AdapterType<JR>& joinedAdapter)
       : merged_scanner(merged_scanner), join_state("PremergedJoin", [&](const auto& k, const auto& v) { joinedAdapter.insert(k, v); })
   {
   }

   ~PremergedJoin() { PremergedJoinLogger::log(stats, join_state.get_remaining_records_to_join(), join_state.get_produced()); }

   void replace_sk(const JK& new_sk) { seek_jk = new_sk; }

   bool has_cached_next() const { return join_state.has_next(); }

   void emplace(const K& k, const V& v, const JK& jk)
   {
      // if (current_jk != jk) join the cached records
      if (join_state.jk_to_join.match(jk) != 0)
         join_state.refresh(jk);  // previous records are joined
      // add the new record to the cache
      join_state.emplace(k, v);
      stats.emplace_cnt++;
   }

   std::optional<std::tuple<K, V, JK>> scan_next(bool to_emplace = true)
   {
      std::optional<std::pair<K, V>> kv = merged_scanner.next();
      if (!kv) {
         return std::nullopt;
      }
      auto& k = kv->first;
      auto& v = kv->second;
      JK jk;
      std::visit([&](auto& actual_key) -> void { jk = actual_key.get_jk(); }, k);
      if (seek_jk != JK::max() && jk.match(seek_jk) != 0) {
         return std::nullopt;  // past the seek_jk
      }
      if (to_emplace)
         emplace(k, v, jk);
      return std::make_tuple(k, v, jk);
   }

   std::tuple<int, int, int> distance(const JK& to_jk)
   {
      assert(join_state.jk_to_join != JK::max());
      return to_jk.first_diff(join_state.jk_to_join);
   }

   template <typename R>
   bool seek_next(const JK& to_jk)
   {
      typename R::Key k{to_jk};
      merged_scanner.template seek<R>(k);
      stats.seek_cnt++;
      auto t = scan_next();
      if (!t.has_value()) {
         return false;  // exhausted scanner
      }
      auto [scanned_k, scanned_v, scanned_jk] = t.value();
      int cmp = scanned_jk.match(to_jk);
      assert(cmp >= 0);
      return cmp == 0;  // true if the cached record matches the seek_jk
   }

   template <typename R>
   bool scan_filter_next(const JK& to_jk, bool tentative = false, int tentative_skip_bytes = FLAGS_tentative_skip_bytes)
   {
      // scan until we find the first record with the right jk
      int bytes_scanned = 0;
      while (!tentative || bytes_scanned < tentative_skip_bytes) {  // HARDCODED page size, scan 2 pages (2 next page calls)
         auto t = scan_next(false);                                 // decide later whether to emplace
         if (!t.has_value()) {
            return false;  // exhausted scanner
         }
         auto [k, v, jk] = t.value();
         bytes_scanned += sizeof(k) + sizeof(v);
         int cmp = jk.match(to_jk);
         if (cmp < 0) {
            continue;  // skip this record, it is before the seek_jk
         }
         if (!tentative) {                                                  // not tentative, should always find in this page
            assert(bytes_scanned <= std::max(tentative_skip_bytes, 4096));  // HARDCODED page size
         }
         stats.scan_filter_success++;
         emplace(k, v, jk);  // emplace the record, it is either the right one or the first one after the seek_jk, which will be needed later
         return cmp == 0;
      }
      stats.scan_filter_fail++;
      // tentative scan did not return
      return seek_next<R>(to_jk);  // seek the first record with the right jk
   }

   template <typename R>
   bool right_next(const JK& to_jk)
   {
      stats.right_next_cnt++;
      auto t = scan_next();
      if (!t.has_value()) {
         return false;  // exhausted scanner
      }
      auto [scanned_k, scanned_v, scanned_jk] = t.value();
      int cmp = scanned_jk.match(to_jk);
      assert(cmp >= 0);
      return cmp == 0;  // true if the cached record matches the seek_jk
   }

   template <typename R, size_t I>
   bool get_next(const JK& full_jk, bool& info_exhausted)
   {
      // CHECK IF INFORMATION IS EXHAUSTED IN FULL_JK
      if (info_exhausted) {  // no more seeks needed
         return true;
      }
      JK to_jk_r = SKBuilder<JK>::template get<R>(full_jk);
      info_exhausted =
          info_exhausted || to_jk_r == full_jk;  // seek_jk has more info only when there is additional non-zero fields, i.e., seek_jk > jk_r
      // if info_exhausted, stop getting the next R

      // SEEK FIRST RECORD AT ALL TIMES
      if (join_state.jk_to_join == JK::max()) {
         return seek_next<R>(to_jk_r);
      }
      // No tentative skipping
      if (FLAGS_tentative_skip_bytes == 0) {
         return seek_next<R>(to_jk_r);  // no skipping, always seek
      }
      auto [_, base, dist] = distance(to_jk_r);
      assert(dist >= 0);  // unexhausted info
      // 1 scanned and cached, or no selection at this field
      if (dist == 0) {  // can happen b/c seeks has to start from the first R, or when the last key has no selection (==0)
         return true;   // no scan or seek needed
      }
      // 2 right the next record
      if (base == 0 && dist == 1) {
         return right_next<R>(to_jk_r);
      }
      // 3 For downstream record types, tentatively scan
      if (I >= sizeof...(Rs) - 2) {  // HARDCODED: the last 2 record types, county & city & customer2
         auto last_kv_in_page = merged_scanner.last_in_page();
         int bytes_advanced = 0;
         if (last_kv_in_page.has_value()) {
            auto [last_k, last_v] = last_kv_in_page.value();
            JK last_jk = SKBuilder<JK>::create(last_k, last_v);
            int cmp = last_jk.match(to_jk_r);
            if (cmp < 0) {  // last key is before the seek_jk, required jk not in this page
               bytes_advanced = merged_scanner.go_to_last_in_page();
               // do tentative skipping below
            } else if (cmp == 0) {  // last key is the seek_jk, found it
               emplace(last_k, last_v, last_jk);
               merged_scanner.go_to_last_in_page();
               return true;                                 // no scan or seek needed
            } else {                                        // cmp > 0, last key is after the seek_jk, scan filter in this page
               return scan_filter_next<R>(to_jk_r, false);  // not tentative scanning
            }
         }  // do tentative skipping below
         return scan_filter_next<R>(to_jk_r, true, FLAGS_tentative_skip_bytes - bytes_advanced);  // tentatively scan 2 pages. Seek if not found.
      } else {                                                                                    // 4 seek directly
         return seek_next<R>(to_jk_r);
      }
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
         if (all_records_exists_for_seek_jk) {  // go to scan_next() in the while loop, yielding a record == seek_jk
            assert(!join_state.has_next());     // previous records would only yield a record < seek_jk
         }  // else go to scan_next() in the while loop, yielding a record > seek_jk
      }

      while (!join_state.has_next()) {
         auto t = scan_next();
         if (!t.has_value()) {
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