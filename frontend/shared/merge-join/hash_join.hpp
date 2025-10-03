#pragma once
#include <cassert>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <map>
#include "../view_templates.hpp"
#include "leanstore/Config.hpp"

struct HashLogger {
   inline static std::ofstream log_file;
   inline static bool is_initialized = false;

   inline static std::map<int, std::tuple<long, double, long long>> build_phase_time;

   static void init()
   {
      if (is_initialized)
         return;
      std::filesystem::path log_path = std::filesystem::path(FLAGS_csv_path) / "hash_stats.csv";
      log_file.open(log_path, std::ios::trunc);
      log_file << "log2_produced_count,build_us,hash_table_bytes\n";
      is_initialized = true;
   }

   static void log1(long produced_count, double build_us, long hash_table_bytes)
   {
      init();
      double log2_produced_count_double = std::log2(produced_count + 1);
      int log2_produced_count = std::round(log2_produced_count_double);
      if (build_phase_time.find(log2_produced_count) == build_phase_time.end()) {
         build_phase_time[log2_produced_count] = std::make_tuple(0, 0.0, 0LL);
      }
      auto& [cnt, total_us, total_ht_bytes] = build_phase_time[log2_produced_count];
      cnt++;
      total_us += build_us;
      total_ht_bytes += static_cast<long long>(hash_table_bytes);
   }

   static void flush()
   {
      std::vector<std::ostream*> out_streams = {&std::cout};
      if (log_file.is_open()) {
         out_streams.emplace_back(&log_file);
      } else {
         std::cerr << "HashLogger::flush() log_file not open!" << std::endl;
      }
      for (const auto& [log2_produced_count, v] : build_phase_time) {
         for (auto* os : out_streams) {
            auto& [cnt, total_us, total_ht_bytes] = v;
            *os << log2_produced_count << ',' << total_us / cnt << ',' << total_ht_bytes / cnt << std::endl;
         }
      }
      log_file.close();
   }
};

// sources -> join_state -> yield joined records
template <typename JK, typename JR, typename R1, typename R2>
struct HashJoin {
   const JK seek_jk;
   const std::function<void(const typename JR::Key&, const JR&)> consume_joined;

   bool enable_logging = false;
   long produced_count = 0;

   std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left;
   std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right;

   typename std::unordered_multimap<JK, std::pair<typename R1::Key, R1>, std::hash<JK>> left_hashtable;

   HashJoin(
       std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left,
       std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right,
       JK seek_jk,
       const std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})
       : seek_jk(seek_jk), consume_joined(consume_joined), fetch_left(fetch_left), fetch_right(fetch_right)
   {
      build_phase();
   }

   ~HashJoin()
   {
      if (enable_logging && FLAGS_log_progress && produced_count > 10000) {
         std::cout << "\rHashJoin produced a total of " << (double)produced_count / 1000 << "k records. Final JK " << seek_jk << "          "
                   << std::endl;
      }
      HashLogger::log1(produced_count, build_us, hash_table_bytes());
   }

   long hash_table_bytes() const { return left_hashtable.size() * (sizeof(JK) + sizeof(typename R1::Key) + sizeof(R1)); }

   double build_us = 0;

   void build_phase()
   {
      std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
      while (true) {
         auto kv = fetch_left();
         if (!kv.has_value()) {
            break;
         }
         auto& [k, v] = *kv;
         JK curr_jk = SKBuilder<JK>::create(k, v);
         if (seek_jk != JK::max() && curr_jk.match(seek_jk) != 0) {
            break;
         }
         left_hashtable.emplace(curr_jk, std::make_pair(k, v));
      }
      std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
      auto build_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      build_us = build_ns / 1000.0;
   }

   std::vector<std::pair<typename JR::Key, JR>> cached_results;

   bool probe_next()
   {
      assert(cached_results.empty());
      auto right_kv = fetch_right();
      if (!right_kv.has_value()) {
         return false;
      }
      auto& [rk, rv] = *right_kv;
      JK curr_jk = SKBuilder<JK>::create(rk, rv);
      if (seek_jk != JK::max() && curr_jk.match(seek_jk) != 0) {
         return false;
      }
      auto keys_to_match = curr_jk.matching_less_specific_keys();
      keys_to_match.emplace_back(curr_jk);
      for (const auto& lsk : keys_to_match) {
         auto range = left_hashtable.equal_range(lsk);
         for (auto it = range.first; it != range.second; ++it) {
            auto& [jk, l] = *it;
            auto& [lk, lv] = l;
            typename JR::Key joined_key = typename JR::Key{lk, rk};
            JR joined_rec = JR{lv, rv};
            cached_results.emplace_back(joined_key, joined_rec);
         }
      }
      return true;  // can probe next even if joined nothing
   }

   bool has_cached_next() const { return !cached_results.empty(); }

   void run()
   {
      enable_logging = true;
      std::optional<std::pair<typename JR::Key, JR>> ret = next();
      while (ret.has_value()) {
         ret = next();
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      if (has_cached_next()) {
         auto res = cached_results.back();
         cached_results.pop_back();
         if (enable_logging && FLAGS_log_progress && produced_count % 10000 == 0) {
            std::cout << "\rHashJoin produced " << (double)(produced_count + 1) / 1000 << "k records. Current JK " << res.first.jk << "...";
         }
         produced_count++;
         return res;
      }
      bool can_probe_next = true;
      while (can_probe_next && cached_results.empty()) {
         can_probe_next = probe_next();
      }
      if (cached_results.empty()) {  // exit condition: can_probe_next == false
         return std::nullopt;
      }
      return next();
   }
   // tricky to implement went_past
   // tricky to implement jk_to_join
   long produced() const { return produced_count; }
};