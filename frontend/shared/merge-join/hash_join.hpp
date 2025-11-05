#pragma once
#include <cassert>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include "../view_templates.hpp"
#include "join_state.hpp"
#include "leanstore/Config.hpp"

struct HashLogger {
   inline static std::ofstream log_file;
   inline static bool is_initialized = false;
   inline static long max_hash_table_bytes = 0;

   inline static std::map<int, std::tuple<long, double, double, long long>> build_phase_time;

   static void init()
   {
      if (is_initialized)
         return;
      std::filesystem::path log_path = std::filesystem::path(FLAGS_csv_path) / "hash_stats.csv";
      log_file.open(log_path, std::ios::trunc);
      log_file << "log2_produced_count,build_us,wait_us,hash_table_bytes\n";
      is_initialized = true;
   }

   static void log1(long produced_count, double build_us, double wait_us, long hash_table_bytes)
   {
      init();
      if (hash_table_bytes > max_hash_table_bytes) {
         max_hash_table_bytes = hash_table_bytes;
      }
      double log2_produced_count_double = std::log2(produced_count + 1);
      int log2_produced_count = std::round(log2_produced_count_double);
      if (build_phase_time.find(log2_produced_count) == build_phase_time.end()) {
         build_phase_time[log2_produced_count] = std::make_tuple(0, 0.0, 0.0, 0LL);
      }
      auto& [cnt, total_build_us, total_wait_us, total_ht_bytes] = build_phase_time[log2_produced_count];
      cnt++;
      total_build_us += build_us;
      total_wait_us += wait_us;
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
      for (auto* os : out_streams) {
         for (const auto& [log2_produced_count, v] : build_phase_time) {
            auto& [cnt, total_build_us, total_wait_us, total_ht_bytes] = v;
            *os << log2_produced_count << ',' << total_build_us / cnt << ',' << total_wait_us / cnt << ',' << total_ht_bytes / cnt << std::endl;
         }
         *os << "nan,nan,nan," << max_hash_table_bytes << std::endl;
      }
      log_file.close();
   }
};

// sources -> join_state -> yield joined records
template <typename JK, typename JR, typename R1, typename R2>
struct HashJoin {
   JK seek_jk = JK::max();
   const std::function<void(const typename JR::Key&, const JR&)> consume_joined;
   JoinState<JK, JR, R1, R2> state;

   std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left;
   std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right;

   typename std::unordered_multimap<JK, std::pair<typename R1::Key, R1>, std::hash<JK>> left_hashtable;

   double wait_us = 0;

   HashJoin(
       std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left,
       std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right,
       const JK& seek_jk,
       const std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})
       : seek_jk(seek_jk), consume_joined(consume_joined), state("HashJoin", consume_joined), fetch_left(fetch_left), fetch_right(fetch_right)
   {
      std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
      build_phase();
      std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
      auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      wait_us = d / 1000.0 - build_us;
   }

   ~HashJoin() { HashLogger::log1(state.get_produced(), build_us, wait_us, hash_table_bytes()); }

   long hash_table_bytes() const { return left_hashtable.size() * (sizeof(JK) + sizeof(typename R1::Key) + sizeof(R1)); }

   double build_us = 0;

   void build_phase()
   {
      std::optional<std::chrono::high_resolution_clock::time_point> start = std::nullopt;
      while (true) {
         auto kv = fetch_left();
         if (!start.has_value()) {
            start = std::chrono::high_resolution_clock::now();
         }
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
      auto build_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - *start).count();
      build_us = build_ns / 1000.0;
   }

   bool probe_next()
   {
      assert(!state.has_next());
      auto right_kv = fetch_right();
      if (!right_kv.has_value()) {
         return false;
      }
      auto& [rk, rv] = *right_kv;
      JK curr_jk = SKBuilder<JK>::create(rk, rv);
      if (seek_jk != JK::max() && curr_jk.match(seek_jk) != 0) {
         return false;
      }
      JK last_jk = state.jk_to_join;
      state.refresh(curr_jk);
      state.template emplace<R2, 1>(rk, rv);
      auto keys_to_match = curr_jk.matching_keys();
      for (const auto& lsk : keys_to_match) {
         auto range = left_hashtable.equal_range(lsk);
         for (auto it = range.first; it != range.second; ++it) {
            auto& [jk, l] = *it;
            auto& [lk, lv] = l;
            state.template emplace<R1, 0>(lk, lv);
         }
      }

      if (state.get_produced() != 0 && !state.has_next()) {
         std::cerr << "WARNING: HashJoin::probe_next() no match found for JK " << last_jk << ", violating integrity constraint" << std::endl;
         std::cerr << "Left hashtable size: " << left_hashtable.size() << std::endl;
         std::cerr << "Seek JK: " << seek_jk << std::endl;
      }
      return true;  // can probe next even if joined nothing
   }

   void run()
   {
      state.enable_logging();
      std::optional<std::pair<typename JR::Key, JR>> ret = next();
      while (ret.has_value()) {
         ret = next();
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      if (state.has_next()) {
         return state.next();
      }
      bool can_probe_next = true;
      while (can_probe_next && !state.has_next()) {
         can_probe_next = probe_next();
      }
      if (!state.has_next()) {  // exit condition: can_probe_next == false
         return std::nullopt;
      }
      return next();
   }
   // tricky to implement went_past
   // tricky to implement jk_to_join
   long produced() const { return state.get_produced(); }
};