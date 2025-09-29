#pragma once
#include <unordered_map>
#include "join_state.hpp"

// sources -> join_state -> yield joined records
template <template <typename> class ScannerType, typename JK, typename JR, typename R1, typename R2>
struct HashJoin {
   JoinState<JK, JR, R1, R2> join_state;
   const JK seek_jk;
   ScannerType<R1>& left_scanner;
   ScannerType<R2>& right_scanner;
   std::unordered_multimap<JK, R1> left_hashtable;
   std::unordered_multimap<JK, R2> right_hashtable;

   std::unordered_multimap<JK, R1>::iterator left_it;

   HashJoin(
       ScannerType<R1>& left_scanner,
       ScannerType<R2>& right_scanner,
       JK seek_jk,
       const std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})

       : join_state("HashJoin", consume_joined), seek_jk(seek_jk), left_scanner(left_scanner), right_scanner(right_scanner)
   {
      refresh_join_state();  // initialize the join state with the smallest JK
      left_scanner.seek(typename R1::Key{seek_jk});
      right_scanner.seek(typename R2::Key{seek_jk});
      hash_phase<R1>(left_scanner, left_hashtable);
      hash_phase<R2>(right_scanner, right_hashtable);
      left_it = left_hashtable.begin();
   }

   long hash_table_bytes() const
   {
      return left_hashtable.size() * (sizeof(JK) + sizeof(R1)) + right_hashtable.size() * (sizeof(JK) + sizeof(R2));
   }

   template <typename R>
   void hash_phase(ScannerType<R>& scanner, std::unordered_multimap<JK, R>& hashtable)
   {
      while (true) {
         auto kv = scanner.next();
         if (!kv.has_value()) {
            break;
         }
         auto& [k, v] = *kv;
         JK curr_jk = SKBuilder<JK>::create(k, v);
         if (curr_jk.match(seek_jk) != 0) {
            break;
         }
         hashtable.emplace(curr_jk, v);
      }
   }

   void refill_current_key()
   {
      auto& [jk, _] = *left_it;
      auto& [left_begin, left_end] = left_hashtable.equal_range(jk);
      for (auto it = left_begin; it != left_end; ++it) {
         join_state.template emplace<R1, 0>(typename R1::Key{jk}, it->second);
      }
      left_it = left_end;
      auto& [right_begin, right_end] = right_hashtable.equal_range(jk);
      for (auto it = right_begin; it != right_end; ++it) {
         join_state.template emplace<R2, 1>(typename R2::Key{jk}, it->second);
      }
   }

   void refresh_join_state()
   {
      JK jk = left_it != left_hashtable.end() ? left_it->first : JK::max();
      // STILL NEED TO HANDLE BOTH JK?
      // int comp = left_jk.match(right_jk);
      // if (comp != 0) {  // cache the smaller one
      join_state.refresh(jk);
   }

   bool went_past(const JK& match_jk) const { return join_state.went_past(match_jk); }

   bool has_cached_next() const { return join_state.has_next(); }

   void next_jk()
   {
      refill_current_key();
      refresh_join_state();
   }

   void run()
   {
      join_state.enable_logging();
      std::optional<std::pair<typename JR::Key, JR>> ret = next();
      while (ret.has_value()) {
         ret = next();
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      while (!join_state.has_next() && left_it != left_hashtable.end()) {
         next_jk();
      }
      return join_state.next();
   }

   JK jk_to_join() const { return join_state.jk_to_join; }
   long produced() const { return join_state.get_produced(); }
};