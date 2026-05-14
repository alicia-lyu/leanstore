#pragma once
#include "join_state.hpp"

// sources -> join_state -> yield joined records
template <typename JK, typename JR, typename R1, typename R2>
struct BinaryMergeJoin {
   JK seek_jk = JK::max();
   JoinState<JK, JR, R1, R2> join_state;
   std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left;
   std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right;
   std::optional<std::pair<typename R1::Key, R1>> next_left;
   std::optional<std::pair<typename R2::Key, R2>> next_right;

   BinaryMergeJoin(
       std::function<std::optional<std::pair<typename R1::Key, R1>>()> fetch_left_func,
       std::function<std::optional<std::pair<typename R2::Key, R2>>()> fetch_right_func,
       const std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})

       : join_state("BinaryMergeJoin", consume_joined),
         fetch_left(std::move(fetch_left_func)),
         fetch_right(std::move(fetch_right_func)),
         next_left(fetch_left()),
         next_right(fetch_right())
   {
      // assert(next_left || next_right);  // at least one source must be available
      refresh_join_state();  // initialize the join state with the smallest JK
   }

   void replace_sk(const JK& new_sk) { seek_jk = new_sk; }

   void refill_current_key()
   {
      while (next_left && SKBuilder<JK>::create(next_left->first, next_left->second).match(join_state.jk_to_join) == 0) {
         join_state.template emplace<R1, 0>(next_left->first, next_left->second);
         next_left = fetch_left();
      }
      while (next_right && SKBuilder<JK>::create(next_right->first, next_right->second).match(join_state.jk_to_join) == 0) {
         join_state.template emplace<R2, 1>(next_right->first, next_right->second);
         next_right = fetch_right();
      }
   }

   void refresh_join_state()
   {
      JK left_jk = next_left ? SKBuilder<JK>::create(next_left->first, next_left->second) : JK::max();
      if (seek_jk != JK::max() && left_jk.match(seek_jk) != 0) {
         left_jk = JK::max();
         next_left = std::nullopt;
      }
      JK right_jk = next_right ? SKBuilder<JK>::create(next_right->first, next_right->second) : JK::max();
      if (seek_jk != JK::max() && right_jk.match(seek_jk) != 0) {
         right_jk = JK::max();
         next_right = std::nullopt;
      }

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

   bool has_cached_next() const { return join_state.has_next(); }

   void next_jk()
   {
      refill_current_key();
      refresh_join_state();
   }

   void run()
   {
      join_state.enable_logging();
      while (next_left || next_right) {
         next();
      }
      // Flush the last group: refresh_join_state() early-returns when both
      // sides exhaust (both JKs = max), leaving the final batch in
      // records_to_join un-joined. Force one more join_and_clear.
      join_state.refresh(JK::max());
      while (join_state.has_next()) {
         join_state.next();
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      while (!join_state.has_next() && (next_left || next_right)) {
         next_jk();
      }
      // Final-group flush: when both sides exhaust in the same next_jk()
      // pass (next_left and next_right both become nullopt while their
      // pre-exhaust JKs were equal), refresh_join_state() early-returns on
      // `left_jk == JK::max()` and never calls join_state.refresh(), so
      // the records emplaced for that last shared key sit in
      // records_to_join un-joined. Force one final flush so the last
      // group's cartesian product is produced before we declare the
      // BMJ exhausted. Idempotent on subsequent calls (records_to_join
      // already empty → join_current() returns 0).
      if (!join_state.has_next() && !next_left && !next_right
          && !final_flushed) {
         join_state.refresh(JK::max());
         final_flushed = true;
      }
      return join_state.next();
   }

  private:
   bool final_flushed = false;

  public:

   JK jk_to_join() const { return join_state.jk_to_join; }
   long produced() const { return join_state.get_produced(); }
};