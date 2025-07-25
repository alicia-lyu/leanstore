#pragma once
#include <cstddef>
#include <functional>

#include <optional>
#include <queue>
#include <variant>
#include <vector>
#include "../variant_tuple_utils.hpp"
#include "../view_templates.hpp"
#include "leanstore/Config.hpp"

template <typename JK, typename JR, typename... Rs>
class JoinState
{
  public:
   JK jk_to_join = JK::max();
   void enable_logging() { logging = true; }

   JoinState(
       const std::string& msg,
       const std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})
       : consume_joined(consume_joined), msg(msg)
   {
   }

   ~JoinState()
   {
      if (
          // jk_to_join % 1000 == 1 || // sampling
          joined > 10000)
         std::cout << "~JoinState: joined " << (double)joined / 1000 << "k records. Ended at JK " << jk_to_join << std::endl;
   }

   bool went_past(const JK& ballpark_jk) const
   {
      if (jk_to_join == JK::max()) {
         return false;
      }
      return jk_to_join.match(ballpark_jk) > 0 &&  // future joined records are all larger than match_jk
             !has_next();                       // no more cached joined records, which might match match_jk, left to consume
   }

   void refresh(const JK& next_jk)
   {
      // assert(next_jk.match(jk_to_join) != 0);
      int curr_joined = join_and_clear(next_jk, std::index_sequence_for<Rs...>{});
      update_print_produced(curr_joined);
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      if (joined_records.empty()) {
         return std::nullopt;
      }
      auto joined_pair = joined_records.front();
      joined_records.pop();
      consume_joined(joined_pair.first, joined_pair.second);
      return joined_pair;
   }

   bool has_next() const { return !joined_records.empty(); }

   template <typename Record, size_t I>
   void emplace(const typename Record::Key& key, const Record& rec)
   {
      JK jk = SKBuilder<JK>::create(key, rec);
      std::get<I>(records_to_join).emplace_back(key, rec);
      jk_to_join = jk;
   }

   template <size_t... Is>
   void emplace(const std::variant<typename Rs::Key...>& key, const std::variant<Rs...>& rec, std::index_sequence<Is...>)
   {
      (([&]() {
          if (std::holds_alternative<typename Rs::Key>(key)) {
             assert(std::holds_alternative<Rs>(rec));
             emplace<Rs, Is>(std::get<typename Rs::Key>(key), std::get<Rs>(rec));
          }
       })(),
       ...);
   }

   void emplace(const std::variant<typename Rs::Key...>& key, const std::variant<Rs...>& rec) { emplace(key, rec, std::index_sequence_for<Rs...>{}); }

   long get_produced() const
   {
      size_t joined_not_produced = joined_records.size();
      if (joined_not_produced > 0) {
         std::cerr << "WARNING: JoinState: get_produced() called while there are still " << joined_not_produced << " joined records in the queue." << std::endl;
      }
      return joined - joined_not_produced;
   }

   size_t get_remaining_records_to_join() const
   {
      size_t remaining = 0;
      for_each(records_to_join, [&](const auto& v) { remaining += v.size(); });
      return remaining;
   }

  private:
   bool logging = false;
   std::tuple<std::vector<std::pair<typename Rs::Key, Rs>>...> records_to_join = {};
   std::queue<std::pair<typename JR::Key, JR>> joined_records = {};

   long joined = 0;

   const std::function<void(const typename JR::Key&, const JR&)> consume_joined;
   const std::string msg;

   template <size_t... Is>
   void assemble_joined_records(std::vector<std::tuple<std::pair<typename Rs::Key, Rs>...>>& cartesian_product,
                                unsigned long* batch_size,
                                size_t len_cartesian_product,
                                std::index_sequence<Is...>)
   {
      (..., ([&] {
          auto& vec = std::get<Is>(records_to_join);
          size_t repeat = *batch_size / vec.size(), batch = len_cartesian_product / *batch_size;
          for (size_t b = 0; b < batch; ++b)
             for (size_t r = 0; r < repeat; ++r)
                for (size_t j = 0; j < vec.size(); ++j) {
                   auto& pairs = cartesian_product.at(b * *batch_size + j * repeat + r);
                   std::get<Is>(pairs) = vec.at(j);
                }
          *batch_size /= vec.size();
       })());
   }

   int join_current()
   {
      size_t len_cartesian_product = 1;  // how many records the cached records can join and produce
      for_each(records_to_join, [&](const auto& v) { len_cartesian_product *= v.size(); });
      if (len_cartesian_product == 0)
         return 0;

      // assign the cached records to the cartesian product to be joined
      std::vector<std::tuple<std::pair<typename Rs::Key, Rs>...>> cartesian_product(len_cartesian_product);
      unsigned long batch_size = len_cartesian_product;
      assemble_joined_records(cartesian_product, &batch_size, len_cartesian_product, std::index_sequence_for<Rs...>{});

      // actually joining the records
      for (auto& cartesian_product_instance : cartesian_product) {
         typename JR::Key joined_key;
         JR joined_rec;
         std::apply(
             [&joined_key, &joined_rec](auto&... pairs) {
                joined_key = typename JR::Key{std::get<0>(pairs)...};
                joined_rec = JR{std::get<1>(pairs)...};
             },
             cartesian_product_instance);
         joined_records.emplace(joined_key, joined_rec);
      }
      return static_cast<int>(len_cartesian_product);
   }

   template <size_t... Is>
   int join_and_clear(const JK& next_jk, std::index_sequence<Is...>)
   {
      int joined_cnt = 0;
      (..., ([&] {
          auto& vec = std::get<Is>(records_to_join);
          using VecElem = typename std::remove_reference_t<decltype(vec)>::value_type;
          using RecordType = std::tuple_element_t<1, VecElem>;
          if (next_jk.match(SKBuilder<JK>::template get<RecordType>(jk_to_join)) != 0) {
             joined_cnt += join_current();
             vec.clear();
          }
       }()));
      jk_to_join = next_jk;
      return joined_cnt;
   }

   long last_logged = 0;

   void update_print_produced(int curr_joined)
   {
      joined += curr_joined;
      if (logging && joined > last_logged + 1000 && FLAGS_log_progress) {
         double progress = (double)joined / 1000;
         std::cout << "\r" << msg << ": joined " << progress << "k records at JK " << jk_to_join << "------------------------------------";
         last_logged = joined;
      }
      if (joined_records.size() > 1000 && joined_records.size() > static_cast<size_t>(curr_joined)) {
         throw std::runtime_error(
             "JoinState: too many joined records in the queue. Are you calling JoinState::next()? JoinState is only supposed to be a pipeline "
             "instead of a storage!");
      }
   }
};