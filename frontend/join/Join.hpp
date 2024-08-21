#pragma once
#include <cassert>
#include <iostream>
#include <optional>
#include <vector>
#include "../shared/Scanner.hpp"
#include "../shared/Types.hpp"
#include "../tpc-c/Schema.hpp"
#include "Exceptions.hpp"
#include "JoinedSchema.hpp"

template <typename Record1, typename Record2, typename JoinedRecord>
class MergeJoin
{
   constexpr static unsigned join_key_length = JoinedRecord::joinKeyLength();

  public:
   MergeJoin(Scanner<Record1>* left_scanner, Scanner<Record2>* right_scanner)
       : left_scanner(left_scanner),
         right_scanner(right_scanner),
         cached_right_key(std::nullopt),
         cached_right_records_iter(cached_right_records.begin())
   {
      cached_right_records.reserve(100);
      advance_left();
      advance_right();
   }

   ~MergeJoin()
   {
      if (produced > 99999) {
         std::cout << "~MergeJoin(): Produced " << produced << ", consumed " << left_consumed << " (left) " << right_consumed << " (right)"
                   << std::endl;
         std::cout << "Left is exhausted: " << left_exhausted << std::endl;
         std::cout << "Right is exhausted: " << right_exhausted << std::endl;
         std::cout << "Left semi-join selectivity: " << (double)left_matched / left_consumed << std::endl;
         std::cout << "Right semi-join selectivity: " << (double)right_matched / right_consumed << std::endl;
      }
   }

   std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> next()
   {
      std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> joined_record = std::nullopt;

      while (!left_exhausted) {
         int cmp = compare_keys();
         if (cmp == 0 && !right_exhausted) {
            // Join current left with current right
            if (!cached_right_key.has_value() || cached_right_key->s_w_id != right_key.s_w_id || cached_right_key->s_i_id != right_key.s_i_id) {
               cached_right_key = right_key;
               cached_right_records.clear();
            }
            cached_right_records.push_back(right_record);
            // Reset iterator after modifying cached_right_records
            cached_right_records_iter = cached_right_records.begin();

            joined_record = std::make_optional(join_records());
            break;
         } else if ((cmp < 0 || right_exhausted)) {
            // Join current left with cached right

            if (cached_right_records.empty()) {
               // std::cout << "Advance left because no cached right records" << std::endl;
               advance_left();
               continue;
            }

            if (cached_right_records_iter == cached_right_records.end()) {
               // Next left record may join with the cached right records
               cached_right_records_iter = cached_right_records.begin();
               // std::cout << "Advance left because cached right records exhausted" << std::endl;
               advance_left();
               continue;
            } else if (compare_keys(true) > 0) {
               // The cached right records are too small, will never be joined with left records beyond this point
               if (right_exhausted)
                  return std::nullopt;  // The following left records has no cached rows or current row to join
               // std::cout << "Advance left because cached right records too small" << std::endl;
               advance_left();
               continue;
            }
            assert(compare_keys(true) == 0);  // Those cached records were joined with a left record smaller than or equal to the current one
            joined_record = std::make_optional(join_records(true));
            break;
         } else if (cmp > 0) {
            if (!right_exhausted) {
               // Current right record is too small. Cached right records are even smaller
               advance_right();
            } else {
               return std::nullopt;
            }
         } else {
            UNREACHABLE();
         }
      }

      if (joined_record.has_value())
         produced++;
      if (joined_record.has_value() && produced % 1000000 == 0) {
         std::cout << "Produced " << produced << ", consumed " << left_consumed << " (left) " << right_consumed << " (right)" << std::endl;
         std::cout << "Current joined record, key: " << joined_record.value().first << std::endl;
         // std::cout << "Payload: " << joined_record.value().second << std::endl;
      }

      return joined_record;
   }

   template <typename LeftKey, typename RightKey, typename JoinedKey>
   static JoinedKey merge_keys(const LeftKey& left_key, const RightKey& right_key);

   template <typename Left, typename Right, typename Joined>
   static Joined merge_records(const Left& left_rec, const Right& right_rec);

   // Only one merge_keys for all variants of joined_t
   template <typename JoinedKey = joined1_t::Key>
   static JoinedKey merge_keys(const ol_sec1_t::Key& left_key, const stock_t::Key&)
   {
      joined1_t::Key key{left_key.ol_w_id, left_key.ol_i_id, left_key.ol_d_id, left_key.ol_o_id, left_key.ol_number};
      return key;
   }

   template <typename Joined = joined1_t>
   static Joined merge_records(const ol_sec1_t& left_rec, const stock_t& right_rec) requires(std::same_as<Joined, joined1_t>)
   {
      joined1_t record{left_rec.ol_supply_w_id, left_rec.ol_delivery_d, left_rec.ol_quantity,   left_rec.ol_amount,  right_rec.s_quantity,
                       right_rec.s_dist_01,     right_rec.s_dist_02,    right_rec.s_dist_03,    right_rec.s_dist_04, right_rec.s_dist_05,
                       right_rec.s_dist_06,     right_rec.s_dist_07,    right_rec.s_dist_08,    right_rec.s_dist_09, right_rec.s_dist_10,
                       right_rec.s_ytd,         right_rec.s_order_cnt,  right_rec.s_remote_cnt, right_rec.s_data};

      return record;
   }

   template <typename Joined = joined0_t>
   static Joined merge_records(const ol_sec0_t&, const stock_t&) requires(std::same_as<Joined, joined0_t>)
   {
      joined0_t record{};
      return record;
   }

   static std::pair<typename JoinedRecord::Key, JoinedRecord> merge(const ol_sec1_t::Key& left_key,
                                                                    const ol_sec1_t& left_rec,
                                                                    const stock_t::Key& right_key,
                                                                    const stock_t& right_rec)
      requires(std::same_as<JoinedRecord, joined_selected_t>)
   {
      joined1_t record = merge_records<joined1_t>(left_rec, right_rec);
      joined1_t::Key key = merge_keys<joined1_t::Key>(left_key, right_key);
      joined_selected_t selected = record.toSelected(key);
      return {key, selected};
   }

   static std::pair<typename JoinedRecord::Key, JoinedRecord> merge(const ol_sec0_t::Key& left_key,
                                                                    const ol_sec0_t& left_rec,
                                                                    const stock_t::Key& right_key,
                                                                    const stock_t& right_rec)
      requires(std::same_as<JoinedRecord, joined0_t> || std::same_as<JoinedRecord, joined1_t>)
   {
      return {merge_keys<typename JoinedRecord::Key>(left_key, right_key), merge_records<JoinedRecord>(left_rec, right_rec)};
   }

  private:
   Scanner<Record1>* left_scanner;
   Scanner<Record2>* right_scanner;
   size_t left_consumed = 0;
   size_t right_consumed = 0;
   size_t produced = 0;

   typename Record1::Key left_key;
   typename Record2::Key right_key;
   Record1 left_record;
   Record2 right_record;
   bool left_exhausted = false;
   bool right_exhausted = false;

   size_t left_matched = 0;
   size_t right_matched = 0;
   bool current_left_matched = false;
   bool current_right_matched = false;

   std::optional<typename Record2::Key> cached_right_key;
   std::vector<Record2> cached_right_records;
   typename std::vector<Record2>::iterator cached_right_records_iter;

   bool advance_left()
   {
      assert(!left_exhausted);
      if (current_left_matched) {
         left_matched++;
      }
      auto ret = left_scanner->next();
      if (!ret.has_value()) {
         left_exhausted = true;
         return false;
      }
      left_consumed++;
      left_key = ret.value().key;
      left_record = ret.value().record;
      current_left_matched = false;
      // std::cout << "Advance left: " << left_key << std::endl;
      return true;
   }

   bool advance_right()
   {
      assert(!right_exhausted);
      if (current_right_matched) {
         right_matched++;
      }
      if (right_exhausted)
         return false;
      auto ret = right_scanner->next();
      if (!ret.has_value()) {
         right_exhausted = true;
         return false;
      }
      right_consumed++;
      right_key = ret.value().key;
      right_record = ret.value().record;
      current_right_matched = false;
      // std::cout << "Advance right: " << right_key << std::endl;
      return true;
   }

   int compare_keys(bool use_cached = false) const
   {
      // HARDCODED
      if (use_cached == false) {
         s32 w_diff = left_key.ol_w_id - right_key.s_w_id;
         if (w_diff != 0) {
            return w_diff;
         }
         return left_key.ol_i_id - right_key.s_i_id;
      } else {
         s32 w_diff = left_key.ol_w_id - cached_right_key.value().s_w_id;
         if (w_diff != 0) {
            return w_diff;
         }
         return left_key.ol_i_id - cached_right_key.value().s_i_id;
      }
   }

   std::pair<typename JoinedRecord::Key, JoinedRecord> join_records(bool use_cached = false)
   {
      const Record2& rightR = use_cached ? *cached_right_records_iter : right_record;
      const typename Record2::Key& rightK = use_cached ? cached_right_key.value() : this->right_key;

      const auto [key, record] =
          merge(const_cast<const typename Record1::Key&>(left_key), const_cast<const Record1&>(left_record), rightK, rightR);

      current_left_matched = true;
      if (use_cached) {
         ++cached_right_records_iter;
      } else {
         current_right_matched = true;
         bool ret = advance_right();
         if ((ret && compare_keys() != 0) || !ret)
            advance_left();
      }

      return {key, record};
   }
};
