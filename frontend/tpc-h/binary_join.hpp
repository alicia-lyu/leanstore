#pragma once
#include <functional>
#include <optional>
#include <vector>
#include "Units.hpp"
#include "view_templates.hpp"

// merge join
// LATER: outer join
template <typename JK, typename JR, typename LeftRec, typename RightRec>
class BinaryJoin
{
   long produced;
   long left_consumed;
   long right_consumed;
   long left_match_cnt;
   long right_match_cnt;
   bool left_matched;
   bool next_right_matched;

   std::vector<std::pair<typename RightRec::Key, RightRec>> cached_right;
   u64 cached_right_ptr;
   std::function<std::optional<std::pair<typename LeftRec::Key, LeftRec>>()> next_left_func;
   std::function<std::optional<std::pair<typename RightRec::Key, RightRec>>()> next_right_func;
   std::optional<std::pair<typename LeftRec::Key, LeftRec>> curr_left;
   std::optional<std::pair<typename RightRec::Key, RightRec>> next_right;

  public:
   BinaryJoin(std::function<std::optional<std::pair<typename LeftRec::Key, LeftRec>>()> next_left_func,
              std::function<std::optional<std::pair<typename RightRec::Key, RightRec>>()> next_right_func)
       : produced(0),
         left_consumed(0),
         right_consumed(0),
         left_match_cnt(0),
         right_match_cnt(0),
         left_matched(false),
         next_right_matched(false),
         next_left_func(next_left_func),
         next_right_func(next_right_func),
         cached_right_ptr(0)
   {
      curr_left = next_left_func();
      next_right = next_right_func();
   }

   ~BinaryJoin()
   {
      std::cout << std::endl;
      std::cout << "Left consumed: " << left_consumed << ", semi-join selectivity: " << (double)left_match_cnt / left_consumed * 100 << "%"
                << std::endl;
      std::cout << "Right consumed: " << right_consumed << ", semi-join selectivity: " << (double)right_match_cnt / right_consumed * 100 << "%"
                << std::endl;
      std::cout << "Produced: " << produced << std::endl;
   }

   std::optional<std::pair<typename LeftRec::Key, LeftRec>> nextLeft()
   {
      left_consumed++;
      if (left_matched) {
         left_match_cnt++;
         left_matched = false;
      }
      return next_left_func();
   }

   std::optional<std::pair<typename RightRec::Key, RightRec>> nextRight()
   {
      right_consumed++;
      if (next_right_matched) {
         right_match_cnt++;
         next_right_matched = false;
      }
      return next_right_func();
   }

   void run()
   {
      while (true) {
         [[maybe_unused]] auto res = next();
         if (!res.has_value()) {
            break;
         }
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      if (!curr_left.has_value()) {
         return std::nullopt;
      }
      auto& [lk, lr] = *curr_left;
      if (cached_right.size() > 0) {
         // std::cout << "size of cached_right: " << cached_right.size() << ", cached_right_ptr: " << cached_right_ptr << std::endl;
         auto curr_right = cached_right.at(cached_right_ptr % cached_right.size());
         [[maybe_unused]] auto& [rk, rr] = curr_right;
         if (cached_right_ptr == cached_right.size()) {
            // see whether next left matches cached right
            curr_left = nextLeft();
            cached_right_ptr = 0;
            if (!curr_left.has_value()) {
               return next();  // eventually return nullopt
            }
            auto& [lk, lr] = *curr_left;
            if (SKBuilder<JK>::create(lk, lr).match(SKBuilder<JK>::create(rk, rr)) != 0) {
               cached_right.clear();
               return next();  // go to second if-else
            }
            // else {
            //    std::cout << lk << " matches " << rk << std::endl;
            //    throw std::runtime_error("Should not happen");
            // }
         }
         cached_right_ptr++;
         return merge(lk, lr, rk, rr);
      }  // else proceed
      // zig zag to new current
      if (!next_right.has_value()) {
         return std::nullopt;
      }
      auto& [rk, rr] = *next_right;
      auto left_jk = SKBuilder<JK>::create(lk, lr);
      auto right_jk = SKBuilder<JK>::create(rk, rr);
      // std::cout << "left_jk: " << left_jk << ", right_jk: " << right_jk << std::endl;
      if (left_jk.match(right_jk) < 0) {
         curr_left = nextLeft();
         return next();  // go to second if-else
      } else if (left_jk.match(right_jk) == 0) {
         while (SKBuilder<JK>::create(rk, rr).match(left_jk) == 0) {
            next_right_matched = true;
            cached_right.push_back(next_right.value());
            next_right = nextRight();
            if (!next_right.has_value())
               break;
            [[maybe_unused]] auto& [rk, rr] = *next_right;
         }
         return next();  // go to first if-else
      } else {
         next_right = nextRight();
         return next();  // go to second if-else
      }
   };

   std::pair<typename JR::Key, JR> merge(typename LeftRec::Key& lk, LeftRec& lr, typename RightRec::Key& rk, RightRec& rr)
   {
      if ((++produced) % 1000 == 1) {
         std::cout << "\rJoined " << produced << " records------------------------------------";
      }
      left_matched = true;
      typename JR::Key jk(lk, rk);
      JR r(lr, rr);
      return std::make_pair(jk, r);
   }
};