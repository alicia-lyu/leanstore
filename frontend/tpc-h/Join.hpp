#pragma once
#include <functional>
#include <optional>
#include <vector>
#include "Units.hpp"

// merge join
// LATER: outer join
template <typename Key, typename Rec, typename LeftKey, typename LeftRec, typename RightKey, typename RightRec>
class Join
{
   long produced;
   long left_consumed;
   long right_consumed;
   long left_match_cnt;
   long right_match_cnt;
   bool left_matched;
   bool next_right_matched;
   std::function<Key(LeftKey&, LeftRec&)> extractLeftJKFunc;
   std::function<Key(RightKey&, RightRec&)> extractRightJKFunc;
   std::function<LeftKey(u8*, u16)> unfoldLeftKeyFunc;
   std::function<RightKey(u8*, u16)> unfoldRightKeyFunc;

   std::vector<std::pair<RightKey, RightRec>> cachedRight;
   u64 cachedRightPtr;
   std::function<std::optional<std::pair<LeftKey, LeftRec>>()> nextLeftFunc;
   std::function<std::optional<std::pair<RightKey, RightRec>>()> nextRightFunc;

   std::optional<std::pair<LeftKey, LeftRec>> curr_left;

   std::optional<std::pair<RightKey, RightRec>> next_right;

  public:
   Join(std::function<Key(LeftKey&, LeftRec&)> extractLeftJKFunc,
        std::function<Key(RightKey&, RightRec&)> extractRightJKFunc,
        std::function<LeftKey(u8*, u16)> unfoldLeftKeyFunc,
        std::function<RightKey(u8*, u16)> unfoldRightKeyFunc,
        std::function<std::optional<std::pair<LeftKey, LeftRec>>()> nextLeftFunc,
        std::function<std::optional<std::pair<RightKey, RightRec>>()> nextRightFunc)
       : produced(0),
         left_consumed(0),
         right_consumed(0),
         left_match_cnt(0),
         right_match_cnt(0),
         left_matched(false),
         next_right_matched(false),
         extractLeftJKFunc(extractLeftJKFunc),
         extractRightJKFunc(extractRightJKFunc),
         unfoldLeftKeyFunc(unfoldLeftKeyFunc),
         unfoldRightKeyFunc(unfoldRightKeyFunc),
         nextLeftFunc(nextLeftFunc),
         nextRightFunc(nextRightFunc),
         cachedRightPtr(0)
   {
      curr_left = nextLeft();
      next_right = nextRight();
   }

   ~Join()
   {
      std::cout << std::endl;
      std::cout << "Left consumed: " << left_consumed << ", semi-join selectivity: " << (double)left_match_cnt / left_consumed * 100 << "%"
                << std::endl;
      std::cout << "Right consumed: " << right_consumed << ", semi-join selectivity: " << (double)right_match_cnt / right_consumed * 100 << "%"
                << std::endl;
      std::cout << "Produced: " << produced << std::endl;
   }

   std::optional<std::pair<LeftKey, LeftRec>> nextLeft()
   {
      left_consumed++;
      if (left_matched) {
         left_match_cnt++;
         left_matched = false;
      }
      return nextLeftFunc();
   }

   std::optional<std::pair<RightKey, RightRec>> nextRight()
   {
      right_consumed++;
      if (next_right_matched) {
         right_match_cnt++;
         next_right_matched = false;
      }
      return nextRightFunc();
   }

   std::optional<std::pair<typename Rec::Key, Rec>> next()
   {
      if (!curr_left.has_value()) {
         return std::nullopt;
      }
      auto& [lk, lr] = *curr_left;
      if (cachedRight.size() > 0) {
         // std::cout << "size of cachedRight: " << cachedRight.size() << ", cachedRightPtr: " << cachedRightPtr << std::endl;
         auto curr_right = cachedRight.at(cachedRightPtr % cachedRight.size());
         [[maybe_unused]] auto& [rk, rr] = curr_right;
         if (cachedRightPtr == cachedRight.size()) {
            // see whether next left matches cached right
            curr_left = nextLeft();
            cachedRightPtr = 0;
            if (!curr_left.has_value()) {
               return next();  // eventually return nullopt
            }
            auto& [lk, lr] = *curr_left;
            if (extractLeftJKFunc(lk, lr).match(extractRightJKFunc(rk, rr)) != 0) {
               cachedRight.clear();
               return next();  // go to second if-else
            }
         }
         cachedRightPtr++;
         return merge(lk, lr, rk, rr);
      }  // else proceed
      // zig zag to new current
      if (!next_right.has_value()) {
         return std::nullopt;
      }
      auto& [rk, rr] = *next_right;
      auto left_jk = extractLeftJKFunc(lk, lr);
      auto right_jk = extractRightJKFunc(rk, rr);
      // std::cout << "left_jk: " << left_jk << ", right_jk: " << right_jk << std::endl;
      if (left_jk.match(right_jk) < 0) {
         curr_left = nextLeft();
         return next();  // go to second if-else
      } else if (left_jk.match(right_jk) == 0) {
         while (extractRightJKFunc(rk, rr).match(left_jk) == 0) {
            next_right_matched = true;
            cachedRight.push_back(next_right.value());
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

   std::pair<typename Rec::Key, Rec> merge(LeftKey& lk, LeftRec& lr, RightKey& rk, RightRec& rr)
   {
      if ((++produced) % 1000 == 1) {
         std::cout << "\rJoined " << produced << " records------------------------------------";
      }
      left_matched = true;
      typename Rec::Key jk(lk, rk);
      Rec r(lr, rr);
      return std::make_pair(jk, r);
   }
};