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
   std::function<Key(LeftKey&, LeftRec&)> extractLeftJKFunc;
   std::function<Key(RightKey&, RightRec&)> extractRightJKFunc;
   std::function<LeftKey(u8*, u16)> unfoldLeftKeyFunc;
   std::function<RightKey(u8*, u16)> unfoldRightKeyFunc;

   std::vector<std::pair<RightKey, RightRec>> cachedRight;
   int cachedRightPtr;
   std::function<std::pair<LeftKey, LeftRec>()> nextLeftFunc;
   std::function<std::pair<RightKey, RightRec>()> nextRightFunc;

   std::pair<LeftKey, LeftRec> curr_left;

   std::pair<RightKey, RightRec> nextRight;

  public:
   Join(std::function<Key(LeftKey&, LeftRec&)> extractLeftJKFunc,
        std::function<Key(RightKey&, RightRec&)> extractRightJKFunc,
        std::function<LeftKey(u8*, u16)> unfoldLeftKeyFunc,
        std::function<RightKey(u8*, u16)> unfoldRightKeyFunc,
        std::function<std::pair<LeftKey, LeftRec>()> nextLeftFunc,
        std::function<std::pair<RightKey, RightRec>()> nextRightFunc)
       : extractLeftJKFunc(extractLeftJKFunc),
         extractRightJKFunc(extractRightJKFunc),
         unfoldLeftKeyFunc(unfoldLeftKeyFunc),
         unfoldRightKeyFunc(unfoldRightKeyFunc),
         nextLeftFunc(nextLeftFunc),
         nextRightFunc(nextRightFunc)
   {
      curr_left = nextLeftFunc();
      nextRight = nextRightFunc();
   }

   std::optional<std::pair<Key, Rec>> next()
   {
      if (curr_left == std::pair<LeftKey, LeftRec>()) {
         // Regular record is not zero-initialized
         return std::nullopt;
      }
      if (cachedRight.size() > 0) {
         if (cachedRightPtr == cachedRight.size())
         {
            // see whether next left matches cached right
            curr_left = nextLeftFunc();
            cachedRightPtr = 0;
            if (extractLeftJKFunc(curr_left) != extractRightJKFunc(cachedRight.front())) {
               cachedRight.clear();
               return next(); // go to second if-else
            }
         }
         auto curr_right = cachedRight.at(cachedRightPtr++);
         return merge(curr_left.first, curr_left.second, curr_right.first, curr_right.second);
      } // else proceed
      // zig zag to new current
      auto left_jk = extractLeftJKFunc(curr_left);
      auto right_jk = extractRightJKFunc(nextRight);
      if (left_jk < right_jk) {
         curr_left = nextLeftFunc();
         return next();  // go to second if-else
      } else if (left_jk == right_jk) {
         while (extractRightJKFunc(nextRight) == left_jk) {
            cachedRight.push_back(nextRight);
            nextRight = nextRightFunc();
         }
         return next();  // go to first if-else
      } else {
         nextRight = nextRightFunc();
         return next();  // go to second if-else
      }
   };

   std::pair<Key, Rec> merge(LeftKey& lk, LeftRec& lr, RightKey&, RightRec& rr)
   {
      Key k = extractLeftJKFunc(lk, lr);
      auto payloads = std::make_tuple(lr, rr);
      Rec r = Rec(payloads);
      return std::make_pair(k, r);
   }
};