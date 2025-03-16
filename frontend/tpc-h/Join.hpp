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
   std::function<std::optional<std::pair<LeftKey, LeftRec>>()> nextLeftFunc;
   std::function<std::optional<std::pair<RightKey, RightRec>>()> nextRightFunc;

   std::optional<std::pair<LeftKey, LeftRec>> curr_left;

   std::optional<std::pair<RightKey, RightRec>> nextRight;

  public:
   Join(std::function<Key(LeftKey&, LeftRec&)> extractLeftJKFunc,
        std::function<Key(RightKey&, RightRec&)> extractRightJKFunc,
        std::function<LeftKey(u8*, u16)> unfoldLeftKeyFunc,
        std::function<RightKey(u8*, u16)> unfoldRightKeyFunc,
        std::function<std::optional<std::pair<LeftKey, LeftRec>>()> nextLeftFunc,
        std::function<std::optional<std::pair<RightKey, RightRec>>()> nextRightFunc)
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

   std::optional<std::pair<typename Rec::Key, Rec>> next()
   {
      if (!curr_left.has_value()) {
         // Regular record is not zero-initialized
         return std::nullopt;
      }
      auto& [lk, lr] = *curr_left;
      if (cachedRight.size() > 0) {
         auto curr_right = cachedRight.at(cachedRightPtr % cachedRight.size());
         auto& [rk, rr] = curr_right;
         if (cachedRightPtr == cachedRight.size())
         {
            // see whether next left matches cached right
            curr_left = nextLeftFunc();
            cachedRightPtr = 0;
            if (!curr_left.has_value()) {
               return next(); // eventually return nullopt
            }
            auto& [lk, lr] = *curr_left;
            if (extractLeftJKFunc(lk, lr) != extractRightJKFunc(rk, rr)) {
               cachedRight.clear();
               return next(); // go to second if-else
            }
         }
         cachedRightPtr++;
         return merge(lk, lr, rk, rr);
      } // else proceed
      // zig zag to new current
      if (!nextRight.has_value()) {
         return std::nullopt;
      }
      auto& [rk, rr] = *nextRight;
      auto left_jk = extractLeftJKFunc(lk, lr);
      auto right_jk = extractRightJKFunc(rk, rr);
      if (left_jk < right_jk) {
         curr_left = nextLeftFunc();
         return next();  // go to second if-else
      } else if (left_jk == right_jk) {
         while (extractRightJKFunc(rk, rr) == left_jk) {
            cachedRight.push_back(nextRight.value());
            nextRight = nextRightFunc();
            if (!nextRight.has_value())
               break;
            auto& [rk, rr] = *nextRight;
         }
         return next();  // go to first if-else
      } else {
         nextRight = nextRightFunc();
         return next();  // go to second if-else
      }
   };

   std::pair<typename Rec::Key, Rec> merge(LeftKey& lk, LeftRec& lr, RightKey& rk, RightRec& rr)
   {
      Key k = extractLeftJKFunc(lk, lr);
      typename Rec::Key jk(lk, rk);
      Rec r(lr, rr);
      return std::make_pair(jk, r);
   }
};