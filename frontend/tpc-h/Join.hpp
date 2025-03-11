#pragma once
#include <functional>
#include <vector>
#include "Units.hpp"

// merge join
template <typename Key, typename Rec, typename LeftKey, typename LeftRec, typename RightKey, typename RightRec>
class Join
{
   std::function<Key(LeftKey, LeftRec)> extractLeftJKFunc;
   std::function<Key(RightKey, RightRec)> extractRightJKFunc;
   std::function<LeftKey(u8*, u16)> unfoldLeftKeyFunc;
   std::function<RightKey(u8*, u16)> unfoldRightKeyFunc;

   std::vector<std::pair<RightKey, RightRec>> cachedRight;
   int cachedRightPtr;
   std::function<std::pair<LeftKey, LeftRec>> nextLeftFunc;
   std::function<std::pair<RightKey, RightRec>> nextRightFunc;

   std::pair<LeftKey, LeftRec> curr_left;

   std::pair<RightKey, RightRec> nextCachedRight() { 
      cachedRightPtr = ++cachedRightPtr % cachedRight.size();
      return cachedRight.at(); 
   }

   std::pair<RightKey, RightRec> nextRight;

  public:
   Join(std::function<Key(LeftKey, LeftRec)> extractLeftJKFunc,
        std::function<Key(RightKey, RightRec)> extractRightJKFunc,
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

   std::pair<Key, Rec> next()
   {
      if (cachedRightPtr < cachedRight.size() && cachedRight.size() > 0) {  // Matching remains in current records
         auto curr_right = nextCachedRight();
         return merge(curr_left.first, curr_left.second, curr_right.first, curr_right.second);
      } else if (cachedRightPtr == cachedRight.size() && cachedRightPtr > 0) {
         // Matching exhausted cached right
         curr_left = nextLeftFunc();
         auto curr_right = nextCachedRight();
         if (extractLeftJKFunc(curr_left) == extractRightJKFunc(curr_right)) {
            return next();  // go to first arm
         } else {
            cachedRight.clear();
         }
      }  // else proceed
      // zig zag to new current
      auto left_jk = extractLeftJKFunc(curr_left);
      auto right_jk = extractRightJKFunc(nextRight);
      if (left_jk < right_jk) {
         curr_left = nextLeftFunc();
         return next();  // go to third (not explicit) arm and zig zag
      } else if (left_jk == right_jk) {
         while (extractRightJKFunc(nextRight) == right_jk) {
            cachedRight.push_back(nextRight);
            nextRight = nextRightFunc();
         }
         return next();  // go to first arm
      } else {
         nextRight = nextRightFunc();
         return next();  // go to third arm
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