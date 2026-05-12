#pragma once
#include "../shared/Types.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

namespace randutils
{
inline Integer rnd(Integer n)
{
   return leanstore::utils::RandomGenerator::getRand(0, n);
}
// [low, high]
inline Integer urand(Integer low, Integer high)
{
   return rnd(high - low + 1) + low;
}

template <int maxLength>
Varchar<maxLength> randomastring(Integer minLenStr, Integer maxLenStr)
{
   assert(maxLenStr <= maxLength);
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<maxLength> result;
   for (Integer index = 0; index < len; index++) {
      // alphanumeric only
      Integer i = rnd(36);
      if (i < 10)
         result.append(48 + i);
      else
         result.append(64 - 10 + i);
   }
   return result;
}

inline Varchar<16> randomnstring(Integer minLenStr, Integer maxLenStr)
{
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<16> result;
   for (Integer i = 0; i < len; i++)
      result.append(48 + rnd(10));
   return result;
}

inline Numeric randomNumeric(Numeric min, Numeric max)
{
   // getRandU64() returns a value in [0, 2^64). Map to [0,1) by dividing by
   // 2^64, then scale to [min, max). The previous implementation divided by
   // RAND_MAX (2^31-1) which produced values orders of magnitude outside the
   // requested range when getRandU64 sat in its upper bits.
   constexpr double inv_u64 = 1.0 / 18446744073709551616.0;  // 2^64
   double r01 = static_cast<double>(leanstore::utils::RandomGenerator::getRandU64()) * inv_u64;
   return min + r01 * (max - min);
}
}  // namespace randutils