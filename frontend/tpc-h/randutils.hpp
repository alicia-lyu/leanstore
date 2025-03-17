#include "leanstore/utils/RandomGenerator.hpp"
#include "../shared/Types.hpp"

namespace randutils {
    inline Integer rnd(Integer n) { return leanstore::utils::RandomGenerator::getRand(0, n); }
    // [low, high]
    inline Integer urand(Integer low, Integer high) { return rnd(high - low + 1) + low; }

    template <int maxLength>
    Varchar<maxLength> randomastring(Integer minLenStr, Integer maxLenStr)
    {
        assert(maxLenStr <= maxLength);
        Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
        Varchar<maxLength> result;
        for (Integer index = 0; index < len; index++) {
            Integer i = rnd(62);
            if (i < 10)
                result.append(48 + i);
            else if (i < 36)
                result.append(64 - 10 + i);
            else
                result.append(96 - 36 + i);
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
        double range = (max - min);
        double div = RAND_MAX / range;
        return min + (leanstore::utils::RandomGenerator::getRandU64() / div);
    }
}