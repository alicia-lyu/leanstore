#include "TPCHWorkload.hpp"

template <template <typename> class AdapterType, class MergedAdapterType>
class BasicJoin {
    TPCHWorkload<AdapterType, MergedAdapterType>& workload;
    
};