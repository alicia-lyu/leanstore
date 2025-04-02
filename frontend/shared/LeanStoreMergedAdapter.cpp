#include "LeanStoreMergedAdapter.hpp"
#include "LeanStoreMergedScanner.hpp"

LeanStoreMergedScanner LeanStoreMergedAdapter::getScanner() {
    if (FLAGS_vi) {
        return LeanStoreMergedScanner(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(btree)));
     } else {
        return LeanStoreMergedScanner(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(btree)));
     }
};