#pragma once

#include <cstdint>
#include <variant>
#include "LeanStoreMergedAdapter.hpp"
#include "MergedScanner.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"
#include "../tpc-h/Merge.hpp"

template <typename JK, typename JoinedRec, typename... Records>
class LeanStoreScannerAdapter : public MergedScannerInterface<JK, Records...> {
public:
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;

  public:
   uint64_t produced = 0;
   std::unique_ptr<BTreeIt> it;

   LeanStoreMergedScanner(BTree& btree) : it(std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(btree)) { reset(); }

   void reset()
   {
      it->reset();
      produced = 0;
   }

   std::optional<std::pair<leanstore::Slice, leanstore::Slice>> next()
   {
      const leanstore::OP_RESULT res = it->next();
      if (res != leanstore::OP_RESULT::OK) {
         return std::nullopt;
      }
      produced++;
      return this->current();
   }

   template <typename RecordType>
   bool seek(typename RecordType::Key k)
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      unsigned pos = RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, pos);
      const leanstore::OP_RESULT res = it->seek(keySlice);
      if (res != leanstore::OP_RESULT::OK) {
         it->seekForPrev(keySlice);  // last key
         return false;
      } else {
         return true;
      }
   }

   template <typename JK>
   bool seek(JK jk)
   {
      u8 keyBuffer[JK::maxFoldLength()];
      unsigned pos = JK::keyfold(keyBuffer, jk);
      leanstore::Slice keySlice(keyBuffer, pos);
      const leanstore::OP_RESULT res = it->seek(keySlice);
      if (res != leanstore::OP_RESULT::OK) {
         it->seekForPrev(keySlice);  // last key
         return false;
      } else {
         return true;
      }
   }

   template <typename... Records>
   std::variant<Records...> current()
   {
      auto [k, v] = current();
      return LeanStoreMergedAdapter::toType<Records...>(k, v);
   }

   std::pair<leanstore::Slice, leanstore::Slice> current()
   {
      it->assembleKey();
      leanstore::Slice key = it->key();
      leanstore::Slice payload = it->value();
      return std::make_pair(key, payload);
   }

   template <typename JK, typename JoinedRec, typename... Records>
   void scanJoin()
   {
      using Merge = MultiWayMerge<JK, JoinedRec, Records...>;
      reset();
      std::vector<std::function<typename Merge::HeapEntry()>> sources = {[&]() {
         auto kv = this->next();
         if (!kv.has_value()) {
            return typename Merge::HeapEntry();
         }

         typename Merge::HeapEntry result;
         int i = 0;

         auto [result_key, result_rec] = LeanStoreMergedAdapter::toType<Records...>(kv->first, kv->second);
         std::visit(
             [&](const auto& k, const auto& v) {
                using T = std::decay_t<decltype(v)>;
                result = typename Merge::HeapEntry(k.jk, T::toBytes(k), T::toBytes(v), i);
                i++;
             },
             result_key, result_rec);
         return result;
      }};
      Merge multiway_merge(sources);
      multiway_merge.run();
      reset();
   }
};