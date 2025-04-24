#pragma once

#include <cstdint>
#include <variant>
#include "../tpc-h/merge.hpp"
#include "MergedScanner.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <typename... Records>
class LeanStoreMergedScanner : public MergedScanner<Records...>
{
  private:
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;
   std::unique_ptr<BTreeIt> it;

   static std::pair<std::variant<typename Records::Key...>, std::variant<Records...>> toType(leanstore::Slice& k, leanstore::Slice& v)
   {
      bool matched = false;
      std::variant<typename Records::Key...> result_key;
      std::variant<Records...> result_rec;

      (([&]() {
          if (!matched && k.size() == Records::maxFoldLength() && v.size() == sizeof(Records)) {
             typename Records::Key key;
             Records::unfoldKey(k.data(), key);
             const Records& rec = *reinterpret_cast<const Records*>(v.data());
             matched = true;
             result_key = key;
             result_rec = rec;
          }
       })(),
       ...);
      assert(matched);
      return std::make_pair(result_key, result_rec);
   }

  public:
   uint64_t produced = 0;

   LeanStoreMergedScanner(BTree& btree) : it(std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(btree)) { reset(); }

   ~LeanStoreMergedScanner() = default;

   void reset()
   {
      it->reset();
      produced = 0;
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> next()
   {
      const leanstore::OP_RESULT res = it->next();
      if (res != leanstore::OP_RESULT::OK) {
         return std::nullopt;
      }
      produced++;
      return this->current();
   }

   template <typename RecordType>
   bool seek(const typename RecordType::Key& k)
      requires std::disjunction_v<std::is_same<RecordType, Records>...>
   // not guaranteed to land on RecordType
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      unsigned pos = RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, pos);
      const leanstore::OP_RESULT res = it->seek(keySlice);  // keySlice as lowerbound
      if (res != leanstore::OP_RESULT::OK) {
         it->seekForPrev(keySlice);  // last key
         return false;
      } else {
         return true;
      }
   }

   template <typename RecordType>
   bool seekForPrev(const typename RecordType::Key& k)
      requires std::disjunction_v<std::is_same<RecordType, Records>...>
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      unsigned pos = RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, pos);
      const leanstore::OP_RESULT res = it->seekForPrev(keySlice);
      if (res != leanstore::OP_RESULT::OK) {
         it->seek(keySlice);  // first key
         return false;
      } else {
         return true;
      }
   }

   template <typename RecordType>
   bool seekTyped(const typename RecordType::Key& k)
      requires std::disjunction_v<std::is_same<RecordType, Records>...>
   {
      [[maybe_unused]] auto result = seekForPrev<RecordType>(k);
      while (true) {
         auto kv = current().value();
         if (std::holds_alternative<RecordType>(kv.second)) {
            return true;
         }
         leanstore::OP_RESULT ret = it->next();
         if (ret != leanstore::OP_RESULT::OK) {
            reset();
            std::cerr << "seekTyped: " << k << " returns " << (int) ret << std::endl;
            return false;
         }
      }
   }

   template <typename JK>
   bool seekJK(const JK& jk)
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

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> current()
   {
      if (it->cur == -1) {
         return std::nullopt;
      }
      it->assembleKey();
      leanstore::Slice key = it->key();
      leanstore::Slice payload = it->value();
      return toType(key, payload);
   }

   template <typename JK, typename JoinedRec>
   void scanJoin(std::function<void(const typename JoinedRec::Key&, const JoinedRec&)> consume_joined = [](const typename JoinedRec::Key&, const JoinedRec&) {})
   {
      reset();
      PremergedJoin<JK, JoinedRec, Records...> joiner(*this, consume_joined);
      joiner.run();
   } 
};