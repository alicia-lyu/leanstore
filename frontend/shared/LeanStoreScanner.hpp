#pragma once

#include <concepts>
#include <optional>
#include "LeanStoreAdapter.hpp"
#include "Scanner.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <class Record, class PayloadType = Record>
class LeanStoreScanner : public Scanner<Record, PayloadType>
{
  protected:
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;
   using Base = Scanner<Record, PayloadType>;

   using pair_t = typename Base::pair_t;

   std::unique_ptr<BTreeIt> it;
   BTree* payloadTree;
   std::function<std::optional<pair_t>()> assemble = [this]() -> std::optional<pair_t> {
      it->assembleKey();
      leanstore::Slice key = it->key();
      leanstore::Slice payload = it->value();

      Record typed_payload = *reinterpret_cast<const Record*>(payload.data());

      typename Record::Key typed_key;
      Record::unfoldKey(key.data(), typed_key);
      return std::make_optional<pair_t>({typed_key, typed_payload});
   };

  public:
   LeanStoreScanner(BTree& btree)
      requires std::same_as<PayloadType, Record>
       : Base(assemble), it(std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(btree)), payloadTree(nullptr)
   {
      reset();
   }

   LeanStoreScanner(BTree& btree, BTree& payloadProvider)
      requires(!std::same_as<PayloadType, Record>)
       : Base(assemble), it(std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(btree)), payloadTree(&payloadProvider)
   {
      reset();
   }

   void reset()
   {
      it->reset();
      this->produced = 0;
   }

   bool seek(const typename Record::Key& key) { return seek<Record>(key); }

   template <typename RecordType>
   bool seek(typename RecordType::Key k)
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, RecordType::maxFoldLength());
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

   std::optional<pair_t> next()
   {
      leanstore::OP_RESULT res = it->next();
      if (res != leanstore::OP_RESULT::OK)
         return std::nullopt;
      this->produced++;
      return Base::assemble();
   }

   std::optional<pair_t> current()
   {
      if (it->cur == -1) {
         return std::nullopt;
      } else {
         return Base::assemble();
      }
   }
};