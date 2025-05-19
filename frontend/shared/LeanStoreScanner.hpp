#pragma once

#include <sys/types.h>
#include <optional>
#include "Scanner.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <class Record>
class LeanStoreScanner : public Scanner<Record>
{
  protected:
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;

   std::unique_ptr<BTreeIt> it;
   uint64_t produced = 0;

  public:
   LeanStoreScanner(BTree& btree) : it(std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(btree)) { reset(); }

   ~LeanStoreScanner() = default;

   void reset()
   {
      it->reset();
      this->produced = 0;
   }

   void seek(const typename Record::Key& key) { return seek<Record>(key); }

   void seekForPrev(const typename Record::Key& key)
   {
      return seekForPrev<Record>(key);
   }

   template <typename RecordType>
   void seek(const typename RecordType::Key& k)
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, RecordType::maxFoldLength());
      [[maybe_unused]] const leanstore::OP_RESULT res = it->seek(keySlice);
      // if (res != leanstore::OP_RESULT::OK) // last key, next will return std::nullopt
   }

   template <typename RecordType>
   void seekForPrev(const typename RecordType::Key& k)
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, RecordType::maxFoldLength());
      const leanstore::OP_RESULT res = it->seekForPrev(keySlice);
      if (res != leanstore::OP_RESULT::OK) {
         it->reset();  // next() will return first key
      }
   }
   

   template <typename JK>
   void seek(const JK& jk)
   {
      u8 keyBuffer[JK::maxFoldLength()];
      unsigned pos = JK::keyfold(keyBuffer, jk);
      leanstore::Slice keySlice(keyBuffer, pos);
      [[maybe_unused]] const leanstore::OP_RESULT res = it->seek(keySlice);
   }

   std::optional<std::pair<typename Record::Key, Record>> next()
   {
      leanstore::OP_RESULT res = it->next();
      if (res != leanstore::OP_RESULT::OK)
         return std::nullopt;
      this->produced++;
      return this->current();
   }

   std::optional<std::pair<typename Record::Key, Record>> current()
   {
      if (it->cur == -1)
         return std::nullopt;
      it->assembleKey();
      leanstore::Slice key = it->key();
      leanstore::Slice payload = it->value();

      Record typed_payload = *reinterpret_cast<const Record*>(payload.data());

      typename Record::Key typed_key;
      Record::unfoldKey(key.data(), typed_key);
      return std::make_pair(typed_key, typed_payload);
   }
};