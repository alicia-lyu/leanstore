#pragma once

#include <sys/types.h>
#include <optional>
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <class Record>
struct LeanStoreScanner {
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;

   std::unique_ptr<BTreeIt> it;
   long long produced = 0;
   bool after_seek = false;

   LeanStoreScanner(BTree& btree) : it(std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(btree)) { reset(); }

   ~LeanStoreScanner() = default;

   void reset()
   {
      it->reset();
      this->produced = 0;
   }

   bool seek(const typename Record::Key& key) { return seek<Record>(key); }

   bool seekForPrev(const typename Record::Key& key) { return seekForPrev<Record>(key); }

   template <typename RecordType>
   bool seek(const typename RecordType::Key& k)
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      unsigned pos = RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, pos);
      [[maybe_unused]] const leanstore::OP_RESULT res = it->seek(keySlice);
      if (res != leanstore::OP_RESULT::OK)
         return false;  // last key, next will return std::nullopt
      after_seek = true;
      return true;
   }

   template <typename RecordType>
   bool seekForPrev(const typename RecordType::Key& k)
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      unsigned pos = RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, pos);
      const leanstore::OP_RESULT res = it->seekForPrev(keySlice);
      if (res != leanstore::OP_RESULT::OK) {
         it->reset();  // next() will return first key
         return false;
      }
      after_seek = true;
      return true;
   }

   template <typename JK>
   bool seek(const JK& jk)
   {
      u8 keyBuffer[JK::maxFoldLength()];
      unsigned pos = JK::keyfold(keyBuffer, jk);
      leanstore::Slice keySlice(keyBuffer, pos);
      [[maybe_unused]] const leanstore::OP_RESULT res = it->seek(keySlice);
      if (res != leanstore::OP_RESULT::OK)
         return false;  // last key, next will return std::nullopt
      after_seek = true;
      return true;
   }

   std::optional<std::pair<typename Record::Key, Record>> next()
   {
      this->produced++;
      if (after_seek) {
         after_seek = false;
         return this->current();
      }
      leanstore::OP_RESULT res = it->next();
      if (res != leanstore::OP_RESULT::OK)
         return std::nullopt;
      return this->current();
   }

   std::optional<std::pair<typename Record::Key, Record>> prev()
   {
      if (after_seek) {
         after_seek = false;
         return this->current();
      }
      leanstore::OP_RESULT res = it->prev();
      if (res != leanstore::OP_RESULT::OK)
         return std::nullopt;
      return this->current();
   }

   std::optional<std::pair<typename Record::Key, Record>> current()
   {
      if (it->cur == -1) {
         return std::nullopt;
      }
      it->assembleKey();
      leanstore::Slice key = it->key();
      leanstore::Slice payload = it->value();

      Record typed_payload = *reinterpret_cast<const Record*>(payload.data());

      typename Record::Key typed_key;
      Record::unfoldKey(key.data(), typed_key);
      return std::make_pair(typed_key, typed_payload);
   }

   std::optional<std::pair<typename Record::Key, Record>> last_in_page()
   {
      if (it->leaf->count > 0) {
         auto prev_cur = it->cur;
         it->cur = it->leaf->count - 1;
         auto kv = current();
         it->cur = prev_cur; // restore the cursor
         return kv;
      } else {
         return std::nullopt; // no records in the page
      }
   }
};