#pragma once


#include <memory>
#include "../variant_utils.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <typename JK, typename JR, typename... Records>
struct LeanStoreMergedScanner
{
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;
   std::unique_ptr<BTreeIt> it;

   bool after_seek = false;
   long long produced = 0;

   LeanStoreMergedScanner(BTree& btree) : it(std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(btree)) { reset(); }

   ~LeanStoreMergedScanner() = default;

   void reset()
   {
      it->reset();
      this->produced = 0;
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> next()
   {
      if (after_seek) {
         after_seek = false;
         return current();
      }
      const leanstore::OP_RESULT res = it->next();
      if (res != leanstore::OP_RESULT::OK) {
         return std::nullopt;
      }
      this->produced++;
      return this->current();
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> prev()
   {
      if (after_seek) {
         after_seek = false;
         return current();
      }
      const leanstore::OP_RESULT res = it->prev();
      if (res != leanstore::OP_RESULT::OK) {
         return std::nullopt;
      }
      return this->current();
   }

   template <typename RecordType>
   void seek(const typename RecordType::Key& k)
      requires std::disjunction_v<std::is_same<RecordType, Records>...>
   // not guaranteed to land on RecordType
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      unsigned pos = RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, pos);
      [[maybe_unused]] const leanstore::OP_RESULT res = it->seek(keySlice);  // keySlice as lowerbound
      if (res != leanstore::OP_RESULT::OK) return; // last key, next will return std::nullopt
      after_seek = true;
   }

   template <typename RecordType>
   void seekForPrev(const typename RecordType::Key& k)
      requires std::disjunction_v<std::is_same<RecordType, Records>...>
   {
      u8 keyBuffer[RecordType::maxFoldLength()];
      unsigned pos = RecordType::foldKey(keyBuffer, k);
      leanstore::Slice keySlice(keyBuffer, pos);
      const leanstore::OP_RESULT res = it->seekForPrev(keySlice);
      if (res != leanstore::OP_RESULT::OK) {
         it->reset();  // next() will return first key
         return;
      }
      after_seek = true;
   }

   template <typename RecordType>
   bool seekTyped(const typename RecordType::Key& k)
      requires std::disjunction_v<std::is_same<RecordType, Records>...>
   {
      seekForPrev<RecordType>(k);
      while (true) {
         auto kv = current().value();
         if (std::holds_alternative<RecordType>(kv.second)) {
            after_seek = true;
            return true;
         }
         leanstore::OP_RESULT ret = it->next();
         if (ret != leanstore::OP_RESULT::OK) {
            // std::cerr << "seekTyped: " << k << " returns " << (int) ret << std::endl;
            return false;
         }
      }
   }

   void seekJK(const JK& jk)
   {
      u8 keyBuffer[JK::maxFoldLength()];
      unsigned pos = JK::keyfold(keyBuffer, jk);
      leanstore::Slice keySlice(keyBuffer, pos);
      [[maybe_unused]] const leanstore::OP_RESULT res = it->seek(keySlice);
      if (res != leanstore::OP_RESULT::OK) return; // last key, next will return std::nullopt
      after_seek = true;
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> current()
   {
      if (it->cur == -1) {
         return std::nullopt;
      }
      it->assembleKey();
      leanstore::Slice key = it->key();
      leanstore::Slice payload = it->value();
      return toType<Records...>(key, payload);
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> last_in_page()
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

   int go_to_last_in_page()
   {
      assert(it->leaf->count > 0);
      
      u16 bytes_advanced = it->leaf->getOffsetDiff(it->cur, it->leaf->count - 1);
      it->cur = it->leaf->count - 1;
      return static_cast<int>(bytes_advanced);
   }

   // void scanJoin(std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
   // {
   //    reset();
   //    PremergedJoin<LeanStoreMergedScanner, JK, JR, Records...> joiner(*this, consume_joined);
   //    joiner.run();
   // }

   // std::tuple<JK, long> next_jk(std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
   // {
   //    PremergedJoin<LeanStoreMergedScanner, JK, JR, Records...> joiner(*this, consume_joined);
   //    joiner.next_jk();
   //    return std::make_tuple(joiner.current_jk, joiner.produced);
   // }
};