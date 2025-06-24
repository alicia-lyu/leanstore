#pragma once

#include "../tpc-h/merge.hpp"
#include "RocksDB.hpp"
#include "variant_utils.hpp"

template <typename JK, typename JR, typename... Records>
struct RocksDBMergedScanner {
   std::unique_ptr<rocksdb::Iterator> it;
   bool after_seek = false;
   long long produced = 0;

   RocksDBMergedScanner(ColumnFamilyHandle* cf_handle, RocksDB& map)
       : it(map.tx_db->NewIterator(map.iterator_ro, cf_handle))
   {
      it->SeekToFirst();
      after_seek = true;
   }

   void reset() 
   {
      it->SeekToFirst();
      after_seek = true;
      this->produced = 0;
   }

   template <typename Record>
   void seek(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->Seek(RSlice(folded_key, folded_key_len));
      after_seek = true;
   }

   template <typename Record>
   void seekForPrev(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->SeekForPrev(RSlice(folded_key, folded_key_len));
      after_seek = true;
   }

   template <typename Record>
   bool seekTyped(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      seekForPrev<Record>(key);
      while (true) {
         if (!it->Valid()) {
            return false;
         }
         auto kv = current().value();
         if (std::holds_alternative<Record>(kv.second)) {
            after_seek = true;
            return true;
         }
         it->Next();
         if (!it->Valid()) {
            return false;
         }
      }
   }

   void seekJK(const JK& jk) 
   {
      u8 folded_jk[JK::maxFoldLength()];
      u16 folded_jk_len = JK::keyfold(folded_jk, jk);
      it->Seek(RSlice(folded_jk, folded_jk_len));
      after_seek = true;
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> next() 
   {
      if (after_seek) {
         after_seek = false;
         return current();  // std::nullopt if !Valid
      }
      it->Next();
      this->produced++;
      return this->current();  // std::nullopt if !Valid
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> prev()
   {
      if (after_seek) {
         after_seek = false;
         return current();
      }
      it->Prev();
      this->produced++;
      return this->current();  // std::nullopt if !Valid
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> current() 
   {
      if (!it->Valid()) {
         return std::nullopt;
      }
      auto [key, rec] = toType<Records...>(it->key(), it->value());
      return std::make_pair(key, rec);
   }

   void scanJoin(std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
   {
      reset();
      PremergedJoin<RocksDBMergedScanner, JK, JR, Records...> joiner(*this, consume_joined);
      joiner.run();
   }

   std::tuple<JK, long> next_jk(std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {
   })
   {
      PremergedJoin<RocksDBMergedScanner, JK, JR, Records...> joiner(*this, consume_joined);
      joiner.next_jk();
      return std::make_tuple(joiner.current_jk, joiner.produced);
   }
};