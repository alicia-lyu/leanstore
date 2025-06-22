#pragma once

#include "../tpc-h/merge.hpp"
#include "MergedScanner.hpp"
#include "RocksDBMergedAdapter.hpp"

template <typename JK, typename JR, typename... Records>
struct RocksDBMergedScanner : public MergedScanner<JK, JR, Records...> {
   ColumnFamilyHandle* cf_handle;  // adapter's lifespan must cover scanner's
   std::unique_ptr<rocksdb::Iterator> it;
   bool afterSeek = false;
   long long produced = 0;

   RocksDBMergedScanner(RocksDBMergedAdapter<Records...>& adapter)
       : cf_handle(adapter.cf_handle.get()),
         it(std::unique_ptr<rocksdb::Iterator>(adapter.map.tx_db->NewIterator(adapter.map.iterator_ro, cf_handle)))
   {
   }

   void reset() override
   {
      it->SeekToFirst();
      afterSeek = true;
      this->produced = 0;
   }

   template <typename Record>
   void seek(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->Seek(RSlice(folded_key, folded_key_len));
      afterSeek = true;
   }

   template <typename Record>
   void seekForPrev(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->SeekForPrev(RSlice(folded_key, folded_key_len));
      afterSeek = true;
   }

   template <typename Record>
   bool seekTyped(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      seekForPrev<Record>(key);
      while (true) {
         auto kv = current().value();
         if (std::holds_alternative<Record>(kv.second)) {
            afterSeek = true;
            return true;
         }
         it->Next();
         if (!it->Valid()) {
            return false;
         }
      }
   }

   void seekJK(const JK& jk) override
   {
      u8 folded_jk[JK::maxFoldLength()];
      u16 folded_jk_len = JK::keyfold(folded_jk, jk);
      it->Seek(RSlice(folded_jk, folded_jk_len));
      afterSeek = true;
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> next() override
   {
      if (afterSeek) {
         afterSeek = false;
         return current();  // std::nullopt if !Valid
      }
      it->Next();
      this->produced++;
      return this->current();  // std::nullopt if !Valid
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> prev() override
   {
      if (afterSeek) {
         afterSeek = false;
         return current();
      }
      it->Prev();
      this->produced++;
      return this->current();  // std::nullopt if !Valid
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> current() override
   {
      if (!it->Valid()) {
         return std::nullopt;
      }
      auto [key, rec] = RocksDBMergedAdapter<Records...>::toType(it->key(), it->value());
      return std::make_pair(key, rec);
   }

   void scanJoin(std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {}) override
   {
      reset();
      PremergedJoin<RocksDBMergedScanner, JK, JR, Records...> joiner(*this, consume_joined);
      joiner.run();
   }

   std::tuple<JK, long> next_jk(std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {
   }) override
   {
      PremergedJoin<RocksDBMergedScanner, JK, JR, Records...> joiner(*this, consume_joined);
      joiner.next_jk();
      return std::make_tuple(joiner.current_jk, joiner.produced);
   }
};