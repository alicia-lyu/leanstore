#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include "RocksDBAdapter.hpp"
#include "Scanner.hpp"
#include "Units.hpp"

using ROCKSDB_NAMESPACE::ColumnFamilyHandle;

template <class Record>
class RocksDBScanner : public Scanner<Record>
{
   ColumnFamilyHandle* cf_handle;  // adapter's lifespan must cover scanner's
   std::unique_ptr<rocksdb::Iterator> it;
   bool afterSeek = false;
   long long produced = 0;

   template <typename T>
   rocksdb::Slice RSlice(T* ptr, u64 len)
   {
      return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
   }

  public:
   using Base = Scanner<Record>;

   RocksDBScanner(RocksDBAdapter<Record>& adapter)
       : cf_handle(adapter.cf_handle), it(std::unique_ptr<rocksdb::Iterator>(adapter.map.tx_db->NewIterator(adapter.map.iterator_ro, cf_handle)))
   {
   }
   ~RocksDBScanner() = default;

   void reset() override
   {
      it->SeekToFirst();
      afterSeek = false;
      produced = 0;
   }

   bool seek(const typename Record::Key key) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->Seek(RSlice(folded_key, folded_key_len));
      afterSeek = true;
      return it->Valid();
   }

   void seekForPrev(const typename Record::Key key) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->SeekForPrev(RSlice(folded_key, folded_key_len));
      afterSeek = true;
   }

   template <typename JK>
   bool seek(const JK& key)
   {
      u8 folded_key[JK::maxFoldLength()];
      u16 folded_key_len = JK::keyfold(folded_key, key);
      it->Seek(RSlice(folded_key, folded_key_len));
      afterSeek = true;
      return it->Valid();
   }

   std::optional<std::pair<typename Record::Key, Record>> current() override
   {
      if (!it->Valid()) {
         return std::nullopt;
      }
      typename Record::Key key;
      Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data()), key);
      const Record& record = *reinterpret_cast<const Record*>(it->value().data());
      return std::make_pair(key, record);
   }

   std::optional<std::pair<typename Record::Key, Record>> next() override
   {
      if (afterSeek) {
         afterSeek = false;
         return current();
      }
      it->Next();
      if (!it->Valid()) {
         return std::nullopt;
      }
      return current();
   }

   std::optional<std::pair<typename Record::Key, Record>> prev() override
   {
      if (afterSeek) {
         afterSeek = false;
         return current();
      }
      it->Prev();
      if (!it->Valid()) {
         return std::nullopt;
      }
      return current();
   }
};