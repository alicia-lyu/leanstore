#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include "RocksDB.hpp"
#include "Units.hpp"

using ROCKSDB_NAMESPACE::ColumnFamilyHandle;

template <class Record>
class RocksDBScanner
{
   std::unique_ptr<rocksdb::Iterator> it;

  public:
   bool after_seek = false;
   long long produced = 0;

   RocksDBScanner(ColumnFamilyHandle* cf_handle, RocksDB& map) : it(map.tx_db->NewIterator(map.iterator_ro, cf_handle))
   {
      it->SeekToFirst();
      after_seek = true;
      assert(it->Valid());
   }
   ~RocksDBScanner() = default;

   void reset()
   {
      it->SeekToFirst();
      after_seek = true;
      produced = 0;
   }

   bool seek(const typename Record::Key key)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->Seek(RSlice(folded_key, folded_key_len));
      after_seek = true;
      return it->Valid();
   }

   void seekForPrev(const typename Record::Key key)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      it->SeekForPrev(RSlice(folded_key, folded_key_len));
      after_seek = true;
   }

   template <typename JK>
   bool seek(const JK& key)
   {
      u8 folded_key[JK::maxFoldLength()];
      u16 folded_key_len = JK::keyfold(folded_key, key);
      it->Seek(RSlice(folded_key, folded_key_len));
      after_seek = true;
      return it->Valid();
   }

   std::optional<std::pair<typename Record::Key, Record>> current()
   {
      if (!it->Valid()) {
         return std::nullopt;
      }
      typename Record::Key key;
      Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data()), key);
      const Record& record = *reinterpret_cast<const Record*>(it->value().data());
      return std::make_pair(key, record);
   }

   std::optional<std::pair<typename Record::Key, Record>> next()
   {
      if (after_seek) {
         after_seek = false;
      } else {
         it->Next();
         produced++;
      }
      return current();
   }

   std::optional<std::pair<typename Record::Key, Record>> prev()
   {
      if (after_seek) {
         after_seek = false;
      } else {
         it->Prev();
      }
      return current();
   }
};