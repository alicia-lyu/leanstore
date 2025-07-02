#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include "RocksDB.hpp"
#include "Units.hpp"

using ROCKSDB_NAMESPACE::ColumnFamilyHandle;

template <class Record>
class RocksDBScanner
{
   RocksDB& map;
   std::unique_ptr<rocksdb::Iterator> it;

  public:
   bool after_seek = false;
   long long produced = 0;

   RocksDBScanner(ColumnFamilyHandle* cf_handle, RocksDB& map) : map(map), it(map.tx_db->NewIterator(map.iterator_ro, cf_handle))
   {
      it->SeekToFirst();
      after_seek = true;
      // assert(it->Valid());
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
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf);
      it->Seek(k_slice);
      after_seek = true;
      return it->Valid();  // may well be false but Next() will validate it
   }

   void seekForPrev(const typename Record::Key key)
   {
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf);
      it->SeekForPrev(k_slice);
      after_seek = true;
   }

   template <typename JK>
   bool seek(const JK& key)
   {
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf);
      it->Seek(k_slice);
      after_seek = true;
      return it->Valid();
   }

   std::optional<std::pair<typename Record::Key, Record>> current()
   {
      if (!it->Valid()) {
         return std::nullopt;
      }
      rocksdb::Slice k = it->key();
      rocksdb::Slice v = it->value();
      typename Record::Key key;
      Record::unfoldKey(reinterpret_cast<const u8*>(k.data()), key);
      const Record record = *reinterpret_cast<const Record*>(v.data());
      return std::make_pair(key, record);
   }

   std::optional<std::pair<typename Record::Key, Record>> next()
   {
      if (after_seek) {
         after_seek = false;
         // if (!it->Valid() && it->status().ok()) {  // may be invalid after seek
         //    it->Next();
         // }
      } else {
         // invalid not after seek is not allowed
         if (!it->Valid()) {
            return std::nullopt;
         }
         it->Next();
         produced++;
      }
      return current();
   }

   std::optional<std::pair<typename Record::Key, Record>> prev()
   {
      if (after_seek) {
         after_seek = false;
         // if (!it->Valid() && it->status().ok()) {  // may be invalid after seek
         //    it->Prev();
         // }
      } else {
         if (!it->Valid()) {
            return std::nullopt;
         }
         it->Prev();
      }
      return current();
   }
};