#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include "../RocksDB.hpp"
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

   RocksDBScanner(ColumnFamilyHandle* cf_handle, RocksDB& map) : map(map), it(map.tx_db->NewIterator(map.iterator_ro, cf_handle)) { seek_to_first(); }
   ~RocksDBScanner() = default;

   void seek_to_first()
   {
      std::string key_buf(1, 0);
      const unsigned pos = fold(reinterpret_cast<u8*>(key_buf.data()), static_cast<u8>(Record::id));
      rocksdb::Slice k_slice(key_buf.data(), pos);
      it->Seek(k_slice);
      after_seek = true;
   }

   void reset()
   {
      seek_to_first();
      produced = 0;
   }

   bool seek(const typename Record::Key key)
   {
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf, true);
      it->Seek(k_slice);
      after_seek = true;
      return it->Valid();  // may well be false but Next() will validate it
   }

   void seekForPrev(const typename Record::Key key)
   {
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf, true);
      it->SeekForPrev(k_slice);
      after_seek = true;
   }

   template <typename JK>
   bool seek(const JK& key)
   {
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf, true);
      it->Seek(k_slice);
      after_seek = true;
      return it->Valid();
   }

   std::optional<std::pair<typename Record::Key, Record>> current()
   {
      if (!it->Valid()) {
         return std::nullopt;
      }
      u8 id;
      const u8* key_data = reinterpret_cast<const u8*>(it->key().data());
      unsigned pos = unfold(key_data, id);
      if(id != static_cast<u8>(Record::id)) { // passed the record type
         assert(id > static_cast<u8>(Record::id));
         return std::nullopt;
      }
      typename Record::Key key;
      Record::unfoldKey(key_data + pos, key);
      const Record record = *reinterpret_cast<const Record*>(it->value().data());
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