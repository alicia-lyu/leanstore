#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include "Scanner.hpp"
#include "Units.hpp"
#include "RocksDB.hpp"

template <class Record>
class RocksDBScanner: public Scanner<Record>
{
   rocksdb::Iterator* it;
   bool afterSeek = false;

   template <typename T>
   rocksdb::Slice RSlice(T* ptr, u64 len)
   {
      return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
   }

  public:
   using SEP = u32;
   using next_ret_t = typename Scanner<Record>::next_ret_t;

   RocksDBScanner(RocksDB& map) : it(map.db->NewIterator(map.ro)) {}

   virtual bool seek(typename Record::Key key)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      it->Seek(RSlice(folded_key, folded_key_len));
      afterSeek = true;
      return it->Valid();
   }

   virtual std::optional<next_ret_t> next()
   {
      if (!afterSeek) {
         it->Next();
      } else {
         afterSeek = false;
      }

      if (!it->Valid()) {
         return std::nullopt;
      }

      typename Record::Key s_key;
      Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
      const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());

      return std::optional<next_ret_t>({s_key, s_value});
   }
};