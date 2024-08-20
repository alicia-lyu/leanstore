#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include "RocksDB.hpp"
#include "Scanner.hpp"
#include "Units.hpp"

template <class Record, class PayloadType = Record>
class RocksDBScanner : public Scanner<Record, PayloadType>
{
   std::unique_ptr<rocksdb::Iterator> it;
   std::unique_ptr<rocksdb::Iterator> payloadIt;
   bool afterSeek = false;

   template <typename T>
   rocksdb::Slice RSlice(T* ptr, u64 len)
   {
      return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
   }

   template <class T>
   uint32_t getId(const T& str)
   {
      return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(str.data())) ^ (1ul << 31);
   }

  public:
   using SEP = u32;
   using Base = Scanner<Record, PayloadType>;
   using pair_t = typename Base::pair_t;

   RocksDBScanner(RocksDB& map) requires std::same_as<PayloadType, Record>
       : Base([this]() -> std::optional<pair_t> {
            if (!it->Valid() || getId(it->key()) != Record::id) {
               return std::nullopt;
            }

            typename Record::Key s_key;
            Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
            const Record* s_value = reinterpret_cast<const Record*>(it->value().data());
            Record s_value_copy = *s_value;

            return std::make_optional<pair_t>(s_key, s_value_copy);
         }),
         it(map.db->NewIterator(map.ro)), payloadIt(nullptr)
   {}

   RocksDBScanner(RocksDB& map) requires (!std::same_as<PayloadType, Record>)
   : Base([this]() -> std::optional<pair_t> {
         if (!it->Valid() || getId(it->key()) != Record::id) {
            return std::nullopt;
         }

         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);

         u8 primaryKeyBuffer[PayloadType::maxFoldLength() + sizeof(SEP)];
         const u32 folded_key_len = fold(primaryKeyBuffer, PayloadType::id) + Record::foldPKey(primaryKeyBuffer + sizeof(SEP), s_key);
         auto p_key = RSlice(primaryKeyBuffer, folded_key_len);

         payloadIt->Seek(p_key);
         
         if (!payloadIt->Valid()) {
            return std::nullopt;
         }

         if (payloadIt->key().compare(p_key) != 0) {
            throw std::runtime_error("RocksDBScanner: Secondary index cannot find primary key");
         }

         PayloadType s_value_copy = *reinterpret_cast<const PayloadType*>(payloadIt->value().data());

         return std::make_optional<pair_t>(s_key, s_value_copy);
      }),
      it(map.db->NewIterator(map.ro)), payloadIt(map.db->NewIterator(map.ro))
   {}

   virtual bool seek(typename Record::Key key)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      it->Seek(RSlice(folded_key, folded_key_len));
      afterSeek = true;
      return it->Valid();
   }

   virtual std::optional<pair_t> next()
   {
      if (!afterSeek) {
         it->Next();
      } else {
         afterSeek = false;
      }
      return Base::assemble();
   }
};