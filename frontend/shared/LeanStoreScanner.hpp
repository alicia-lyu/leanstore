#pragma once

#include <optional>
#include <concepts>
#include "Scanner.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <class Record, class PayloadType = Record>
class LeanStoreScanner : public Scanner<Record, PayloadType>
{
  protected:
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;
   using Base = Scanner<Record, PayloadType>;

   using pair_t = typename Base::pair_t;

   BTreeIt it;
   std::optional<BTreeIt> payloadIt;
   bool afterSeek = false;

  public:
   LeanStoreScanner(BTree& btree) requires std::same_as<PayloadType, Record>
       : Base([this]() -> std::optional<pair_t> {
            it.assembleKey();
            leanstore::Slice key = it.key();
            leanstore::Slice payload = it.value();

            assert(payload.length() == sizeof(Record));
            const Record* record_ptr = reinterpret_cast<const Record*>(payload.data());
            Record typed_payload = *record_ptr;

            typename Record::Key typed_key;
            Record::unfoldKey(key.data(), typed_key);
            return std::optional<pair_t>({typed_key, typed_payload});
         }),
         it(btree), payloadIt(std::nullopt)
   {}

   LeanStoreScanner(BTree& btree, BTree& payloadProvider) requires (!std::same_as<PayloadType, Record>)
   : Base([this]() -> std::optional<pair_t> {
         it.assembleKey();
         leanstore::Slice key = it.key();
         // Parse secondary key and get the primary key
         typename Record::Key typed_key;
         Record::unfoldKey(key.data(), typed_key);
         u8 primaryKeyBuffer[PayloadType::maxFoldLength()];
         Record::foldPKey(primaryKeyBuffer, typed_key);
         leanstore::Slice primaryKey(primaryKeyBuffer, PayloadType::maxFoldLength());
         // Search in primary index
         auto res1 = payloadIt->seekExact(primaryKey);
         if (res1 != leanstore::OP_RESULT::OK)
            return std::nullopt;
         payloadIt->assembleKey();
         leanstore::Slice payload = payloadIt->value();
         PayloadType typed_payload = *reinterpret_cast<const PayloadType*>(payload.data());

         return std::optional<pair_t>({typed_key, typed_payload});
     }),
     it(btree), payloadIt(std::make_optional<BTreeIt>(payloadProvider))
   {}

   virtual bool seek(typename Record::Key key)
   {
      u8 keyBuffer[Record::maxFoldLength()];
      Record::foldKey(keyBuffer, key);
      leanstore::Slice keySlice(keyBuffer, Record::maxFoldLength());
      leanstore::OP_RESULT res = it.seek(keySlice);
      if (res == leanstore::OP_RESULT::OK)
         afterSeek = true;
      return res == leanstore::OP_RESULT::OK;
   }

   virtual std::optional<pair_t> next()
   {
      if (!afterSeek) {
         leanstore::OP_RESULT res = it.next();
         if (res != leanstore::OP_RESULT::OK)
            return std::nullopt;
      } else {
         afterSeek = false;
      }
      return Base::assemble();
   }
};