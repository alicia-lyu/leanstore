#pragma once

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
   std::conditional<std::is_same<Record, PayloadType>::value, bool, BTreeIt> payloadProvider = false;
   bool afterSeek = false;

  public:
   // Template argument PayloadType = Record
   LeanStoreScanner(BTree& btree)
       : Base([&]() -> std::optional<pair_t> {
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
         it(btree)
   {}

   LeanStoreScanner(BTree& btree, BTreeIt& payloadProvider)
   : Base([&]() -> std::optional<pair_t> {
         it.assembleKey();
         leanstore::Slice key = it.key();
         // Parse secondary key and get the primary key
         typename Record::Key typed_key;
         Record::unfoldKey(key.data(), typed_key);
         u8 primaryKeyBuffer[PayloadType::maxFoldLength()];
         Record::foldPKey(primaryKeyBuffer, typed_key);
         leanstore::Slice primaryKey(primaryKeyBuffer, PayloadType::maxFoldLength());
         // Search in primary index
         auto res1 = payloadProvider.seekExact(primaryKey);
         if (res1 != leanstore::OP_RESULT::OK)
            return std::nullopt;
         payloadProvider.assembleKey();
         leanstore::Slice payload = payloadProvider.value();
         assert(payload.length() == sizeof(PayloadType));
         const PayloadType* record_ptr = reinterpret_cast<const PayloadType*>(payload.data());
         PayloadType typed_payload = *record_ptr;

         return std::optional<pair_t>({typed_key, typed_payload});
     }),
     it(btree), payloadProvider(payloadProvider)
   {}

   virtual ~LeanStoreScanner() {}

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