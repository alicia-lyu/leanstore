#pragma once

#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <class Record>
class Scanner
{
  protected:
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;
   BTreeIt it;
   bool afterSeek = false;

  public:
   struct next_ret_t {
      typename Record::Key key;
      Record record;
   };

   Scanner(BTree& btree) : it(btree) {}
   virtual ~Scanner() {}

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

   virtual std::optional<next_ret_t> next()
   {
      if (!afterSeek)
      {
         leanstore::OP_RESULT res = it.next();
         if (res != leanstore::OP_RESULT::OK)
            return std::nullopt;
      } else {
         afterSeek = false;
      }
      it.assembleKey();
      leanstore::Slice key = it.key();
      leanstore::Slice payload = it.value();

      assert(payload.length() == sizeof(Record));
      const Record* record_ptr = reinterpret_cast<const Record*>(payload.data());
      Record typed_payload = *record_ptr;

      typename Record::Key typed_key;
      Record::unfoldKey(key.data(), typed_key);
      return std::optional<next_ret_t>({typed_key, typed_payload});
   }
};

template <class Record1, class Record2>
class ScannerSec : public Scanner<Record1>
{
   using Base = Scanner<Record1>;
   using typename Base::BTree;
   using typename Base::BTreeIt;
   using typename Base::next_ret_t;

   BTreeIt sec_it;

  public:
   ScannerSec(BTree& btree, BTree& sec_btree) : Base(btree), sec_it(sec_btree) {}
   virtual ~ScannerSec() {}

   virtual bool seek(typename Record2::Key typed_key)
   {
      u8 keyBuffer[Record2::maxFoldLength()];
      Record2::foldKey(keyBuffer, typed_key);
      leanstore::Slice key(keyBuffer, Record2::maxFoldLength());
      leanstore::OP_RESULT res = sec_it.seek(key);
      if (res == leanstore::OP_RESULT::OK)
         this->afterSeek = true;
      return res == leanstore::OP_RESULT::OK;
   }

   virtual std::optional<next_ret_t> next()
   {
      // Guided by secondary index
      if (!this->afterSeek)
      {
         leanstore::OP_RESULT res = sec_it.next();
         if (res != leanstore::OP_RESULT::OK)
            return std::nullopt;
      } else {
         this->afterSeek = false;
      }
      sec_it.assembleKey();
      leanstore::Slice sec_key = sec_it.key();

      // Parse secondary key and get the primary key
      typename Record2::Key sec_typed_key;
      Record2::unfoldKey(sec_key.data(), sec_typed_key);
      u8 primaryKeyBuffer[Record1::maxFoldLength()];
      Record2::foldPKey(primaryKeyBuffer, sec_typed_key);
      leanstore::Slice primaryKey(primaryKeyBuffer, Record1::maxFoldLength());
      // Search in primary index
      auto res1 = this->it.seekExact(primaryKey);
      if (res1 != leanstore::OP_RESULT::OK)
         return std::nullopt;
      this->it.assembleKey();

      leanstore::Slice key = this->it.key();
      assert(key == primaryKey);
      leanstore::Slice payload = this->it.value();

      assert(payload.length() == sizeof(Record1));
      const Record1* record_ptr = reinterpret_cast<const Record1*>(payload.data());
      Record1 typed_payload = *record_ptr;

      typename Record1::Key typed_key;
      Record1::unfoldKey(key.data(), typed_key);
      return std::optional<next_ret_t>({typed_key, typed_payload});
   }
};