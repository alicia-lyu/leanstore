#pragma once

#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"

template <class Record>
class Scanner
{
  protected:
   using BTreeIt = leanstore::storage::btree::BTreeSharedIterator;
   using BTree = leanstore::storage::btree::BTreeGeneric;
   BTreeIt it;

  public:
   struct next_ret_t {
      typename Record::Key key;
      Record record;
   };

   Scanner(BTree& btree) : it(btree) {}

   virtual std::optional<next_ret_t> next()
   {
      leanstore::OP_RESULT res = it.next();
      if (res != leanstore::OP_RESULT::OK)
         return std::nullopt;
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

   std::optional<next_ret_t> next()
   {
      // Guided by secondary index
      leanstore::OP_RESULT res2 = sec_it.next();
      if (res2 != leanstore::OP_RESULT::OK)
         return std::nullopt;
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

      assert(payload.length() == sizeof(Record));
      const Record1* record_ptr = reinterpret_cast<const Record1*>(payload.data());
      Record1 typed_payload = *record_ptr;

      typename Record1::Key typed_key;
      Record1::unfoldKey(key.data(), typed_key);
      return std::optional<next_ret_t>({typed_key, typed_payload});
   }
};