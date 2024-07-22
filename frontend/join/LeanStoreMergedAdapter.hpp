#pragma once
#include "MergedAdapter.hpp"
#include "leanstore/LeanStore.hpp"

using namespace leanstore;
template <class... Records>
struct LeanStoreMergedAdapter : MergedAdapter<Records...> {
   using Record = RecordVariant<Records...>;
   leanstore::KVInterface* btree;
   string name;

   LeanStoreMergedAdapter() = default;
   LeanStoreMergedAdapter(LeanStore& db, string name) : name(name) {
      if (FLAGS_vi) {
         if (FLAGS_recover) {
            btree = &db.retrieveBTreeVI(name);
         } else {
            btree = &db.registerBTreeVI(name, {.enable_wal = FLAGS_wal, .use_bulk_insert = false});
         }
      } else {
         if (FLAGS_recover) {
            btree = &db.retrieveBTreeLL(name);
         } else {
            btree = &db.registerBTreeLL(name, {.enable_wal = FLAGS_wal, .use_bulk_insert = false});
         }
      }
   }

   void insert(const typename Record::Key& key, const Record::Value& record) final {
      std::visit([this, &key](auto&& arg) {
         using RecordType = std::decay_t<decltype(arg)>;
         u8 folded_key[RecordType::maxFoldLength()];
         u16 folded_key_len = RecordType::foldKey(folded_key, key);
         const OP_RESULT res = btree->insert(folded_key, folded_key_len, (u8*)(&arg), sizeof(arg));
         ensure(res == leanstore::OP_RESULT::OK || res == leanstore::OP_RESULT::ABORT_TX);
         if (res == leanstore::OP_RESULT::ABORT_TX) {
            cr::Worker::my().abortTX();
         }
      }, record);
   }

   void lookup(const typename Record::Key& key, const std::function<void(const typename Record::Value &)>& cb) final {
      std::visit([this, &key, &cb](auto&& arg) {
         using RecordType = std::decay_t<decltype(arg)>;
         u8 folded_key[RecordType::maxFoldLength()];
         u16 folded_key_len = RecordType::foldKey(folded_key, key);
         const OP_RESULT res = btree->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
            static_cast<void>(payload_length);
            const RecordType& typed_payload = *reinterpret_cast<const RecordType*>(payload);
            cb(typed_payload);
         });
         if (res == leanstore::OP_RESULT::ABORT_TX) {
            cr::Worker::my().abortTX();
         }
         ensure(res == leanstore::OP_RESULT::OK);
      }, key);
   }

   bool erase(const typename Record::Key& key) final {
      std::visit([this, &key](auto&& arg) {
         using RecordType = std::decay_t<decltype(arg)>;
         u8 folded_key[RecordType::maxFoldLength()];
         u16 folded_key_len = RecordType::foldKey(folded_key, key);
         const auto res = btree->remove(folded_key, folded_key_len);
         if (res == leanstore::OP_RESULT::ABORT_TX) {
            cr::Worker::my().abortTX();
         }
         return (res == leanstore::OP_RESULT::OK);
      }, key);
   }

   void scan(const typename Record::Key& key,
              const std::function<bool(const typename Record::Key&, const typename Record::Value&)>& cb,
              std::function<void()> undo) final {
      std::visit([this, &key, &cb, &undo](auto&& arg) {
         using RecordType = std::decay_t<decltype(arg)>;
         u8 folded_key[RecordType::maxFoldLength()];
         u16 folded_key_len = RecordType::foldKey(folded_key, key);
         OP_RESULT ret = btree->scanAsc(
            folded_key, folded_key_len,
            [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
               if (key_length != folded_key_len) {
                  return false;
               }
               static_cast<void>(payload_length);
               typename RecordType::Key typed_key;
               RecordType::unfoldKey(key, typed_key);
               const RecordType& typed_payload = *reinterpret_cast<const RecordType*>(payload);
               return cb(typed_key, typed_payload);
            },
            undo);
         if (ret == leanstore::OP_RESULT::ABORT_TX) {
            cr::Worker::my().abortTX();
         }
      }, key);
   }

   void update(const typename Record::Key& key,
               const std::function<void(typename Record::Value&)>& cb,
               leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) final {
      std::visit([this, &key, &cb, &update_descriptor](auto&& arg) {
         using RecordType = std::decay_t<decltype(arg)>;
         u8 folded_key[RecordType::maxFoldLength()];
         u16 folded_key_len = RecordType::foldKey(folded_key, key);
         if (!FLAGS_vi_delta) {
            ensure(update_descriptor.count > 0);
            ensure(!FLAGS_vi_fat_tuple);
            update_descriptor.count = 1;
            update_descriptor.slots[0].offset = 0;
            update_descriptor.slots[0].length = sizeof(arg);
         }
         const OP_RESULT res = btree->updateSameSizeInPlace(
            folded_key, folded_key_len,
            [&](u8* payload, u16 payload_length) {
               static_cast<void>(payload_length);
               assert(payload_length == sizeof(arg));
               RecordType& typed_payload = *reinterpret_cast<RecordType*>(payload);
               cb(typed_payload);
            },
            update_descriptor);
         ensure(res != leanstore::OP_RESULT::NOT_FOUND);
         if (res == leanstore::OP_RESULT::ABORT_TX) {
            cr::Worker::my().abortTX();
         }
      }, key);
   }
};