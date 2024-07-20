#pragma once
#include "Adapter.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

using namespace leanstore;
template <class Record>
struct LeanStoreAdapter : Adapter<Record> {
   leanstore::KVInterface* btree;
   string name;
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : name(name)
   {
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
   // -------------------------------------------------------------------------------------
   void printTreeHeight() { cout << name << " height = " << btree->getHeight() << endl; }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& cb,
                 std::function<void()> undo) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      OP_RESULT ret = btree->scanDesc(
          folded_key, folded_key_len,
          [&](const u8* key, [[maybe_unused]] u16 key_length, const u8* payload, [[maybe_unused]] u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             typename Record::Key typed_key;
             Record::unfoldKey(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return cb(typed_key, typed_payload);
          },
          undo);
      if (ret == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const OP_RESULT res = btree->insert(folded_key, folded_key_len, (u8*)(&record), sizeof(Record));
      ensure(res == leanstore::OP_RESULT::OK || res == leanstore::OP_RESULT::ABORT_TX);
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& cb) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const OP_RESULT res = btree->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         cb(typed_payload);
      });
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      ensure(res == leanstore::OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb, UpdateSameSizeInPlaceDescriptor& update_descriptor) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      if (!FLAGS_vi_delta) {
         // Disable deltas, copy the whole tuple [hacky]
         ensure(update_descriptor.count > 0);
         ensure(!FLAGS_vi_fat_tuple);
         update_descriptor.count = 1;
         update_descriptor.slots[0].offset = 0;
         update_descriptor.slots[0].length = sizeof(Record);
      }
      // -------------------------------------------------------------------------------------
      const OP_RESULT res = btree->updateSameSizeInPlace(
          folded_key, folded_key_len,
          [&](u8* payload, u16 payload_length) {
             static_cast<void>(payload_length);
             assert(payload_length == sizeof(Record));
             Record& typed_payload = *reinterpret_cast<Record*>(payload);
             cb(typed_payload);
          },
          update_descriptor);
      ensure(res != leanstore::OP_RESULT::NOT_FOUND);
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const auto res = btree->remove(folded_key, folded_key_len);
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      return (res == leanstore::OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   void scan(const typename Record::Key& key,
             const std::function<bool(const typename Record::Key&, const Record&)>& cb,
             std::function<void()> undo) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      OP_RESULT ret = btree->scanAsc(
          folded_key, folded_key_len,
          [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             static_cast<void>(payload_length);
             typename Record::Key typed_key;
             Record::unfoldKey(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return cb(typed_key, typed_payload);
          },
          undo);
      if (ret == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      Field local_f;
      const OP_RESULT res = btree->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         Record& typed_payload = *const_cast<Record*>(reinterpret_cast<const Record*>(payload));
         local_f = (typed_payload).*f;
      });
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      ensure(res == OP_RESULT::OK);
      return local_f;
   }
   // -------------------------------------------------------------------------------------
   u64 count() { return btree->countEntries(); }

   virtual Scanner<Record> getScanner() {
      if (FLAGS_vi) {
         return Scanner<Record>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(btree)));
      } else {
         return Scanner<Record>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(btree)));
      }
   }
};

template <class... Records>
struct LeanstoreMergedAdapter : MergedAdapter<Records...> {
   using RecordVariant = std::variant<Records...>;
   leanstore::KVInterface* btree;
   string name;

   LeanstoreMergedAdapter() = default;
   LeanstoreMergedAdapter(LeanStore& db, string name) : name(name) {
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

   void insert(const typename RecordVariant::Key& key, const RecordVariant& record) final {
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

   void lookup(const typename RecordVariant::Key& key, const std::function<void(const RecordVariant&)>& cb) final {
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

   bool erase(const typename RecordVariant::Key& key) final {
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

   void scan(const typename RecordVariant::Key& key,
              const std::function<bool(const typename RecordVariant::Key&, const RecordVariant&)>& cb,
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

   void update(const typename RecordVariant::Key& key,
               const std::function<void(RecordVariant&)>& cb,
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

   // Scanner<Records...> getScanner() {
   //    if (FLAGS_vi) {
   //       return Scanner<Records...>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(btree)));
   //    } else {
   //       return Scanner<Records...>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(btree)));
   //    }
   // }
};
