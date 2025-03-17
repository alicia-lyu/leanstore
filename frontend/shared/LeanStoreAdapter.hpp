#pragma once
#include "Adapter.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "LeanStoreScanner.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstring>
#include <functional>
#include <optional>
#include <iomanip>

using namespace leanstore;
template <class Record>
struct LeanStoreAdapter : Adapter<Record> {
   leanstore::KVInterface* btree;
   leanstore::storage::btree::BTreeGeneric* btree_generic;
   std::unique_ptr<leanstore::storage::btree::BTreeSharedIterator> it;
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
      btree_generic = static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(btree));
      it = std::make_unique<leanstore::storage::btree::BTreeSharedIterator>(*btree_generic);
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
      // print hex
      std::cout << "folded key length: " << folded_key_len << std::endl;
      for (size_t i = 0; i < folded_key_len; ++i) {
         std::cout << std::hex << std::setw(2) << std::setfill('0') << (int)folded_key[i] << " ";
     }
     std::cout << std::dec << std::endl;
      // try unfold
      typename Record::Key typed_key;
      Record::unfoldKey(folded_key, typed_key);
      std::cout << "unfolded key: " << typed_key << std::endl;
      if (res != leanstore::OP_RESULT::OK && res != leanstore::OP_RESULT::ABORT_TX) {
         std::cerr << "LeanStoreAdapter::insert failed with res value " << std::to_string((int) res) << ", key: " << key << std::endl;
         this->scan(typename Record::Key{}, [&](const typename Record::Key& k, const Record&) {
            std::cout << "key: " << k << std::endl;
            return true;
         },
         []() {});
         exit(1);
      }
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
      if (res != leanstore::OP_RESULT::OK) {
         std::cout << std::to_string((int) res) << key << std::endl;
         ensure(false);
      }
   }

   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const OP_RESULT res = btree->tryLookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         cb(typed_payload);
      });
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      if (res != leanstore::OP_RESULT::OK) {
         return false;
      } else {
         return true;
      }
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
   
   std::optional<std::pair<typename Record::Key, Record>> next() {
      leanstore::OP_RESULT res = it->next();
      if (res != leanstore::OP_RESULT::OK)
         return std::nullopt;
      it->assembleKey();
      leanstore::Slice key = it->key();
      leanstore::Slice payload = it->value();

      Record typed_payload = *reinterpret_cast<const Record*>(payload.data());

      typename Record::Key typed_key;
      Record::unfoldKey(key.data(), typed_key);
      return std::make_pair(typed_key, typed_payload);
   };

   void seek(const typename Record::Key& key) {
      u8 keyBuffer[Record::maxFoldLength()];
      Record::foldKey(keyBuffer, key);
      leanstore::Slice keySlice(keyBuffer, Record::maxFoldLength());
      leanstore::OP_RESULT res = it->seek(keySlice);
      if (res != leanstore::OP_RESULT::OK) {
         std::cerr << "LeanStoreAdapter::seek failed" << std::endl;
      }
   }

   void resetIterator() {
      it->reset();
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

   std::unique_ptr<Scanner<Record>> getScanner() {
      if (FLAGS_vi) {
         return std::make_unique<LeanStoreScanner<Record>>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(btree)));
      } else {
         return std::make_unique<LeanStoreScanner<Record>>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(btree)));
      }
   }

   template <class PayloadProvider>
   std::unique_ptr<Scanner<Record, PayloadProvider>> getScanner(Adapter<PayloadProvider>* payloadProvider) {
      leanstore::KVInterface* payloadBtree = dynamic_cast<LeanStoreAdapter<PayloadProvider>*>(payloadProvider)->btree;
      if (FLAGS_vi) {
         return std::make_unique<LeanStoreScanner<Record, PayloadProvider>>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(btree)),
         *static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(payloadBtree)));
      } else {
         return std::make_unique<LeanStoreScanner<Record, PayloadProvider>>(*static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(btree)), 
         *static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(payloadBtree)));
      }
   }

   u64 estimatePages() final { return btree->estimatePages(); }
   u64 estimateLeafs() final { return btree->estimateLeafs(); }
};
