#pragma once
// #include <stdexcept>
#include "Exceptions.hpp"
// #include "MergedAdapter.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"

using namespace leanstore;

struct LeanStoreMergedAdapter {
   leanstore::KVInterface* btree;
   string name;

   LeanStoreMergedAdapter() {
      // hack
   }

   LeanStoreMergedAdapter(LeanStore& db, string name) : name(name)
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

   void printTreeHeight() { cout << name << " height = " << btree->getHeight() << endl; }

   template <class Record>
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& cb,
                 std::function<void()> undo)
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
   template <class Record>
   void insert(const typename Record::Key& key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const OP_RESULT res = btree->insert(folded_key, folded_key_len, (u8*)(&record), sizeof(Record));
      if (res != leanstore::OP_RESULT::OK && res != leanstore::OP_RESULT::ABORT_TX) {
         std::cerr << "LeanStoreMergedAdapter::insert failed with res value " << std::to_string((int) res) << ", key: " << key << std::endl;
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
         exit(1);
      }
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const OP_RESULT res = btree->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         cb(typed_payload);
      });
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      ensure(res == leanstore::OP_RESULT::OK);
   }

   template <class Record>
   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const OP_RESULT res = btree->tryLookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
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
   template <class Record>
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb, UpdateSameSizeInPlaceDescriptor& update_descriptor)
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
   template <class Record>
   bool erase(const typename Record::Key& key)
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
   template <class Record, class OtherRec>
   void scan(const typename Record::Key& key,
             const std::function<bool(const typename Record::Key&, const Record&)>& cb,
             const std::function<bool(const typename OtherRec::Key&, const OtherRec&)>& other_cb,
             std::function<void()> undo)
   {
      u8 folded_joinkey[Record::joinKeyLength()];
      u16 folded_joinkey_len = Record::foldJKey(folded_joinkey, key);

      u16 folded_key_len = Record::maxFoldLength();
      u16 other_folded_key_len = OtherRec::maxFoldLength();
      
      OP_RESULT ret = btree->scanAsc(
          folded_joinkey, folded_joinkey_len,
          [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
             if (key_length == folded_key_len && payload_length == sizeof(Record)) {
                static_cast<void>(payload_length);
                typename Record::Key typed_key;
                Record::unfoldKey(key, typed_key);
                const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
                return cb(typed_key, typed_payload);
             } else if (key_length == other_folded_key_len && payload_length == sizeof(OtherRec)) {
                static_cast<void>(key_length);
                typename OtherRec::Key typed_key;
                OtherRec::unfoldKey(key, typed_key);
                const OtherRec& typed_payload = *reinterpret_cast<const OtherRec*>(payload);
                return other_cb(typed_key, typed_payload);
             } else {
                UNREACHABLE();
             }
          },
          undo);
      if (ret == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }
   struct ScanCallbackDescriptor
   {
      using UntypedCallbackFn = std::function<bool(const u8*, const u8*)>;

      u16 folded_key_len;
      u16 payload_size;
      UntypedCallbackFn callback;
      std::shared_ptr<void> user_callback;

      template <class Rec>
      static ScanCallbackDescriptor create(std::function<bool(const typename Rec::Key&, const Rec&)> cb)
      {

         auto cb_ptr = std::make_shared<std::function<bool(const typename Rec::Key&, const Rec&)>>(std::move(cb));
         return {
            Rec::maxFoldLength(),
            sizeof(Rec),
            [cb_ptr](const u8* key, const u8* payload) -> bool {
               typename Rec::Key typed_key;
               Rec::unfoldKey(key, typed_key);
               auto& typed_payload = *reinterpret_cast<const Rec*>(payload);
               return (*cb_ptr)(typed_key, typed_payload);
            },
            cb_ptr
         };
      }
   };
   void scan(u8* start_jk,
         const u16 jk_len,
         std::vector<ScanCallbackDescriptor> callback_descriptors,
         std::function<void()> undo)
   {
   OP_RESULT ret = btree->scanAsc(
      start_jk, jk_len,
      [&](const u8* key_data, u16 key_length, const u8* payload, u16 payload_length) {
         for (auto& desc : callback_descriptors) {
            if (desc.folded_key_len == key_length && desc.payload_size == payload_length) {
               return desc.callback(key_data, payload);
            }
         }
         UNREACHABLE();
      },
      undo
      );

      if (ret == leanstore::OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
      }
   }
   // -------------------------------------------------------------------------------------
   template <class Field, class Record>
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

   u64 estimatePages() { return btree->estimatePages(); }
   double size() {
      double s = btree->estimatePages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0;
      return s;
   }
   u64 estimateLeafs() { return btree->estimateLeafs(); }

};