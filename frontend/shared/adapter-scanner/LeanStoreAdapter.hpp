#pragma once
#include "Adapter.hpp"
// -------------------------------------------------------------------------------------
#include "LeanStoreScanner.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstring>
#include <functional>
#include <iomanip>

using namespace leanstore;
template <class Record>
struct LeanStoreAdapter : Adapter<Record> {
   leanstore::KVInterface* btree;
   u64 produced;
   string name;
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : produced(0), name(name)
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
      if (res != leanstore::OP_RESULT::OK && res != leanstore::OP_RESULT::ABORT_TX) {
         bad_res(res, key, "insert");
         throw std::runtime_error("insert failed with res value " + std::to_string((int)res));
      }
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& cb) final
   {
      bool res = tryLookup(key, cb);
      if (!res) {
         std::cerr << "LeanStoreAdapter::lookup1 failed for key: " << key << std::endl;
         throw std::runtime_error("lookup1 failed");
      }
   }

   template <typename JK>
   bool tryLookup(const JK& key, const std::function<void(const Record&)>& cb)
   {
      u8 folded_jk[JK::maxFoldLength()];
      u16 folded_jk_len = JK::keyfold(folded_jk, key);
      const OP_RESULT res = btree->lookup(folded_jk, folded_jk_len, [&](const u8* payload, u16 payload_length) {
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
   void bad_res(const OP_RESULT res, const typename Record::Key& key, std::string operation)
   {
      std::cerr << "LeanStoreAdapter<" << Record::id << ">::" << operation << " failed with res value " << std::to_string((int)res)
                << ", key: " << key << std::endl;
      // print hex
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      std::cerr << "LeanstoreAdapter::" << operation << ": folded key length: " << folded_key_len << std::endl;
      for (size_t i = 0; i < folded_key_len; ++i) {
         std::cerr << std::hex << std::setw(2) << std::setfill('0') << (int)folded_key[i] << " ";
      }
      std::cerr << std::dec << std::endl;
      // try unfold
      typename Record::Key typed_key;
      Record::unfoldKey(folded_key, typed_key);
      std::cerr << "unfolded key: " << typed_key << std::endl;
   }

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
      if (res == leanstore::OP_RESULT::NOT_FOUND) {
         bad_res(res, key, "update1");
         throw std::runtime_error("update1 failed with res value " + std::to_string((int)res));
      }
      if (res == leanstore::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }

   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb) final
   {
      // lookup value
      Record record;
      const bool lookup_res = tryLookup(key, [&](const Record& r) {
         record = r;
         cb(record);
      });
      assert(lookup_res);
      // remove
      const bool remove_res = erase(key);
      assert(remove_res);
      // then insert
      insert(key, record);
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
          [&](const u8* key, u16 key_length, const u8* payload, u16) {
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
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::* f)
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

   std::unique_ptr<LeanStoreScanner<Record>> getScanner()
   {
      std::unique_ptr<LeanStoreScanner<Record>> scanner;
      if (FLAGS_vi) {
         scanner = std::make_unique<LeanStoreScanner<Record>>(
             *static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeVI*>(btree)));
      } else {
         scanner = std::make_unique<LeanStoreScanner<Record>>(
             *static_cast<leanstore::storage::btree::BTreeGeneric*>(dynamic_cast<leanstore::storage::btree::BTreeLL*>(btree)));
      }
      return scanner;
   }

   u64 estimatePages() final { return btree->estimatePages(); }

   double size()
   {
      double s = estimatePages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0;  // MiB
      return s;
   }

   u64 estimateLeafs() final { return btree->estimateLeafs(); }
};
