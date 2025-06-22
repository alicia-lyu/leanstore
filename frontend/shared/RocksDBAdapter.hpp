#pragma once
#include <functional>
#include "Adapter.hpp"
#include "RocksDB.hpp"
#include "RocksDBScanner.hpp"
#include "Types.hpp"
#include "leanstore/utils/JumpMU.hpp"
// -------------------------------------------------------------------------------------

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::Range;

template <class Record>
struct RocksDBAdapter : public Adapter<Record> {
   int idx;  // index in RocksDB::cf_handles
   std::unique_ptr<ColumnFamilyHandle> cf_handle;
   RocksDB& map;
   RocksDBAdapter(RocksDB& map) : map(map)
   {
      ColumnFamilyDescriptor cf_desc = ColumnFamilyDescriptor(std::string(Record::id), ColumnFamilyOptions());
      map.cf_descs.push_back(cf_desc);
      map.cf_handles.push_back(nullptr);
      idx = map.cf_descs.size() - 1;
      map.get_handle_cbs.push_back([this]() { get_handle(); });
   }

   void get_handle()
   {
      assert(map.tx_db != nullptr);
      cf_handle.reset(map.cf_handles.at(idx));
   }

   ~RocksDBAdapter()
   {
      Status s = map.tx_db->DestroyColumnFamilyHandle(cf_handle.get());
      cf_handle = nullptr;
      assert(s.ok());
   }
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      s = map.txn->Put(RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
      if (!s.ok()) {
         map.txn->Rollback();
         jumpmu::jump();
      }
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn) final { assert(tryLookup(key, fn)); }

   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      s = map.txn->Get(map.ro, cf_handle.get(), RSlice(folded_key, folded_key_len), &value);
      if (!s.ok()) {
         return false;
      }
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
      return true;
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& fn, leanstore::UpdateSameSizeInPlaceDescriptor&) final
   {
      update1(key, fn);
   }

   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb) final
   {
      Record r;
      lookup1(key, [&](const Record& rec) { r = rec; });
      cb(r);
      insert(key, r);
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      s = map.txn->Delete(RSlice(folded_key, folded_key_len));
      if (!s.ok()) {
         map.txn->Rollback();
         jumpmu::jump();
      }
      return true;
   }
   // -------------------------------------------------------------------------------------
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      if (folded_key_len > Record::maxFoldLength()) {
         std::cout << "folded_key_len: " << folded_key_len << " Record::maxFoldLength(): " << Record::maxFoldLength() << std::endl;
      }
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro);
      for (it->Seek(RSlice(folded_key, folded_key_len)); it->Valid(); it->Next()) {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data()), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                 std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro);
      for (it->SeekForPrev(RSlice(folded_key, folded_key_len)); it->Valid(); it->Prev()) {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data()), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::* f)
   {
      Field local_f;
      [[maybe_unused]] bool found = false;
      lookup1(key, [&](const Record& record) {
         found = true;
         local_f = (record).*f;
      });
      assert(found);
      return local_f;
   }

   std::unique_ptr<Scanner<Record>> getScanner() { return std::make_unique<RocksDBScanner<Record>>(map); }

   double size() {
      std::array<u64, 1> sizes;
      std::array<Range, 1> ranges;
      // min key
      std::vector<u8> min_key(1, 0);  // min key
      std::vector<u8> max_key(Record::maxFoldLength(), 255);  // max key
      ranges[0].start = RSlice(min_key.data(), min_key.size());
      ranges[0].limit = RSlice(max_key.data(), max_key.size());
      map.tx_db->GetApproximateSizes(cf_handle.get(), ranges.data(), ranges.size(), sizes.data());
      return static_cast<double>(sizes[0]) / 1024.0 / 1024.0;  // convert to MB
   }
};
