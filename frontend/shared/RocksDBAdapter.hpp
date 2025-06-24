#pragma once
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
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
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::Range;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;

template <class Record>
struct RocksDBAdapter : public Adapter<Record> {
   int idx;  // index in RocksDB::cf_handles
   std::unique_ptr<ColumnFamilyHandle> cf_handle;
   RocksDB& map;
   const std::string name = "table" + std::to_string(Record::id);
   RocksDBAdapter(RocksDB& map) : map(map)
   {
      ColumnFamilyDescriptor cf_desc = ColumnFamilyDescriptor(name, ColumnFamilyOptions());
      map.cf_descs.push_back(cf_desc);
      map.cf_handles.push_back(nullptr);
      idx = map.cf_descs.size() - 1;
      map.get_handle_cbs.push_back([this]() { get_handle(); });
   }

   void get_handle()
   {
      assert(map.tx_db != nullptr);
      cf_handle.reset(map.cf_handles.at(idx + 1));  // +1 because cf_handles[0] is the default column family
   }

   ~RocksDBAdapter()
   {
      Status s = map.tx_db->DestroyColumnFamilyHandle(cf_handle.get());
      cf_handle = nullptr;
      assert(s.ok());
   }

   bool Put(const Slice& key, const Slice& value)
   {
      if (map.txn == nullptr) {
         Status s = map.tx_db->Put(map.wo, cf_handle.get(), key, value);
         return s.ok();
      } else {
         Status s = map.txn->Put(cf_handle.get(), key, value);
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
         return true;
      }
   }

   void insert(const typename Record::Key& key, const Record& record) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      Put(RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn) final { assert(tryLookup(key, fn)); }

   bool Get(const Slice& key, PinnableSlice* value)
   {
      if (map.txn == nullptr) {
         Status s = map.tx_db->Get(map.ro, cf_handle.get(), key, value);
         return s.ok();
      } else {
         Status s = map.txn->Get(map.ro, cf_handle.get(), key, value);
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
         return true;
      }
   }

   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      bool res = Get(RSlice(folded_key, folded_key_len), &value);
      if (!res) {
         std::cerr << "RocksDBAdapter::tryLookup: Get failed for key: " << key << std::endl;
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

   bool GetForUpdate(const Slice& key, PinnableSlice* value)
   {
      if (map.txn == nullptr) {
         Status s = map.tx_db->Get(map.ro, cf_handle.get(), key, value);
         return s.ok();
      } else {
         Status s = map.txn->GetForUpdate(map.ro, cf_handle.get(), key, value);
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
         return true;
      }
   }

   bool Merge(const Slice& key, const Slice& value)
   {
      if (map.txn == nullptr) {
         Status s = map.tx_db->Merge(map.wo, cf_handle.get(), key, value);
         return s.ok();
      } else {
         Status s = map.txn->Merge(cf_handle.get(), key, value);
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
         return true;
      }
   }

   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb) final
   {
      Record r;
      u8 folded_key[Record::maxFoldLength()];
      const auto folded_key_len = Record::foldKey(folded_key, key);
      rocksdb::PinnableSlice r_slice;
      r_slice.PinSelf(RSlice(&r, sizeof(r)));
      GetForUpdate(RSlice(folded_key, folded_key_len), &r_slice);
      Record r_lookedup = *reinterpret_cast<const Record*>(r_slice.data());
      cb(r_lookedup);
      Merge(RSlice(folded_key, folded_key_len), RSlice(&r_lookedup, sizeof(r_lookedup)));
   }
   // -------------------------------------------------------------------------------------
   bool Delete(const Slice& key)
   {
      if (map.txn == nullptr) {
         Status s = map.tx_db->Delete(map.wo, cf_handle.get(), key);
         return s.ok();
      } else {
         Status s = map.txn->Delete(cf_handle.get(), key);
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
         return true;
      }
   }

   bool erase(const typename Record::Key& key) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      Delete(RSlice(folded_key, folded_key_len));
      return true;
   }
   // Not part of a txn
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro, cf_handle.get());
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
   // Not part of a txn
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                 std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key, key);
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro, cf_handle.get());
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

   std::unique_ptr<RocksDBScanner<Record>> getScanner() { return std::make_unique<RocksDBScanner<Record>>(cf_handle.get(), map); }

   double size()
   {
      std::vector<u8> min_key(1, 0);  // min key
      auto start_slice = RSlice(min_key.data(), min_key.size());
      std::vector<u8> max_key(Record::maxFoldLength(), 255);  // max key
      auto limit_slice = RSlice(max_key.data(), max_key.size());

      std::cout << "Compacting " << name << "..." << std::endl;
      auto compact_options = rocksdb::CompactRangeOptions();
      compact_options.change_level = true;
      auto ret = map.tx_db->CompactRange(compact_options, cf_handle.get(), &start_slice, &limit_slice);
      assert(ret.ok());

      std::array<u64, 1> sizes;
      std::array<Range, 1> ranges{Range{start_slice, limit_slice}};

      rocksdb::SizeApproximationOptions size_options;
      size_options.include_memtables = true;
      size_options.include_files = true;
      size_options.files_size_error_margin = 0.1;
      map.tx_db->GetApproximateSizes(size_options, cf_handle.get(), ranges.data(), ranges.size(), sizes.data());
      return (double)sizes[0] / 1024.0 / 1024.0;  // convert to MB
   }
};
