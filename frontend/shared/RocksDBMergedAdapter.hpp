#pragma once
#include <rocksdb/db.h>
#include "Exceptions.hpp"
#include "RocksDB.hpp"
#include "Units.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/utils/JumpMU.hpp"

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::Range;

template <typename... Records>
struct RocksDBMergedAdapter {
   int idx;  // index in RocksDB::cf_handles
   std::unique_ptr<ColumnFamilyHandle> cf_handle;
   RocksDB& map;

   RocksDBMergedAdapter(RocksDB& map) : map(map)
   {
      std::string merged_id = ((std::string(Records::id) + std::string("-")) + ...);
      ColumnFamilyDescriptor cf_desc = ColumnFamilyDescriptor(merged_id, ColumnFamilyOptions());
      map.cf_descs.push_back(cf_desc);
      map.cf_handles.push_back(nullptr);
      idx = map.cf_descs.size() - 1;
   }

   void get_handle()
   {
      assert(map.tx_db != nullptr);
      cf_handle.reset(map.cf_handles.at(idx));
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void insert(const typename Record::Key& key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      if (folded_key_len > Record::maxFoldLength()) {
         throw std::runtime_error("folded_key_len > Record::maxFoldLength() + sizeof(SEP): " + std::to_string(folded_key_len) + " > " +
                                  std::to_string(Record::maxFoldLength()));
      }
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      s = map.txn->Put(RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
      if (!s.ok()) {
         map.txn->Rollback();
         jumpmu::jump();
      }
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      s = map.txn->Get(map.ro, cf_handle.get(), RSlice(folded_key, folded_key_len), &value);
      assert(s.ok());
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
   }

   template <class Record>
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
   template <class Record>
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& fn, leanstore::UpdateSameSizeInPlaceDescriptor&)
   {
      Record r;
      lookup1<Record>(key, [&](const Record& rec) { r = rec; });
      fn(r);
      insert<Record>(key, r);
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   bool erase(const typename Record::Key& key)
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
   template <class Field, class Record>
   Field lookupField(const typename Record::Key& key, Field Record::* f)
   {
      Field local_f;
      bool found = false;
      lookup1(key, [&](const Record& record) {
         found = true;
         local_f = (record).*f;
      });
      assert(found);
      return local_f;
   }

   u64 estimatePages() { UNREACHABLE(); }
   u64 estimateLeafs() { UNREACHABLE(); }

   double size()
   {
      std::array<u64, 1> sizes;
      std::array<Range, 1> ranges;
      // min key
      std::vector<u8> min_key(1, 0);                          // min key
      std::vector<u8> max_key(std::max(Records::maxFoldLength()...), 255);  // max key
      ranges[0].start = RSlice(min_key.data(), min_key.size());
      ranges[0].limit = RSlice(max_key.data(), max_key.size());
      map.tx_db->GetApproximateSizes(cf_handle.get(), ranges.data(), ranges.size(), sizes.data());
      return static_cast<double>(sizes[0]) / 1024.0 / 1024.0;  // convert to MB
   }
};