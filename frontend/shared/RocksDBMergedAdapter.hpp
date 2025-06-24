#pragma once
#include <rocksdb/db.h>
#include "Exceptions.hpp"
#include "RocksDB.hpp"
#include "RocksDBMergedScanner.hpp"
#include "Units.hpp"
#include "leanstore/KVInterface.hpp"

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::Range;
using ROCKSDB_NAMESPACE::Status;

template <typename... Records>
struct RocksDBMergedAdapter {
   int idx;  // index in RocksDB::cf_handles
   std::unique_ptr<ColumnFamilyHandle> cf_handle;
   RocksDB& map;
   const std::string name = "merged" + ((std::to_string(Records::id) + std::string("-")) + ...);

   RocksDBMergedAdapter(RocksDB& map) : map(map)
   {
      ColumnFamilyDescriptor cf_desc = ColumnFamilyDescriptor(name, ColumnFamilyOptions());
      map.cf_descs.push_back(cf_desc);
      map.cf_handles.push_back(nullptr);
      idx = map.cf_descs.size() - 1;
   }

   void get_handle()
   {
      assert(map.tx_db != nullptr);
      cf_handle.reset(map.cf_handles.at(idx + 1));  // +1 because cf_handles[0] is the default column family
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
      map.Put(cf_handle.get(), RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      assert(tryLookup<Record>(key, fn));
   }

   template <class Record>
   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      map.Get(cf_handle.get(), RSlice(folded_key, folded_key_len), &value);
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
      return true;
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb, leanstore::UpdateSameSizeInPlaceDescriptor&)
   {
      Record r;
      u8 folded_key[Record::maxFoldLength()];
      const auto folded_key_len = Record::foldKey(folded_key, key);
      rocksdb::PinnableSlice r_slice;
      r_slice.PinSelf(RSlice(&r, sizeof(r)));
      map.GetForUpdate(cf_handle.get(), RSlice(folded_key, folded_key_len), &r_slice);
      Record r_lookedup = *reinterpret_cast<const Record*>(r_slice.data());
      cb(r_lookedup);
      map.Merge(cf_handle.get(), RSlice(folded_key, folded_key_len), RSlice(&r_lookedup, sizeof(r_lookedup)));
      r_slice.Reset();
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      map.Delete(cf_handle.get(), RSlice(folded_key, folded_key_len));
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
      std::vector<u8> min_key(1, 0);  // min key
      auto start_slice = RSlice(min_key.data(), min_key.size());
      std::vector<u8> max_key(std::max({Records::maxFoldLength()...}), 255);  // max key
      auto limit_slice = RSlice(max_key.data(), max_key.size());

      std::cout << "Compacting " << name << "..." << std::endl;
      auto compact_options = rocksdb::CompactRangeOptions();
      compact_options.change_level = true;
      auto ret = map.tx_db->CompactRange(compact_options, cf_handle.get(), &start_slice, &limit_slice);
      assert(ret.ok());

      std::array<u64, 1> sizes;
      std::array<Range, 1> ranges;
      ranges[0].start = start_slice;
      ranges[0].limit = limit_slice;
      rocksdb::SizeApproximationOptions size_options;
      size_options.include_memtables = true;
      size_options.include_files = true;
      size_options.files_size_error_margin = 0.1;
      map.tx_db->GetApproximateSizes(size_options, cf_handle.get(), ranges.data(), ranges.size(), sizes.data());
      return (double)sizes[0] / 1024.0 / 1024.0;  // convert to MB
   }

   template <typename JK, typename JR>
   std::unique_ptr<RocksDBMergedScanner<JK, JR, Records...>> getScanner()
   {
      return std::make_unique<RocksDBMergedScanner<JK, JR, Records...>>(cf_handle.get(), map);
   }
};