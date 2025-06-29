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

using ROCKSDB_NAMESPACE::Status;

template <typename... Records>
struct RocksDBMergedAdapter {
   const int idx;  // index in RocksDB::cf_handles
   const std::string name = "merged" + ((std::to_string(Records::id) + std::string("-")) + ...);
   ColumnFamilyHandle* cf_handle;
   RocksDB& map;

   RocksDBMergedAdapter(RocksDB& map) : idx(map.cf_descs.size()), map(map)
   {
      ColumnFamilyDescriptor cf_desc = ColumnFamilyDescriptor(name, ColumnFamilyOptions());
      map.cf_descs.push_back(cf_desc);
      map.cf_handles.push_back(nullptr);
      map.get_handle_cbs.push_back([this]() { get_handle(); });
   }

   void get_handle()
   {
      assert(map.tx_db != nullptr);
      ColumnFamilyHandle* handle = map.cf_handles.at(idx + 1); // +1 because cf_handles[0] is the default column family
      assert(handle != nullptr);
      cf_handle = handle;
   }

   ~RocksDBMergedAdapter()
   {
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void insert(const typename Record::Key& key, const Record& record)
   {
      map.template insert<Record>(cf_handle, key, record);
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      map.template lookup1<Record>(cf_handle, key, cb);
   }

   template <class Record>
   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      return map.template tryLookup<Record>(cf_handle, key, cb);
   }

   template <class Record>
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& fn, leanstore::UpdateSameSizeInPlaceDescriptor&)
   {
      map.template update1<Record>(cf_handle, key, fn);
   }

   template <class Record>
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb)
   {
      map.template update1<Record>(cf_handle, key, cb);
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   bool erase(const typename Record::Key& key)
   {
      return map.template erase<Record>(cf_handle, key);
   }
   // -------------------------------------------------------------------------------------
   template <class Field, class Record>
   Field lookupField(const typename Record::Key& key, Field Record::* f)
   {
      return map.template lookupField<Record>(cf_handle, key, f);
   }

   u64 estimatePages() { UNREACHABLE(); }
   u64 estimateLeafs() { UNREACHABLE(); }

   double size()
   {
      return map.get_size(cf_handle, std::max({Records::maxFoldLength()...}), name);
   }

   template <typename JK, typename JR>
   std::unique_ptr<RocksDBMergedScanner<JK, JR, Records...>> getScanner()
   {
      return std::make_unique<RocksDBMergedScanner<JK, JR, Records...>>(cf_handle, map);
   }
};