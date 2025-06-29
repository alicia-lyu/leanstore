#pragma once
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <functional>
#include "Adapter.hpp"
#include "RocksDB.hpp"
#include "RocksDBScanner.hpp"

// -------------------------------------------------------------------------------------

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;

using ROCKSDB_NAMESPACE::Range;
using ROCKSDB_NAMESPACE::Status;

template <class Record>
struct RocksDBAdapter : public Adapter<Record> {
   const int idx;  // index in RocksDB::cf_handles
   const std::string name = "table" + std::to_string(Record::id);
   ColumnFamilyHandle* cf_handle;
   RocksDB& map;
   RocksDBAdapter(RocksDB& map) : idx(map.cf_descs.size()), map(map)
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

   ~RocksDBAdapter()
   {
   }

   void insert(const typename Record::Key& key, const Record& record) final { map.template insert<Record>(cf_handle, key, record); }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& cb) final
   {
      map.template lookup1<Record>(cf_handle, key, cb);
   }

   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      return map.template tryLookup<Record>(cf_handle, key, cb);
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb, leanstore::UpdateSameSizeInPlaceDescriptor&) final
   {
      map.template update1<Record>(cf_handle, key, cb);
   }

   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb) final
   {
      map.template update1<Record>(cf_handle, key, cb);
   }
   // -------------------------------------------------------------------------------------

   bool erase(const typename Record::Key& key) final { return map.template erase<Record>(cf_handle, key); }
   // Not part of a txn
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& cb, std::function<void()>) final
   {
      std::string key_buf;
      rocksdb::Slice folded_key = map.template fold_key<Record>(key, key_buf);
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro, cf_handle);
      for (it->Seek(folded_key); it->Valid(); it->Next()) {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data()), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!cb(s_key, s_value))
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
      std::string key_buf;
      rocksdb::Slice folded_key = map.template fold_key<Record>(key, key_buf);
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro, cf_handle);
      for (it->SeekForPrev(folded_key); it->Valid(); it->Prev()) {
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
      return map.template lookupField<Record>(cf_handle, key, f);
   }

   std::unique_ptr<RocksDBScanner<Record>> getScanner() { return std::make_unique<RocksDBScanner<Record>>(cf_handle, map); }

   double size() { return map.get_size(cf_handle, Record::maxFoldLength(), name); }
};
