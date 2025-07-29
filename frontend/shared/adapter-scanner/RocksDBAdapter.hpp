#pragma once
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <functional>
#include "../RocksDB.hpp"
#include "Adapter.hpp"
#include "RocksDBScanner.hpp"
#include "Units.hpp"

// -------------------------------------------------------------------------------------

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;

using ROCKSDB_NAMESPACE::Range;
using ROCKSDB_NAMESPACE::Status;

template <class Record>
struct RocksDBAdapter : public Adapter<Record> {
   ColumnFamilyHandle* cf_handle;
   RocksDB& map;
   RocksDBAdapter(RocksDB& map) : map(map)
   {
      map.get_handle_cbs.push_back([this]() { get_handle(); });
   }

   void get_handle()
   {
      assert(map.tx_db != nullptr);
      cf_handle = map.cf_handles.at(0);  // default column family
   }

   ~RocksDBAdapter() {}

   void insert(const typename Record::Key& key, const Record& record) final { map.template insert<Record>(cf_handle, key, record, true); }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& cb) final
   {
      map.template lookup1<Record>(cf_handle, key, cb, true);
   }

   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      return map.template tryLookup<Record>(cf_handle, key, cb, true);
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb, leanstore::UpdateSameSizeInPlaceDescriptor&) final
   {
      map.template update1<Record>(cf_handle, key, cb, true);
   }

   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb) final
   {
      map.template update1<Record>(cf_handle, key, cb, true);
   }
   // -------------------------------------------------------------------------------------

   bool erase(const typename Record::Key& key) final { return map.template erase<Record>(cf_handle, key, true); }
   // Not part of a txn
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& cb, std::function<void()>) final
   {
      std::string key_buf;
      rocksdb::Slice folded_key = map.template fold_key<Record>(key, key_buf, true);
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro, cf_handle);
      for (it->Seek(folded_key); it->Valid(); it->Next()) {
         u8 id;
         const u8* key_data = reinterpret_cast<const u8*>(it->key().data());
         unsigned pos = unfold(key_data, id);
         if (id != static_cast<u8>(Record::id)) {  // passed the record type
            assert(id > static_cast<u8>(Record::id));
            break;
         }
         typename Record::Key s_key;
         Record::unfoldKey(key_data + pos, s_key);
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
      rocksdb::Slice folded_key = map.template fold_key<Record>(key, key_buf, true);
      rocksdb::Iterator* it = map.tx_db->NewIterator(map.iterator_ro, cf_handle);
      for (it->SeekForPrev(folded_key); it->Valid(); it->Prev()) {
         u8 id;
         const u8* key_data = reinterpret_cast<const u8*>(it->key().data());
         unsigned pos = unfold(key_data, id);
         if (id != static_cast<u8>(Record::id)) {  // passed the record type
            assert(id < static_cast<u8>(Record::id));
            break;  // no more records of this type
         }
         typename Record::Key s_key;
         Record::unfoldKey(key_data + pos, s_key);
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

   double size() { return map.get_size<Record>(); }
};
