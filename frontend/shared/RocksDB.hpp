#pragma once

#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/rocksdb_namespace.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/wide_columns.h>

// -------------------------------------------------------------------------------------
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>

template <typename T>
inline rocksdb::Slice RSlice(T* ptr, u64 len)
{
   return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
}

using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::Range;

struct RocksDB {
   rocksdb::TransactionDB* tx_db;
   std::vector<ColumnFamilyDescriptor> cf_descs;
   std::vector<ColumnFamilyHandle*> cf_handles;
   std::vector<std::function<void()>> get_handle_cbs;
   static thread_local rocksdb::Transaction* txn;

   rocksdb::Options db_options;
   rocksdb::WriteOptions wo;
   rocksdb::ReadOptions ro;
   rocksdb::ReadOptions iterator_ro;

   enum class DB_TYPE : u8 { DB, TransactionDB, OptimisticDB };
   const DB_TYPE type;
   // -------------------------------------------------------------------------------------
   RocksDB(DB_TYPE type = DB_TYPE::TransactionDB) : type(type)
   {
      assert(type == DB_TYPE::TransactionDB);  // only allow TransactionDB for now
      // PERSIST & RECOVER
      if (FLAGS_trunc == false && std::filesystem::exists(FLAGS_ssd_path)) {
         FLAGS_recover = true;
      } else if (FLAGS_trunc == true && std::filesystem::exists(FLAGS_ssd_path)) {
         std::filesystem::remove_all(FLAGS_ssd_path);
         std::filesystem::create_directory(FLAGS_ssd_path);
      } else if (!std::filesystem::exists(FLAGS_ssd_path)) {
         std::filesystem::create_directory(FLAGS_ssd_path);
      }
      if (FLAGS_persist_file == "./leanstore.json") {
         FLAGS_persist = false;
      } else {
         FLAGS_persist = true;
      }
      set_options();
   }

   void set_options();

   void open()
   {
      // Add default column family
      cf_descs.insert(cf_descs.begin(), ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, ColumnFamilyOptions()));
      cf_handles.insert(cf_handles.begin(), nullptr);

      rocksdb::Status s = rocksdb::TransactionDB::Open(db_options, {}, FLAGS_ssd_path, cf_descs, &cf_handles, &tx_db);  // will create absent cfs
      if (!s.ok())
         cerr << s.ToString() << endl;
      assert(s.ok());
      // Set the column family handles for all adapters
      for (std::function<void()>& cb : get_handle_cbs) {
         cb();
      }
   }

   ~RocksDB()
   {
      std::cout << "RocksDB::~RocksDB() ";
      if (FLAGS_persist) {
         std::cout << "Waiting for compaction to finish" << std::endl;
         rocksdb::WaitForCompactOptions wfc_options;
         wfc_options.close_db = true;
         rocksdb::Status s = tx_db->WaitForCompact(wfc_options);
         std::ofstream persist_file(FLAGS_persist_file, std::ios::trunc);
         persist_file << "Placeholder. Code logic only needs the file to exist." << std::endl;
         assert(s.ok());
      } else {
         std::cout << "FLAGS_persist is false, compaction skipped. Unable to undo changes. You need to manually remove files.";
      }
      delete tx_db;
   }

   void startTX()
   {
      assert(tx_db != nullptr);
      assert(txn == nullptr);
      rocksdb::Status s;
      txn = tx_db->BeginTransaction(wo, {});
   }
   void commitTX()
   {
      assert(tx_db != nullptr);
      rocksdb::Status s;
      s = txn->Commit();
      delete txn;
      txn = nullptr;
   }

   double get_size(ColumnFamilyHandle* cf_handle, const int max_fold_len, const std::string& name);

   bool Put(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, const rocksdb::Slice& value);

   bool Get(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, PinnableSlice* value);

   bool GetForUpdate(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, PinnableSlice* value);

   bool Delete(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key);

   std::string key_buf; // temporary buffer for ONE folded key

   template <typename Record>
   rocksdb::Slice fold_key(const typename Record::Key& key)
   {
      key_buf.resize(Record::maxFoldLength());
      const u32 folded_key_len = Record::foldKey(reinterpret_cast<u8*>(key_buf.data()), key);
      return RSlice(reinterpret_cast<const char*>(key_buf.data()), folded_key_len);
   }

   template <typename Record>
   rocksdb::Slice fold_record(const Record& record)
   {
      // record's lifetime is managed by the caller
      return RSlice(&record, sizeof(record));
   }

   template <typename Record>
   void insert(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const Record& record)
   {
      Put(cf_handle, fold_key<Record>(key), fold_record<Record>(record));
   }

   template <typename Record>
   void lookup1(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      rocksdb::Slice k_slice = fold_key<Record>(key);
      rocksdb::PinnableSlice value;
      Get(cf_handle, k_slice, &value);
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      cb(record);
      value.Reset();
   }

   template <typename Record>
   bool tryLookup(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const std::function<void(const Record&)>& cb)
   {
      rocksdb::Slice k_slice = fold_key<Record>(key);
      rocksdb::PinnableSlice value;
      Status s = tx_db->Get(ro, cf_handle, k_slice, &value); // force not as part of txn
      if (!s.ok()) {
         return false;
      }
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      cb(record);
      value.Reset();
      return true;
   }

   template <class Record>
   void update1(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const std::function<void(Record&)>& cb, leanstore::UpdateSameSizeInPlaceDescriptor&)
   {
      update1(cf_handle, key, cb);
   }

   template <class Record>
   void update1(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, const std::function<void(Record&)>& cb)
   {
      Record r;
      rocksdb::PinnableSlice r_slice;
      r_slice.PinSelf(RSlice(&r, sizeof(r)));
      GetForUpdate(cf_handle, fold_key<Record>(key), &r_slice);
      Record r_lookedup = *reinterpret_cast<const Record*>(r_slice.data());
      cb(r_lookedup);
      Put(cf_handle, fold_key<Record>(key), fold_record<Record>(r_lookedup));
      r_slice.Reset();
   }

   template <class Record>
   bool erase(ColumnFamilyHandle* cf_handle, const typename Record::Key& key)
   {
      Delete(cf_handle, fold_key<Record>(key));
      return true;
   }

   template <class Field, class Record>
   Field lookupField(ColumnFamilyHandle* cf_handle, const typename Record::Key& key, Field Record::* f)
   {
      Field local_f;
      bool found = false;
      lookup1(cf_handle, key, [&](const Record& record) {
         found = true;
         local_f = (record).*f;
      });
      assert(found);
      return local_f;
   }
};