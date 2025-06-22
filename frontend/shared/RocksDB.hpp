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
      }
      set_options();
   }

   void set_options();

   void create_cfs()
   {
      assert(tx_db == nullptr);
      rocksdb::Status s = rocksdb::TransactionDB::Open(db_options, {}, FLAGS_ssd_path, &tx_db);  // open without specified column families
      assert(s.ok());
      tx_db->CreateColumnFamilies(cf_descs, &cf_handles);
      for (auto& cf_handle : cf_handles) {
         tx_db->DestroyColumnFamilyHandle(cf_handle);
      }
      delete tx_db;
   }

   void open()
   {
      if (!FLAGS_recover) {  // cf not present
         create_cfs();
      }
      // Add default column family
      cf_descs.push_back(ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, ColumnFamilyOptions()));
      cf_handles.push_back(nullptr);

      rocksdb::Status s = rocksdb::TransactionDB::Open(db_options, {}, FLAGS_ssd_path, cf_descs, &cf_handles, &tx_db);
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
};