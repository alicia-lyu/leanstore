#pragma once
#include "../shared/RocksDBAdapter.hpp"
#include "Exceptions.hpp"
#include "Units.hpp"

template <typename... Records>
struct RocksDBMergedAdapter {
   RocksDB& map;
   RocksDBMergedAdapter(RocksDB& map) : map(map) {

   }
   // -------------------------------------------------------------------------------------
   template <typename T>
   rocksdb::Slice RSlice(T* ptr, u64 len)
   {
      return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
   }

   template <class T>
   uint32_t getId(const T& str)
   {
      return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(str.data())) ^ (1ul << 31);
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void insert(const typename Record::Key& key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, merged_id) + Record::foldKey(folded_key + sizeof(SEP), key);
      if (folded_key_len > Record::maxFoldLength() + sizeof(SEP)) {
         throw std::runtime_error("folded_key_len > Record::maxFoldLength() + sizeof(SEP): " + std::to_string(folded_key_len) + " > " + std::to_string(Record::maxFoldLength() + sizeof(SEP)));
      }
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Put(map.wo, RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
         ensure(s.ok());
      } else {
         s = map.txn->Put(RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
      }
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, merged_id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      } else {
         s = map.txn->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      }
      assert(s.ok());
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
   }

   template <class Record>
   bool tryLookup(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, merged_id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      } else {
         s = map.txn->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      }
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
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, merged_id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      if (map.type == RocksDB::DB_TYPE::DB) {
         s = map.db->Delete(map.wo, RSlice(folded_key, folded_key_len));
         if (s.ok()) {
            return true;
         } else {
            return false;
         }
      } else {
         s = map.txn->Delete(RSlice(folded_key, folded_key_len));
         if (!s.ok()) {
            map.txn->Rollback();
            jumpmu::jump();
         }
         return true;
      }
   }
   //             [&](const neworder_t::Key& key, const neworder_t&) {
   template <class Record, class OtherRec>
   void scan(const typename Record::Key& key, // It is the caller's reponsibility to choose the smaller key
             const std::function<bool(const typename Record::Key&, const Record&)>& fn,
             const std::function<bool(const typename OtherRec::Key&, const OtherRec&)>& other_fn,
             std::function<void()>)
   {
      rocksdb::Slice start_key;
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, merged_id) + Record::foldKey(folded_key + sizeof(SEP), key);
      start_key = RSlice(folded_key, folded_key_len);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = map.db->NewIterator(map.iterator_ro);
      u32 rec_len = Record::maxFoldLength() + sizeof(SEP);
      u32 other_rec_len = OtherRec::maxFoldLength() + sizeof(SEP);
      for (it->Seek(start_key); it->Valid() && getId(it->key()) == merged_id; it->Next()) {
         bool is_rec = false;
         bool is_other_rec = false;
         size_t key_len = it->key().size();
         if (rec_len < other_rec_len) {
            if (key_len <= rec_len) {
               is_rec = true;
            } else if (key_len <= other_rec_len) {
               is_other_rec = true;
            } else {
               // typename OtherRec::Key s_key;
               // OtherRec::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
               // const OtherRec& s_value = *reinterpret_cast<const OtherRec*>(it->value().data());
               std::cout << "key_len: " << key_len << " rec_len: " << rec_len << " other_rec_len: " << other_rec_len << std::endl;
               UNREACHABLE();
            }
         } else if (rec_len > other_rec_len) {
            if (key_len <= other_rec_len) {
               is_other_rec = true;
            } else if (key_len <= rec_len) {
               is_rec = true;
            } else {
               std::cout << "key_len: " << key_len << " rec_len: " << rec_len << " other_rec_len: " << other_rec_len << std::endl;
               UNREACHABLE();
            }
         } else {
            std::cout << "key_len: " << key_len << " rec_len: " << rec_len << " other_rec_len: " << other_rec_len << std::endl;
            UNREACHABLE(); // Do not allow same length
         }
         if (is_rec) {
            typename Record::Key s_key;
            Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
            const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
            if (!fn(s_key, s_value))
               break;
         } else if (is_other_rec) {
            typename OtherRec::Key s_key;
            OtherRec::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
            const OtherRec& s_value = *reinterpret_cast<const OtherRec*>(it->value().data());
            if (!other_fn(s_key, s_value))
               break;
         } else {
            UNREACHABLE();
         }
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   template <class Field, class Record>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
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
};