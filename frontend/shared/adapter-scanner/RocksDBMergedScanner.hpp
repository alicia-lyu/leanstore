#pragma once

#include "Exceptions.hpp"
#include "../RocksDB.hpp"
#include "../variant_utils.hpp"

template <typename JK, typename JR, typename... Records>
struct RocksDBMergedScanner {
   RocksDB& map;
   std::unique_ptr<rocksdb::Iterator> it;
   bool after_seek = false;
   long long produced = 0;

   RocksDBMergedScanner(ColumnFamilyHandle* cf_handle, RocksDB& map) : map(map), it(map.tx_db->NewIterator(map.iterator_ro, cf_handle))
   {
      it->SeekToFirst();
      after_seek = true;
   }

   void reset()
   {
      it->SeekToFirst();
      after_seek = true;
      this->produced = 0;
   }

   template <typename Record>
   void seek(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf, false);
      it->Seek(k_slice);
      after_seek = true;
   }

   template <typename Record>
   void seekForPrev(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      std::string key_buf;
      rocksdb::Slice k_slice = map.template fold_key<Record>(key, key_buf, false);
      it->SeekForPrev(k_slice);
      after_seek = true;
   }

   template <typename Record>
   bool seekTyped(const typename Record::Key& key)
      requires std::disjunction_v<std::is_same<Record, Records>...>
   {
      seekForPrev<Record>(key);
      while (true) {
         if (!it->Valid()) {
            return false;
         }
         auto kv = current().value();
         if (std::holds_alternative<Record>(kv.second)) {
            after_seek = true;
            return true;
         }
         it->Next();
      }
      UNREACHABLE();
   }

   void seekJK(const JK& jk)
   {
      u8 folded_jk[JK::maxFoldLength()];
      unsigned pos = JK::keyfold(folded_jk, jk);
      it->Seek(RSlice(folded_jk, pos));
      after_seek = true;
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> next()
   {
      if (after_seek) {
         after_seek = false;
         // if (!it->Valid() && it->status().ok()) {  // may be invalid after seek
         //    it->Next();
         // }
      } else {
         // invalid not after seek is not allowed
         if (!it->Valid()) {
            return std::nullopt;
         }
         it->Next();
         produced++;
      }
      return current();
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> prev()
   {
      if (after_seek) {
         after_seek = false;
         // if (!it->Valid() && it->status().ok()) {  // may be invalid after seek
         //    it->Prev();
         // }
      } else {
         if (!it->Valid()) {
            return std::nullopt;
         }
         it->Prev();
      }
      return current();
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> current()
   {
      if (!it->Valid()) {
         return std::nullopt;
      }
      auto [key, rec] = toType<Records...>(it->key(), it->value());
      return std::make_pair(key, rec);
   }

   std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> last_in_page()
   {
      return std::nullopt; // a page/block is not necessarily continuous
   }

   int go_to_last_in_page()
   {
      assert(false); // should never be called
      return 0;
   }

   // ---------------------------------------------------------------------------
   // next_raw() — A2c fused_emit scanner path.
   //
   // Advances the iterator and returns (idx_tag, key_slice, value_slice) without
   // constructing the std::variant payload.  The idx_tag is the trailing byte of
   // the COLI tagged key (coli_idx_id encoding — see views_coli.hpp).
   //
   // Callers must not outlive the iterator position — slices point into RocksDB's
   // internal buffers and are invalidated by the next iterator operation.
   //
   // Only meaningful for COLI tagged keys; generic merged scanners whose records
   // don't carry a trailing idx_id byte should use next() instead.
   std::optional<std::tuple<u8, rocksdb::Slice, rocksdb::Slice>> next_raw()
   {
      if (after_seek) {
         after_seek = false;
      } else {
         if (!it->Valid()) {
            return std::nullopt;
         }
         it->Next();
         produced++;
      }
      if (!it->Valid()) {
         return std::nullopt;
      }
      rocksdb::Slice k = it->key();
      rocksdb::Slice v = it->value();
      // The trailing byte of the COLI tagged key encodes the record type
      // (coli_idx_id).  key_len is always >= 1 for any valid COLI record.
      u8 tag = (k.size() > 0)
             ? static_cast<u8>(k.data()[k.size() - 1])
             : u8{0};
      return std::make_tuple(tag, k, v);
   }
};