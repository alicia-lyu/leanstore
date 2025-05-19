#pragma once
#include <variant>
#include "../shared/MergedScanner.hpp"
#include "Exceptions.hpp"
#include "merge_helpers.hpp"
#include "view_templates.hpp"

template <typename JK, typename JR, typename... Rs>
struct MergeJoin {
   HeapMergeHelper<JK, Rs...> heap_merge;
   long produced = 0;

   std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {};

   template <template <typename> class ScannerType>
   MergeJoin(ScannerType<Rs>&... scanners)
       : heap_merge(getHeapConsumesToBeJoined(), scanners...), consume_joined([](const typename JR::Key&, const JR&) {})
   {
      heap_merge.init();
   }

   MergeJoin(std::vector<std::function<HeapEntry<JK>()>>& sources)
       : heap_merge(sources, getHeapConsumesToBeJoined()), consume_joined([](const typename JR::Key&, const JR&) {})
   {
      heap_merge.init();
   }

   template <template <typename> class AdapterType, template <typename> class ScannerType>
   MergeJoin(AdapterType<JR>& joinedAdapter, ScannerType<Rs>&... scanners)
       : heap_merge(getHeapConsumesToBeJoined(), scanners...), consume_joined([&](const auto& k, const auto& v) { joinedAdapter.insert(k, v); })
   {
      heap_merge.init();
   }

   MergeJoin(std::function<void(const typename JR::Key&, const JR&)>& consume_joined, std::vector<std::function<HeapEntry<JK>()>>& sources)
       : heap_merge(sources, getHeapConsumesToBeJoined()), consume_joined(consume_joined)
   {
      heap_merge.init();
   }

   ~MergeJoin()
   {
      if (produced > 1000)
         std::cout << "\r~MergeJoin: produced " << (double)produced / 1000 << "k records------------------------------------" << std::endl;
   }

   void updateAndPrintProduced(int curr_joined)
   {
      produced += curr_joined;
      if (heap_merge.current_jk % 10 == 0) {
         double progress = (double)produced / 1000;
         std::cout << "\rMergeJoin: produced " << progress << "k records------------------------------------";
      }
   }

   template <typename CurrRec, size_t I>
   auto getHeapConsumeToBeJoined()
   {
      return [this](HeapEntry<JK>& entry) {
         heap_merge.current_entry = entry;
         int joined_cnt = joinAndClear<JK, JR, Rs...>(heap_merge.cached_records, heap_merge.current_jk, heap_merge.current_entry.jk, consume_joined,
                                                      std::index_sequence_for<Rs...>{});
         updateAndPrintProduced(joined_cnt);
         std::get<I>(heap_merge.cached_records)
             .emplace_back(typename CurrRec::Key(CurrRec::template fromBytes<typename CurrRec::Key>(entry.k)),
                           CurrRec(CurrRec::template fromBytes<CurrRec>(entry.v)));
      };
   }

   template <size_t... Is>
   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToBeJoined(std::index_sequence<Is...>)
   {
      return {getHeapConsumeToBeJoined<Rs, Is>()...};
   }

   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToBeJoined()
   {
      return getHeapConsumesToBeJoined(std::index_sequence_for<Rs...>{});
   }

   void run() { heap_merge.run(); }

   void next_jk() { heap_merge.next_jk(); }
};

template <typename JK, typename JR, typename... Rs>
struct PremergedJoin {
   long produced = 0;
   JK current_jk = JK::max();
   std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...> cached_records;
   std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {};

   MergedScanner<Rs...>& merged_scanner;

   PremergedJoin(
       MergedScanner<Rs...>& merged_scanner,
       std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
       : consume_joined(consume_joined), merged_scanner(merged_scanner)
   {
   }

   ~PremergedJoin()
   {
      if (produced > 1000)
         std::cout << "\r~PremergedJoin: produced " << (double)produced / 1000 << "k records------------------------------------" << std::endl;
   }

   void updateAndPrintProduced(int curr_joined)
   {
      produced += curr_joined;
      if (current_jk % 10 == 0) {
         double progress = (double)produced / 1000;
         std::cout << "\rPremergedJoin: produced " << progress << "k records------------------------------------";
      }
   }

   int next()
   {
      auto kv = merged_scanner.next();
      if (!kv) {
         return -1;
      }
      auto& k = kv->first;
      auto& v = kv->second;
      JK jk;
      std::visit([&](auto& actual_key) -> void { jk = actual_key.get_jk(); }, k);
      // if (current_jk != jk) join the cached records
      int curr_joined;
      if (current_jk != jk)
         curr_joined = joinAndClear<JK, JR, Rs...>(cached_records, current_jk, jk, consume_joined, std::index_sequence_for<Rs...>{});
      else
         curr_joined = 0;
      updateAndPrintProduced(curr_joined);
      // add the new record to the fcache
      match_emplace_tuple(k, v, std::index_sequence_for<Rs...>{});
      return curr_joined;
   }

   int next_jk()
   {
      while (true) {
         int res = next();
         if (res == 0)
            continue;
         return res;
      }
   }

   void run()
   {
      while (true) {
         int curr_joined = next();
         if (curr_joined == -1) {
            break;
         }
      }
   }

   template <typename Record, size_t I>
   void emplace_tuple(const typename Record::Key& key, const Record& rec)
   {
      std::get<I>(cached_records).emplace_back(key, rec);
   }

   template <size_t... Is>
   void match_emplace_tuple(const std::variant<typename Rs::Key...>& key, const std::variant<Rs...>& rec, std::index_sequence<Is...>)
   {
      (([&]() {
          if (std::holds_alternative<typename Rs::Key>(key)) {
             assert(std::holds_alternative<Rs>(rec));
             emplace_tuple<Rs, Is>(std::get<typename Rs::Key>(key), std::get<Rs>(rec));
          }
       })(),
       ...);
   }
};

template <typename JK, typename... Rs>
struct Merge {
   HeapMergeHelper<JK, Rs...> heap_merge;

   template <typename MergedAdapterType, template <typename> class ScannerType, typename... SourceRecords>
   Merge(MergedAdapterType& mergedAdapter, ScannerType<SourceRecords>&... scanners)
       : heap_merge(getHeapConsumesToMerged<MergedAdapterType, SourceRecords...>(mergedAdapter), scanners...)
   {
      heap_merge.init();
   }

   ~Merge()
   {
      if (heap_merge.sifted > 1000)
         std::cout << "\r~Merge: produced " << (double)heap_merge.sifted / 1000 << "k records------------------------------------" << std::endl;
   }

   void printProgress()
   {
      if (heap_merge.current_jk % 10 == 0) {
         double progress = (double)heap_merge.sifted / 1000;
         std::cout << "\rMerge: " << progress << "k records------------------------------------";
      }
   }

   template <typename MergedAdapterType, typename RecordType, typename SourceRecord>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry<JK>& entry) {
         heap_merge.current_entry = entry;
         auto source_k = SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k);
         auto source_v = SourceRecord::template fromBytes<SourceRecord>(entry.v);
         mergedAdapter.insert(typename RecordType::Key(source_k, source_v), RecordType(source_v));
         printProgress();
      };
   }

   template <typename MergedAdapterType, typename... SourceRecords>
   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToMerged(MergedAdapterType& mergedAdapter)
   {
      return {getHeapConsumeToMerged<MergedAdapterType, Rs, SourceRecords>(mergedAdapter)...};
   }

   void run() { heap_merge.run(); }

   void next_jk() { heap_merge.next_jk(); }
};

template <typename JK, typename JR, typename R1, typename R2>
struct BinaryMergeJoin {
   long produced = 0;
   JK current_jk = JK::max();
   std::tuple<std::vector<std::tuple<typename R1::Key, R1>>, std::vector<std::tuple<typename R2::Key, R2>>> cached_records = {
       std::vector<std::tuple<typename R1::Key, R1>>(),
       std::vector<std::tuple<typename R2::Key, R2>>(),
   };
   std::vector<std::tuple<typename R1::Key, R1>>& cached_r1s = std::get<0>(cached_records);
   std::vector<std::tuple<typename R2::Key, R2>>& cached_r2s = std::get<1>(cached_records);
   std::optional<std::pair<typename R1::Key, R1>> cutoff_r1 = std::nullopt;  // larger than current_jk
   std::optional<std::pair<typename R2::Key, R2>> cutoff_r2 = std::nullopt;  // larger than current_jk

   std::queue<std::pair<typename JR::Key, JR>> joined_records;

   std::function<std::optional<std::pair<typename R1::Key, R1>>()> next_left_func;
   std::function<std::optional<std::pair<typename R2::Key, R2>>()> next_right_func;

   std::function<void(const typename JR::Key&, const JR&)> consume_joined = [this](const typename JR::Key& k, const JR& v) {
      std::pair<typename JR::Key, JR> joined_record = {k, v};
      joined_records.push(joined_record);
   };

   BinaryMergeJoin(std::function<std::optional<std::pair<typename R1::Key, R1>>()> next_left_func,
                   std::function<std::optional<std::pair<typename R2::Key, R2>>()> next_right_func)
       : next_left_func(next_left_func), next_right_func(next_right_func)
   {
      cutoff_r1 = next_left_func();
   }

   ~BinaryMergeJoin()
   {
      if (produced > 1000)
         std::cout << "\r~BinaryMergeJoin: produced " << (double)produced / 1000 << "k records------------------------------------" << std::endl;
   }

   void updateAndPrintProduced(int curr_joined)
   {
      produced += curr_joined;
      if (current_jk % 10 == 0) {
         double progress = (double)produced / 1000;
         std::cout << "\rBinaryMergeJoin: produced " << progress << "k records------------------------------------";
      }
   }

   int join_current_and_clear(JK current_entry_jk)  // also updated current_jk
   {
      auto joined_cnt = joinAndClear<JK, JR, R1, R2>(cached_records, current_jk, current_entry_jk, consume_joined, std::index_sequence_for<R1, R2>{});
      updateAndPrintProduced(joined_cnt);
      return joined_cnt;
   }

   int join_update_cutoff(std::pair<typename R1::Key, R1>& r1, std::pair<typename R2::Key, R2>& r2)
   {
      // join the cached records
      auto r1_jk = SKBuilder<JK>::create(r1.first, r1.second);
      auto r2_jk = SKBuilder<JK>::create(r2.first, r2.second);
      auto match_ret = r1_jk.match(r2_jk);
      auto joined_cnt = join_current_and_clear(match_ret <= 0 ? r1_jk : r2_jk);

      // update cutoff
      if (match_ret < 0) {
         cutoff_r1 = std::nullopt;
         cached_r1s.emplace_back(r1.first, r1.second);
         cutoff_r2 = r2;
      } else if (match_ret > 0) {
         cutoff_r2 = std::nullopt;
         cached_r2s.emplace_back(r2.first, r2.second);
         cutoff_r1 = r1;
      } else {
         cached_r1s.emplace_back(r1.first, r1.second);
         cached_r2s.emplace_back(r2.first, r2.second);
         cutoff_r1 = next_left_func();
         cutoff_r2 = std::nullopt;
      }
      return joined_cnt;
   }

   int next_jk()
   {
      if (cutoff_r1.has_value() && !cutoff_r2.has_value()) {
         auto kv = next_right_func();
         if (!kv) {  // last batch of joins
            cutoff_r1 = std::nullopt;
            cutoff_r2 = std::nullopt;
            return join_current_and_clear(SKBuilder<JK>::create(cutoff_r1->first, cutoff_r1->second));
         } else {
            if (SKBuilder<JK>::create(kv->first, kv->second).match(current_jk) == 0) {  // fill cached records
               cached_r2s.emplace_back(kv->first, kv->second);
               return next_jk();
            } else {  // cached records are cutoff, with competing cutoffs
               return join_update_cutoff(*cutoff_r1, *kv);
            }
         }
      } else if (!cutoff_r1.has_value() && cutoff_r2.has_value()) {
         auto kv = next_left_func();
         if (!kv) {  // last batch of joins
            cutoff_r1 = std::nullopt;
            cutoff_r2 = std::nullopt;
            return join_current_and_clear(SKBuilder<JK>::create(cutoff_r2->first, cutoff_r2->second));
            // proceed to join
         } else {
            if (SKBuilder<JK>::create(kv->first, kv->second).match(current_jk) == 0) {  // fill cached records
               cached_r1s.emplace_back(kv->first, kv->second);
               return next_jk();
            } else {  // cached records are cutoff, with competing cutoffs
               return join_update_cutoff(*kv, *cutoff_r2);
            }
         }
      } else if (!cutoff_r1.has_value() && !cutoff_r2.has_value()) {
         // either scanner_r1 or scanner_r2 is empty
         return -1;
      } else {
         UNREACHABLE();
      }
   }

   void run()
   {
      while (true) {
         int curr_joined = next_jk();
         clear_cached_joined_records();
         if (curr_joined == -1) {
            break;
         }
      }
   }

   void clear_cached_joined_records()
   {
      while (!joined_records.empty()) {
         joined_records.pop();
      }
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      if (joined_records.empty()) {
         while (true) {
            auto curr_joined = next_jk();
            if (curr_joined == -1) {
               return std::nullopt;
            } else if (curr_joined > 0) {
               break;
            }
         }
      }
      auto kv = joined_records.front();
      joined_records.pop();
      return std::make_optional(kv);
   }
};