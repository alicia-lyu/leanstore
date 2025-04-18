#pragma once
#include "../shared/MergedScanner.hpp"
#include "merge_helpers.hpp"

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

   ~MergeJoin() { std::cout << "\r~MergeJoin: produced " << (double)produced / 1000 << "k records------------------------------------" << std::endl; }

   void updateAndPrintProduced(int curr_joined)
   {
      produced += curr_joined;
      if (heap_merge.last_joined_jk % 10 == 0) {
         double progress = (double)produced / 1000;
         std::cout << "\rMergeJoin: produced " << progress << "k records------------------------------------";
      }
   }

   template <typename CurrRec, size_t I>
   auto getHeapConsumeToBeJoined()
   {
      return [this](HeapEntry<JK>& entry) {
         heap_merge.current_entry = entry;
         int joined_cnt = joinAndClear<JK, JR, Rs...>(heap_merge.cached_records, heap_merge.last_joined_jk, heap_merge.current_entry.jk, consume_joined,
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
   JK last_joined_jk;
   std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...> cached_records;
   std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {};

   MergedScanner<Rs...>& merged_scanner;

   PremergedJoin(MergedScanner<Rs...>& merged_scanner, std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {})
       : consume_joined(consume_joined), merged_scanner(merged_scanner)
   {
   }

   ~PremergedJoin()
   {
      std::cout << "\r~PremergedJoin: produced " << (double)produced / 1000 << "k records------------------------------------" << std::endl;
   }

   void updateAndPrintProduced(int curr_joined)
   {
      produced += curr_joined;
      if (last_joined_jk % 10 == 0) {
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
      std::visit([&](auto& actual_key) { jk = actual_key.jk; }, k);
      match_emplace_tuple(cached_records, k, v, std::index_sequence_for<Rs...>{});
      auto curr_joined = joinAndClear<JK, JR, Rs...>(cached_records, last_joined_jk, jk, consume_joined, std::index_sequence_for<Rs...>{});
      updateAndPrintProduced(curr_joined);
      return curr_joined;
   }

   int next_jk()
   {
      auto last_joined_jk_copy = last_joined_jk;
      while (last_joined_jk_copy == last_joined_jk) {
         next();
      }
      return last_joined_jk;
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
   void emplace_tuple(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                      const typename Record::Key& key,
                      const Record& rec)
   {
      std::get<I>(cached_records).emplace_back(key, rec);
   }

   template <size_t... Is>
   void match_emplace_tuple(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                            const std::variant<typename Rs::Key...>& key,
                            const std::variant<Rs...>& rec,
                            std::index_sequence<Is...>)
   {
      (([&]() {
          if (std::holds_alternative<typename Rs::Key>(key) && std::holds_alternative<Rs>(rec)) {
             emplace_tuple<Rs, Is>(cached_records, std::get<typename Rs::Key>(key), std::get<Rs>(rec));
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
      std::cout << "\r~Merge: produced " << (double)heap_merge.sifted / 1000 << "k records------------------------------------" << std::endl;
   }

   void printProgress()
   {
      if (heap_merge.last_joined_jk % 10 == 0) {
         double progress = (double)heap_merge.sifted / 1000;
         std::cout << "\rMerge: " << progress << "k records------------------------------------";
      }
   }

   template <typename MergedAdapterType, typename RecordType, typename SourceRecord>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry<JK>& entry) {
         heap_merge.current_entry = entry;
         mergedAdapter.insert(typename RecordType::Key(heap_merge.last_joined_jk, SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k)),
                              RecordType{SourceRecord::template fromBytes<SourceRecord>(entry.v)});
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