#pragma once
#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <vector>
#include "../shared/MergedScanner.hpp"
#include "Exceptions.hpp"
#include "Units.hpp"
#include "view_templates.hpp"

// MultiWayMerge performs a multi-way merge-join or merge-insert from multiple sorted sources
// Each source yields key-value pairs, merged by JK, into JR or MergedAdapterType

template <typename JK>
struct HeapEntry {
   JK jk;
   std::vector<std::byte> k, v;
   u8 source;
   HeapEntry() : jk(JK::max()), source(std::numeric_limits<u8>::max()) {}
   HeapEntry(JK jk, std::vector<std::byte> k, std::vector<std::byte> v, u8 source) : jk(jk), k(std::move(k)), v(std::move(v)), source(source) {}
   bool operator>(const HeapEntry& other) const { return jk > other.jk; }
};

template <typename Tuple, typename Func>
inline void forEach(Tuple& tup, Func func)
{
   std::apply([&](auto&... elems) { (..., func(elems)); }, tup);
}

template <typename JK, typename... Rs>
struct HeapMergeHelper {
   static constexpr size_t nways = sizeof...(Rs);

   std::vector<std::function<HeapEntry<JK>()>> sources;
   std::vector<std::function<void(HeapEntry<JK>&)>> consumes;
   std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...> cached_records;
   std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<>> heap;
   long sifted = 0;
   JK current_jk;
   HeapEntry<JK> current_entry;

   HeapMergeHelper(std::vector<std::function<HeapEntry<JK>()>> sources, std::vector<std::function<void(HeapEntry<JK>&)>> consumes)
       : sources(sources), consumes(consumes)
   {
   }

   template <template <typename> class ScannerType>
   HeapMergeHelper(std::vector<std::function<void(HeapEntry<JK>&)>> consumes, ScannerType<Rs>&... scanners)
       : HeapMergeHelper(getHeapSources(scanners...), consumes)
   {
   }

   template <template <typename> class ScannerType, typename... SourceRecords>
   HeapMergeHelper(std::vector<std::function<void(HeapEntry<JK>&)>> consumes, ScannerType<SourceRecords>&... scanners)
       : HeapMergeHelper(getHeapSources(scanners...), consumes)
   {
   }

   void run()
   {
      while (!heap.empty())
         next();
   }

   void init()
   {
      assert(consumes.size() == nways);
      for (auto& s : sources) {
         auto entry = s();
         if (entry.jk != JK::max())
            heap.push(entry);
      }
   }

   void next()
   {
      HeapEntry<JK> entry = heap.top();
      heap.pop();
      sifted++;
      consumes.at(entry.source)(entry);

      HeapEntry<JK> next;
      if (nways == sources.size()) {
         next = sources.at(entry.source)();
      } else if (sources.size() == 1) {
         next = sources.at(0)();
      } else {
         UNREACHABLE();
      }

      if (next.jk != JK::max())
         heap.push(next);
   }

   template <template <typename> class ScannerType, typename RecordType>
   static std::function<HeapEntry<JK>()> getHeapSource(ScannerType<RecordType>& scanner, u8 source)
   {
      return [source, &scanner]() {
         auto kv = scanner.next();
         if (!kv)
            return HeapEntry<JK>();
         auto& [k, v] = *kv;
         return HeapEntry<JK>(SKBuilder<JK>::create(k, v), RecordType::toBytes(k), RecordType::toBytes(v), source);
      };
   }

   template <template <typename> class ScannerType, size_t... Is, typename... SourceRecords>
   static std::vector<std::function<HeapEntry<JK>()>> getHeapSources(std::index_sequence<Is...>, ScannerType<SourceRecords>&... scanners)
   {
      return {getHeapSource(scanners, Is)...};
   }

   template <template <typename> class ScannerType, typename... SourceRecords>
   static std::vector<std::function<HeapEntry<JK>()>> getHeapSources(ScannerType<SourceRecords>&... scanners)
   {
      return getHeapSources(std::index_sequence_for<SourceRecords...>{}, scanners...);
   }
};

template <typename JK, typename JR, typename... Rs, std::size_t... Is>
inline int joinAndClear(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                        JK current_jk,
                        JK current_entry_jk,
                        std::function<void(const typename JR::Key&, const JR&)>& consume_joined,
                        std::index_sequence<Is...>)
{
   int joined_cnt = 0;
   (..., ([&] {
       auto& vec = std::get<Is>(cached_records);
       using VecElem = typename std::remove_reference_t<decltype(vec)>::value_type;
       using RecordType = std::tuple_element_t<1, VecElem>;
       if (current_entry_jk.match(SKBuilder<JK>::template get<RecordType>(current_jk)) != 0) {
          joined_cnt += join_current<JK, JR, Rs...>(cached_records, consume_joined);
          vec.clear();
       }
    }()));
    return joined_cnt;
}

template <typename JK, typename JR, typename... Rs>
inline int join_current(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                        std::function<void(const typename JR::Key&, const JR&)>& consume_joined)
{
   int count = 1;
   forEach(cached_records, [&](const auto& v) { count *= v.size(); });
   if (count == 0)
      return 0;

   std::vector<std::tuple<std::tuple<typename Rs::Key, Rs>...>> output(count);
   unsigned long batch_size = count;
   assignRecords<Rs...>(cached_records, output, &batch_size, count, std::index_sequence_for<Rs...>{});

   for (auto& rec : output) {
      std::apply([&](auto&... pairs) { consume_joined(typename JR::Key{std::get<0>(pairs)...}, JR{std::get<1>(pairs)...}); }, rec);
   }
   return count;
}

template <typename... Rs, size_t... Is>
inline void assignRecords(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                          std::vector<std::tuple<std::tuple<typename Rs::Key, Rs>...>>& output,
                          unsigned long* batch_size,
                          int total,
                          std::index_sequence<Is...>)
{
   (..., ([&] {
       auto& vec = std::get<Is>(cached_records);
       int repeat = *batch_size / vec.size(), batch = total / *batch_size;
       for (int b = 0; b < batch; ++b)
          for (int r = 0; r < repeat; ++r)
             for (int j = 0; j < vec.size(); ++j)
                std::get<Is>(output[b * *batch_size + j * repeat + r]) = vec.at(j);
       *batch_size /= vec.size();
    })());
}

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
         heap_merge.current_jk = entry.jk;
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
};

template <typename JK, typename JR, typename... Rs>
struct PremergedJoin {
   long produced = 0;
   JK current_jk;
   std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...> cached_records;
   std::function<void(const typename JR::Key&, const JR&)> consume_joined = [](const typename JR::Key&, const JR&) {};

   MergedScanner<Rs...>& merged_scanner;

   PremergedJoin(MergedScanner<Rs...>& merged_scanner, std::function<void(const typename JR::Key&, const JR&)>& consume_joined)
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
      if (current_jk % 10 == 0) {
         double progress = (double)produced / 1000;
         std::cout << "\rPremergedJoin: produced " << progress << "k records------------------------------------";
      }
   }

   void run()
   {
      while (true) {
         auto kv = merged_scanner.next();
         if (!kv) {
            break;
         }
         auto& k = kv->first;
         auto& v = kv->second;
         JK jk;
         std::visit([&](auto& actual_key) { jk = actual_key.jk; }, k);
         match_emplace_tuple(cached_records, k, v, std::index_sequence_for<Rs...>{});
         auto curr_joined = joinAndClear<JK, JR, Rs...>(cached_records, current_jk, jk, consume_joined, std::index_sequence_for<Rs...>{});
         updateAndPrintProduced(curr_joined);
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
         heap_merge.current_jk = entry.jk;
         mergedAdapter.insert(typename RecordType::Key(heap_merge.current_jk, SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k)),
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
};