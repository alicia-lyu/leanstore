#pragma once
#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <vector>
#include "Exceptions.hpp"
#include "Units.hpp"
#include "view_templates.hpp"

// MultiWayMerge performs a multi-way merge-join or merge-insert from multiple sorted sources
// Each source yields key-value pairs, merged by JK, into JoinedRec or MergedAdapterType

template <typename JK, typename JoinedRec, typename... Records>
struct MultiWayMerge {
   static constexpr size_t nways = sizeof...(Records);

   struct HeapEntry {
      JK jk;
      std::vector<std::byte> k, v;
      u8 source;
      HeapEntry() : jk(JK::max()), source(std::numeric_limits<u8>::max()) {}
      HeapEntry(JK jk, std::vector<std::byte> k, std::vector<std::byte> v, u8 source) : jk(jk), k(std::move(k)), v(std::move(v)), source(source) {}
      bool operator>(const HeapEntry& other) const { return jk > other.jk; }
   };

   std::vector<std::function<HeapEntry()>> sources;
   std::vector<std::function<void(HeapEntry&)>> consumes;
   std::tuple<std::vector<std::tuple<typename Records::Key, Records>>...> cached_records;
   long produced = 0;
   JK current_jk = JK::max();
   HeapEntry current_entry;
   std::optional<std::function<void(const typename JoinedRec::Key&, const JoinedRec&)>> consume_joined;
   std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> heap;
   std::string msg = "Joined";
   // Default: To be joined
   template <template <typename> class ScannerType>
   MultiWayMerge(ScannerType<Records>&... scanners)
       : sources(getHeapSources(scanners...)),
         consumes(getHeapConsumesToBeJoined()),
         consume_joined([](const typename JoinedRec::Key&, const JoinedRec&) {})
   {
      init();
   }

   MultiWayMerge(std::vector<std::function<HeapEntry()>>& sources)
       : sources(sources), consumes(getHeapConsumesToBeJoined()), consume_joined([](const typename JoinedRec::Key&, const JoinedRec&) {})
   {
      init();
   }

   // Explicit joinedAdapter
   template <template <typename> class AdapterType, template <typename> class ScannerType>
   MultiWayMerge(AdapterType<JoinedRec>& joinedAdapter, ScannerType<Records>&... scanners)
       : sources(getHeapSources(scanners...)),
         consumes(getHeapConsumesToBeJoined()),
         consume_joined([&](const auto& k, const auto& v) { joinedAdapter.insert(k, v); })
   {
      init();
   }
   // explicit consume_joined
   MultiWayMerge(std::function<void(const typename JoinedRec::Key&, const JoinedRec&)>& consume_joined,
                 std::vector<std::function<HeapEntry()>>& sources)
       : sources(sources), consumes(getHeapConsumesToBeJoined()), consume_joined(consume_joined)
   {
      init();
   }

   template <typename MergedAdapterType, template <typename> class ScannerType, typename... SourceRecords>
   MultiWayMerge(MergedAdapterType& mergedAdapter, ScannerType<SourceRecords>&... scanners)
       : sources(getHeapSources(scanners...)),
         consumes(getHeapConsumesToMerged<MergedAdapterType, SourceRecords...>(mergedAdapter)),
         consume_joined(std::nullopt),
         msg("Merged")
   {
      init();
   }

   ~MultiWayMerge()
   {
      std::cout << "\r~MultiWayMerge: " << msg << " " << (double)produced / 1000 << "k records------------------------------------" << std::endl;
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
      HeapEntry entry = heap.top();
      heap.pop();
      consumes.at(entry.source)(entry);

      HeapEntry next;
      if (nways == sources.size()) {
         next = sources.at(entry.source)();
      } else if (sources.size() == 1) {
         next = sources.at(0)();
      } else {
         UNREACHABLE();
      }
      if (next.jk != JK::max())
         heap.push(next);
      else if (consume_joined)
         heap = {};
   }

   template <template <typename> class ScannerType, typename RecordType>
   static std::function<HeapEntry()> getHeapSource(ScannerType<RecordType>& scanner, u8 source)
   {
      return [source, &scanner]() {
         auto kv = scanner.next();
         if (!kv)
            return HeapEntry();
         auto& [k, v] = *kv;
         return HeapEntry(SKBuilder<JK>::create(k, v), RecordType::toBytes(k), RecordType::toBytes(v), source);
      };
   }

   template <template <typename> class ScannerType, size_t... Is, typename... SourceRecords>
   static std::vector<std::function<HeapEntry()>> getHeapSources(std::index_sequence<Is...>, ScannerType<SourceRecords>&... scanners)
   {
      return {getHeapSource(scanners, Is)...};
   }

   template <template <typename> class ScannerType, typename... SourceRecords>
   static std::vector<std::function<HeapEntry()>> getHeapSources(ScannerType<SourceRecords>&... scanners)
   {
      return getHeapSources(std::index_sequence_for<SourceRecords...>{}, scanners...);
   }

   template <typename Tuple, typename Func>
   static void forEach(Tuple& tup, Func func)
   {
      std::apply([&](auto&... elems) { (..., func(elems)); }, tup);
   }

   template <std::size_t... Is>
   static void joinAndClear(std::tuple<std::vector<std::tuple<typename Records::Key, Records>>...>& cached_records,
                            JK current_jk,
                            JK current_entry_jk,
                            std::function<void(const typename JoinedRec::Key&, const JoinedRec&)>& consume_joined,
                            std::index_sequence<Is...>)
   {
      (..., ([&] {
          auto& vec = std::get<Is>(cached_records);
          using VecElem = typename std::remove_reference_t<decltype(vec)>::value_type;
          using RecordType = std::tuple_element_t<1, VecElem>;
          if (current_entry_jk.match(SKBuilder<JK>::template get<RecordType>(current_jk)) != 0) {
             join_current(cached_records, consume_joined);
             vec.clear();
          }
       }()));
   }

   void joinAndClear() { joinAndClear(cached_records, current_jk, current_entry.jk, consume_joined.value(), std::index_sequence_for<Records...>{}); }

   template <typename CurrRec, size_t I>
   auto getHeapConsumeToBeJoined()
   {
      return [this](HeapEntry& entry) {
         current_entry = entry;
         joinAndClear();
         current_jk = entry.jk;
         std::get<I>(cached_records)
             .emplace_back(typename CurrRec::Key(CurrRec::template fromBytes<typename CurrRec::Key>(entry.k)),
                           CurrRec(CurrRec::template fromBytes<CurrRec>(entry.v)));
      };
   }

   template <size_t... Is>
   std::vector<std::function<void(HeapEntry&)>> getHeapConsumesToBeJoined(std::index_sequence<Is...>)
   {
      return {getHeapConsumeToBeJoined<Records, Is>()...};
   }

   std::vector<std::function<void(HeapEntry&)>> getHeapConsumesToBeJoined()
   {
      return getHeapConsumesToBeJoined(std::index_sequence_for<Records...>{});
   }

   void printProgress()
   {
      if (current_jk % 10 == 0) {
         double progress = (double)produced / 1000;
         std::cout << "\r" << msg << " " << progress << "k records------------------------------------";
      }
   }

   template <typename MergedAdapterType, typename RecordType, typename SourceRecord>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry& entry) {
         current_entry = entry;
         current_jk = entry.jk;
         mergedAdapter.insert(typename RecordType::Key(current_jk, SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k)),
                              RecordType{SourceRecord::template fromBytes<SourceRecord>(entry.v)});
         produced++;
         printProgress();
      };
   }

   template <typename MergedAdapterType, typename... SourceRecords>
   std::vector<std::function<void(HeapEntry&)>> getHeapConsumesToMerged(MergedAdapterType& mergedAdapter)
   {
      return {getHeapConsumeToMerged<MergedAdapterType, Records, SourceRecords>(mergedAdapter)...};
   }

   int joinCurrent()
   {
      int count = join_current(cached_records, consume_joined);
      produced += count;
      printProgress();
      return count;
   }

   static int join_current(std::tuple<std::vector<std::tuple<typename Records::Key, Records>>...>& cached_records,
                           std::function<void(const typename JoinedRec::Key&, const JoinedRec&)>& consume_joined)
   {
      int count = 1;
      forEach(cached_records, [&](const auto& v) { count *= v.size(); });
      if (count == 0)
         return 0;

      std::vector<std::tuple<std::tuple<typename Records::Key, Records>...>> output(count);
      unsigned long batch_size = count;
      assignRecords(cached_records, output, &batch_size, count, std::index_sequence_for<Records...>{});

      for (auto& rec : output) {
         std::apply([&](auto&... pairs) { consume_joined(typename JoinedRec::Key{std::get<0>(pairs)...}, JoinedRec{std::get<1>(pairs)...}); }, rec);
      }
      return count;
   }

   template <size_t... Is>
   void assignRecords(std::vector<std::tuple<std::tuple<typename Records::Key, Records>...>>& output,
                      unsigned long* batch_size,
                      int total,
                      std::index_sequence<Is...>)
   {
      return assignRecords(cached_records, output, batch_size, total, std::index_sequence<Is...>{});
   }

   template <size_t... Is>
   static void assignRecords(std::tuple<std::vector<std::tuple<typename Records::Key, Records>>...>& cached_records,
                             std::vector<std::tuple<std::tuple<typename Records::Key, Records>...>>& output,
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
};
