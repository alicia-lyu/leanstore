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

template <typename JK, typename... Rs>
struct HeapMergeImpl {
   static constexpr size_t nways = sizeof...(Rs);

   std::vector<std::function<HeapEntry<JK>()>> sources;
   std::vector<std::function<void(HeapEntry<JK>&)>> consumes;
   std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...> cached_records;
   std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<>> heap;
   long produced = 0;
   JK current_jk;
   HeapEntry<JK> current_entry;

   HeapMergeImpl(std::vector<std::function<HeapEntry<JK>()>> sources, std::vector<std::function<void(HeapEntry<JK>&)>> consumes)
       : sources(sources), consumes(consumes)
   {
   }

   template <template <typename> class ScannerType>
   HeapMergeImpl(std::vector<std::function<void(HeapEntry<JK>&)>> consumes, ScannerType<Rs>&... scanners)
       : HeapMergeImpl(getHeapSources(scanners...), consumes)
   {
   }

   template <template <typename> class ScannerType, typename... SourceRecords>
   HeapMergeImpl(std::vector<std::function<void(HeapEntry<JK>&)>> consumes, ScannerType<SourceRecords>&... scanners)
       : HeapMergeImpl(getHeapSources(scanners...), consumes)
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

   template <typename Tuple, typename Func>
   static void forEach(Tuple& tup, Func func)
   {
      std::apply([&](auto&... elems) { (..., func(elems)); }, tup);
   }
};

template <typename JK, typename JR, size_t ST, typename... Rs>
struct MergeJoinImpl : public HeapMergeImpl<JK, Rs...> {
   using Base = HeapMergeImpl<JK, Rs...>;

   std::optional<std::function<void(const typename JR::Key&, const JR&)>> consume_joined;

   template <template <typename> class ScannerType>
   MergeJoinImpl(ScannerType<Rs>&... scanners)
       : Base(getHeapConsumesToBeJoined(), scanners...), consume_joined([](const typename JR::Key&, const JR&) {})
   {
      this->init();
   }

   MergeJoinImpl(std::vector<std::function<HeapEntry<JK>()>>& sources)
       : Base(sources, getHeapConsumesToBeJoined()), consume_joined([](const typename JR::Key&, const JR&) {})
   {
      this->init();
   }

   template <template <typename> class AdapterType, template <typename> class ScannerType>
   MergeJoinImpl(AdapterType<JR>& joinedAdapter, ScannerType<Rs>&... scanners)
       : Base(getHeapConsumesToBeJoined(), scanners...),
         consume_joined([&](const auto& k, const auto& v) { joinedAdapter.insert(k, v); })
   {
      this->init();
   }

   MergeJoinImpl(std::function<void(const typename JR::Key&, const JR&)>& consume_joined, std::vector<std::function<HeapEntry<JK>()>>& sources)
       : Base(sources, getHeapConsumesToBeJoined()), consume_joined(consume_joined)
   {
      this->init();
   }

   ~MergeJoinImpl()
   {
      std::cout << "\r~MergeJoin: produced " << (double)this->produced / 1000 << "k records------------------------------------" << std::endl;
   }

   void printProgress()
   {
      if (this->current_jk % 10 == 0) {
         double progress = (double)this->produced / 1000;
         std::cout << "\rMergeJoin: produced " << progress << "k records------------------------------------";
      }
   }

   template <std::size_t... Is>
   static void joinAndClear(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                            JK current_jk,
                            JK current_entry_jk,
                            std::function<void(const typename JR::Key&, const JR&)>& consume_joined,
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

   void joinAndClear() { joinAndClear(this->cached_records, this->current_jk, this->current_entry.jk, consume_joined.value(), std::index_sequence_for<Rs...>{}); }

   template <typename CurrRec, size_t I>
   auto getHeapConsumeToBeJoined()
   {
      return [this](HeapEntry<JK>& entry) {
         this->current_entry = entry;
         joinAndClear();
         this->current_jk = entry.jk;
         std::get<I>(this->cached_records)
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

   int joinCurrent()
   {
      int count = join_current(this->cached_records, consume_joined);
      this->produced += count;
      printProgress();
      return count;
   }

   static int join_current(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                           std::function<void(const typename JR::Key&, const JR&)>& consume_joined)
   {
      int count = 1;
      Base::forEach(cached_records, [&](const auto& v) { count *= v.size(); });
      if (count == 0)
         return 0;

      std::vector<std::tuple<std::tuple<typename Rs::Key, Rs>...>> output(count);
      unsigned long batch_size = count;
      assignRecords(cached_records, output, &batch_size, count, std::index_sequence_for<Rs...>{});

      for (auto& rec : output) {
         std::apply([&](auto&... pairs) { consume_joined(typename JR::Key{std::get<0>(pairs)...}, JR{std::get<1>(pairs)...}); }, rec);
      }
      return count;
   }

   template <size_t... Is>
   void assignRecords(std::vector<std::tuple<std::tuple<typename Rs::Key, Rs>...>>& output,
                      unsigned long* batch_size,
                      int total,
                      std::index_sequence<Is...>)
   {
      return assignRecords(this->cached_records, output, batch_size, total, std::index_sequence<Is...>{});
   }

   template <size_t... Is>
   static void assignRecords(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
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
   
};

template <typename JK, typename... Rs>
struct Merge : public HeapMergeImpl<JK, Rs...> {
   using Base = HeapMergeImpl<JK, Rs...>;
   

   template <typename MergedAdapterType, template <typename> class ScannerType, typename... SourceRecords>
   Merge(MergedAdapterType& mergedAdapter, ScannerType<SourceRecords>&... scanners)
       : Base(getHeapConsumesToMerged<MergedAdapterType, SourceRecords...>(mergedAdapter), scanners...)
   {
      this->init();
   }

   void printProgress()
   {
      if (this->current_jk % 10 == 0) {
         double progress = (double)this->produced / 1000;
         std::cout << "\rMerge: " << progress << "k records------------------------------------";
      }
   }

   template <typename MergedAdapterType, typename RecordType, typename SourceRecord>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry<JK>& entry) {
         this->current_entry = entry;
         this->current_jk = entry.jk;
         mergedAdapter.insert(typename RecordType::Key(this->current_jk, SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k)),
                              RecordType{SourceRecord::template fromBytes<SourceRecord>(entry.v)});
         this->produced++;
         printProgress();
      };
   }

   template <typename MergedAdapterType, typename... SourceRecords>
   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToMerged(MergedAdapterType& mergedAdapter)
   {
      return {getHeapConsumeToMerged<MergedAdapterType, Rs, SourceRecords>(mergedAdapter)...};
   }
};

template <typename JK, typename JR, typename...Rs>
struct MergeJoin: public MergeJoinImpl<JK, JR, sizeof...(Rs), Rs...> {
   using MergeJoinImpl<JK, JR, sizeof...(Rs), Rs...>::MergeJoinImpl;
};

template <typename JK, typename JR, typename R1, typename R2>
struct BinaryJoin : public MergeJoinImpl<JK, JR, 2, R1, R2> {
};