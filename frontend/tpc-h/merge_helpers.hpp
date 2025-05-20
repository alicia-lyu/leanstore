#pragma once
#include <functional>
#include <limits>
#include <queue>
#include <vector>
#include "Exceptions.hpp"
#include "Units.hpp"
#include "view_templates.hpp"

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
   JK current_jk = JK::max();
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

   void next_jk()
   {
      auto current_jk_copy = current_jk;
      while (!heap.empty() && current_jk_copy == current_jk) {
         next();
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
   std::function<HeapEntry<JK>()> getHeapSource(ScannerType<RecordType>& scanner, u8 source)
   {
      return [source, this, &scanner]() {
         auto kv = current_jk == JK::max() ? scanner.current() : scanner.next();
         if (!kv)
            return HeapEntry<JK>();
         auto& [k, v] = *kv;
         return HeapEntry<JK>(current_jk, RecordType::toBytes(k), RecordType::toBytes(v), source);
      };
   }

   template <template <typename> class ScannerType, size_t... Is, typename... SourceRecords>
   std::vector<std::function<HeapEntry<JK>()>> getHeapSources(std::index_sequence<Is...>, ScannerType<SourceRecords>&... scanners)
   {
      return {getHeapSource(scanners, Is)...};
   }

   template <template <typename> class ScannerType, typename... SourceRecords>
   std::vector<std::function<HeapEntry<JK>()>> getHeapSources(ScannerType<SourceRecords>&... scanners)
   {
      return getHeapSources(std::index_sequence_for<SourceRecords...>{}, scanners...);
   }
};

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
             for (size_t j = 0; j < vec.size(); ++j)
                std::get<Is>(output[b * *batch_size + j * repeat + r]) = vec.at(j);
       *batch_size /= vec.size();
    })());
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

template <typename JK, typename JR, typename... Rs, std::size_t... Is>
inline int joinAndClear(std::tuple<std::vector<std::tuple<typename Rs::Key, Rs>>...>& cached_records,
                        JK& current_jk,
                        JK& current_entry_jk,
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
   current_jk = current_entry_jk;
   return joined_cnt;
}