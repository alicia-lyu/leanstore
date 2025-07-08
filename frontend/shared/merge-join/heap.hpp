#pragma once
#include <functional>
#include <limits>
#include <queue>
#include <vector>
#include "Exceptions.hpp"
#include "Units.hpp"

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
struct Heap {
   static constexpr size_t nways = sizeof...(Rs);

   std::vector<std::function<HeapEntry<JK>()>> sources;
   std::vector<std::function<void(HeapEntry<JK>&)>> consumes;

   std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<>> heap;
   long sifted = 0;

   Heap(std::vector<std::function<HeapEntry<JK>()>> sources, std::vector<std::function<void(HeapEntry<JK>&)>> consumes)
       : sources(sources), consumes(consumes)
   {
   }

   template <template <typename> class ScannerType>
   Heap(std::vector<std::function<void(HeapEntry<JK>&)>> consumes, ScannerType<Rs>&... scanners)
       : Heap(getHeapSources(scanners...), consumes)
   {
   }

   template <template <typename> class ScannerType, typename... SourceRecords>
   Heap(std::vector<std::function<void(HeapEntry<JK>&)>> consumes, ScannerType<SourceRecords>&... scanners)
       : Heap(getHeapSources(scanners...), consumes)
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

   bool has_next() const { return !heap.empty(); }

   template <template <typename> class ScannerType, typename RecordType>
   std::function<HeapEntry<JK>()> getHeapSource(ScannerType<RecordType>& scanner, u8 source)
   {
      return [source, this, &scanner]() {
         auto kv = scanner.next();
         if (!kv)
            return HeapEntry<JK>();
         auto& [k, v] = *kv;
         return HeapEntry<JK>(SKBuilder<JK>(k, v), RecordType::toBytes(k), RecordType::toBytes(v), source);
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