#pragma once
#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <vector>
#include "Exceptions.hpp"
#include "Units.hpp"
#include "Views.hpp"

template <typename JK, typename JoinedRec, typename... Records>
struct MultiWayMerge {
   static constexpr size_t nways = sizeof...(Records);
   struct HeapEntry {
      JK jk;
      std::vector<std::byte> k;
      std::vector<std::byte> v;
      u8 source;

      HeapEntry() : jk(JK::max()), k(), v(), source(std::numeric_limits<u8>::max()) {}

      HeapEntry(JK jk, std::vector<std::byte> k, std::vector<std::byte> v, u8 source) : jk(jk), k(k), v(v), source(source) {}

      bool operator>(const HeapEntry& other) const { return jk > other.jk; }
   };

   std::vector<std::function<HeapEntry()>>& sources;
   std::vector<std::function<void(HeapEntry&)>> consumes;
   std::tuple<std::vector<std::tuple<typename Records::Key, Records>>...> cached_records;
   long produced;
   JK current_jk;
   HeapEntry current_entry;
   std::optional<std::function<void(const typename JoinedRec::Key&, const JoinedRec&)>> consume_joined;
   std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap;

   template <template <typename> class AdapterType>
   MultiWayMerge(std::vector<std::function<HeapEntry()>>& sources, std::optional<AdapterType<JoinedRec>>& joinedAdapter)
       : sources(sources),
         consumes(getHeapConsumesToBeJoined()),
         cached_records(),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined([&joinedAdapter](const typename JoinedRec::Key& k, const JoinedRec& v) {
            if (joinedAdapter.has_value()) {
               joinedAdapter->insert(k, v);
            }
         }),
         heap()
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   void fillHeap()
   {
      for (auto& s : sources) {
         auto entry = s();
         if (entry.jk == JK::max()) {
            std::cout << "Warning: source is empty" << std::endl;
            continue;
         }
         heap.push(entry);
      }
   }

   template <typename MergedAdapterType>
   MultiWayMerge(std::vector<std::function<HeapEntry()>>& sources, MergedAdapterType& mergedAdapter)
       : sources(sources),
         consumes(getHeapConsumesToMerged(mergedAdapter)),
         cached_records({}),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined(nullptr),
         heap()
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   void next()
   {
      HeapEntry entry = heap.top();
      heap.pop();
      consumes.at(entry.source)(entry);
      HeapEntry next;
      if (nways == sources.size()) {
         next = sources.at(entry.source)();
      } else if (nways == 1) {
         next = sources.at(0)();
      } else {
         UNREACHABLE();
      }

      if (next.jk != JK::max()) {
         heap.push(next);
      }
   }

   void run()
   {
      while (!heap.empty()) {
         next();
      }
   }

   template <template <typename> class AdapterType, typename RecordType>
   static std::function<HeapEntry()> getHeapSource(AdapterType<RecordType>& adapter, u8 source)
   {
      return [source, &adapter]() {
         auto kv = adapter.next();
         if (kv == std::nullopt) {
            return HeapEntry();
         }
         auto& [k, v] = *kv;
         JK jk = JKBuilder<JK>::create(k, v);
         return HeapEntry(jk, RecordType::toBytes(k), RecordType::toBytes(v), source);
      };
   }

   template <template <typename> class AdapterType, size_t... Is>
   static std::vector<std::function<HeapEntry()>> getHeapSources(AdapterType<Records>&... adapters, std::index_sequence<Is...>)
   {
      return {getHeapSource<AdapterType, Records>(adapters, Is)...};
   }

   template <template <typename> class AdapterType>
   static std::vector<std::function<HeapEntry()>> getHeapSources(AdapterType<Records>&... adapters)
   {
      return getHeapSources(adapters..., std::index_sequence_for<Records...>{});
   }

   template <typename Tuple, typename Func>
   static void forEach(Tuple& tup, Func func)
   {
      std::apply(
          [&func](auto&... elems) {
             (..., func(elems));  // Fold expression to call `func` on each element
          },
          tup);
   }

   template <std::size_t... Is>
   void joinAndClear(std::index_sequence<Is...>)
   {
      (..., ([&] {
          auto& vec = std::get<Is>(cached_records);
          using RecordType = typename std::tuple_element<1, decltype(vec)>::type;
          if (current_entry.jk.match(JKBuilder<JK>::template get<RecordType>(current_jk)) != 0) {
             joinCurrent();
             vec.clear();
          }
       }()));
   }

   template <typename CurrRec, size_t I>
   auto getHeapConsumeToBeJoined()
   {
      return [this](HeapEntry& entry) {
         joinAndClear(std::index_sequence_for<Records...>{});
         current_jk = entry.jk;
         current_entry = entry;
         cached_records.get<I> push_back(CurrRec::template fromBytes<CurrRec>(entry.v));
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

   void printProgress(std::string msg)
   {
      if (produced % 1000 == 0) {
         std::cout << "\r" << msg << " " << produced / 1000 << "k records------------------------------------";
      }
   }

   template <typename CurrRec, typename MergedAdapterType>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry& entry) {
         typename CurrRec::Key k_new(CurrRec::template fromBytes<typename CurrRec::Key>(entry.k));
         CurrRec v_new(CurrRec::template fromBytes<CurrRec>(entry.v));
         mergedAdapter.insert(k_new, v_new);
         produced++;
         printProgress("Merged");
      };
   }

   template <typename MergedAdapterType>
   std::vector<std::function<void(HeapEntry&)>> getHeapConsumesToMerged(MergedAdapterType& mergedAdapter)
   {
      return {getHeapConsumeToMerged<Records>(mergedAdapter)...};
   }

   // calculate cartesian produce of current cached records
   int joinCurrent()
   {
      int curr_joined_cnt = 1;
      forEach(cached_records, [&](const auto& vec) {
         // std::cout << vec.size() << ", ";
         curr_joined_cnt *= vec.size();
      });
      std::vector<std::tuple<std::tuple<typename Records::Key, Records>...>> matched_records(curr_joined_cnt);
      produced += curr_joined_cnt;
      printProgress("Joined");
      if (curr_joined_cnt == 0) {
         return 0;
      }
      unsigned long batch_size = curr_joined_cnt;
      assignRecords(matched_records, cached_records, &batch_size, curr_joined_cnt, std::index_sequence_for<Records...>{});
      for (auto& rec : matched_records) {
         std::apply(
             [&](auto&... pairs) {
                auto joined_key = JoinedRec::Key(pairs.first...);
                auto joined_rec = JoinedRec(pairs.second...);
                consume_joined(joined_rec);
             },
             rec);
      }
      return curr_joined_cnt;
   }

   template <size_t... Is>
   void assignRecords(std::vector<std::tuple<std::tuple<typename Records::Key, Records>...>>& matched_records,
                      unsigned long* batch_size,
                      int curr_joined_cnt,
                      std::index_sequence<Is...>)
   {
      (
          [&] {
             auto& vec = std::get<Is>(cached_records);
             int repeat = *batch_size / vec.size();  // repeat times for each vec in a batch
             int batch_cnt = curr_joined_cnt / *batch_size;
             for (int x = 0; x < batch_cnt; x++) {
                for (int i = 0; i < repeat; i++) {
                   for (int j = 0; j < vec.size(); j++) {
                      std::get<Is>(matched_records.at(x * *batch_size + j * repeat + i)) = vec.at(j);
                   }
                }
             }
             *batch_size /= vec.size();
          }(),
          ...);
   }
};