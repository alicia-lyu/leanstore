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

   std::vector<std::function<HeapEntry()>> sources;
   std::vector<std::function<void(HeapEntry&)>> consumes;
   std::tuple<std::vector<std::tuple<typename Records::Key, Records>>...> cached_records;
   long produced;
   JK current_jk;
   HeapEntry current_entry;
   std::optional<std::function<void(const typename JoinedRec::Key&, const JoinedRec&)>> consume_joined;
   std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap;
   std::string msg;

   // Default: To be joined
   template <template <typename> class AdapterType>
   MultiWayMerge(AdapterType<Records>&... adapters)
       : sources(getHeapSources(adapters...)),
         consumes(getHeapConsumesToBeJoined()),
         cached_records(),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined([](const typename JoinedRec::Key&, const JoinedRec&) {}),
         heap(),
         msg("Joined")
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   MultiWayMerge(std::vector<std::function<HeapEntry()>>& sources)
       : sources(sources),
         consumes(getHeapConsumesToBeJoined()),
         cached_records(),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined([](const typename JoinedRec::Key&, const JoinedRec&) {}),
         heap(),
         msg("Joined")
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   // Explicit joinedAdapter
   template <template <typename> class AdapterType>
   MultiWayMerge(AdapterType<JoinedRec>& joinedAdapter, AdapterType<Records>&... adapters)
       : sources(getHeapSources(adapters...)),
         consumes(getHeapConsumesToBeJoined()),
         cached_records(),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined([&joinedAdapter](const typename JoinedRec::Key& k, const JoinedRec& v) { joinedAdapter.insert(k, v); }),
         heap(),
         msg("Joined")
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   template <template <typename> class AdapterType>
   MultiWayMerge(AdapterType<JoinedRec>& joinedAdapter, std::vector<std::function<HeapEntry()>>& sources)
       : sources(sources),
         consumes(getHeapConsumesToBeJoined()),
         cached_records(),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined([&joinedAdapter](const typename JoinedRec::Key& k, const JoinedRec& v) { joinedAdapter->insert(k, v); }),
         heap(),
         msg("Joined")
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   // explicit consume_joined

   MultiWayMerge(std::function<void(const typename JoinedRec::Key&, const JoinedRec&)>& consume_joined,
                 std::vector<std::function<HeapEntry()>>& sources)
       : sources(sources),
         consumes(getHeapConsumesToBeJoined()),
         cached_records(),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined(consume_joined),
         heap(),
         msg("Joined")
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   // Explicit mergedAdapter
   template <typename MergedAdapterType, template <typename> class AdapterType, typename... SourceRecords>
   MultiWayMerge(MergedAdapterType& mergedAdapter, AdapterType<SourceRecords>&... adapters)
       : sources(getHeapSources(adapters...)),
         consumes(getHeapConsumesToMerged<MergedAdapterType, SourceRecords...>(mergedAdapter)),
         cached_records({}),
         produced(0),
         current_jk(JK::max()),
         current_entry(),
         consume_joined(std::nullopt),
         heap(),
         msg("Merged")
   {
      assert(consumes.size() == nways);
      fillHeap();
   }

   ~MultiWayMerge() { 
      double progress = (double)produced / 1000;
      std::cout << "\r~MultiWayMerge: " << msg << " " << progress << "k records------------------------------------" << std::endl; 
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

      if (next.jk != JK::max()) {
         heap.push(next);
      } else if (consume_joined.has_value()) { // no records to be joined at all
         while (!heap.empty()) {
            heap.pop();
         }
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

   template <template <typename> class AdapterType, size_t... Is, typename... SourceRecords>
   static std::vector<std::function<HeapEntry()>> getHeapSources(std::index_sequence<Is...>, AdapterType<SourceRecords>&... adapters)
   {
      return {getHeapSource<AdapterType, SourceRecords>(adapters, Is)...};
   }

   template <template <typename> class AdapterType, typename... SourceRecords>
   static std::vector<std::function<HeapEntry()>> getHeapSources(AdapterType<SourceRecords>&... adapters)
   {
      return getHeapSources(std::index_sequence_for<SourceRecords...>{}, adapters...);
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
          using VecElem = typename std::tuple_element<Is, decltype(cached_records)>::type::value_type;
          using RecordType = std::tuple_element_t<1, VecElem>;
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
         current_entry = entry;
         joinAndClear(std::index_sequence_for<Records...>{});
         current_jk = entry.jk;
         std::get<I>(cached_records)
             .push_back(std::make_tuple(typename CurrRec::Key(CurrRec::template fromBytes<typename CurrRec::Key>(entry.k)),
                                        CurrRec(CurrRec::template fromBytes<CurrRec>(entry.v))));
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
      if (produced % 1000 == 0) {
         double progress = (double)produced / 1000;
         std::cout << "\r" << msg << " " << progress << "k records------------------------------------";
      }
   }

   template <typename MergedAdapterType, typename RecordType, typename SourceRecord>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry& entry) {
         current_jk = entry.jk;
         current_entry = entry;
         typename RecordType::Key k_new(current_jk, SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k));
         RecordType v_new(SourceRecord::template fromBytes<SourceRecord>(entry.v));
         mergedAdapter.insert(k_new, v_new);
         printProgress();
         produced++;
      };
   }

   template <typename MergedAdapterType, typename... SourceRecords>
   std::vector<std::function<void(HeapEntry&)>> getHeapConsumesToMerged(MergedAdapterType& mergedAdapter)
   {
      return {getHeapConsumeToMerged<MergedAdapterType, Records, SourceRecords>(mergedAdapter)...};
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
      printProgress();
      produced += curr_joined_cnt;
      if (curr_joined_cnt == 0) {
         return 0;
      }
      unsigned long batch_size = curr_joined_cnt;
      assignRecords(matched_records, &batch_size, curr_joined_cnt, std::index_sequence_for<Records...>{});
      for (auto& rec : matched_records) {
         std::apply(
             [&](auto&... pairs) {
                auto joined_key = typename JoinedRec::Key(std::get<0>(pairs)...);
                auto joined_rec = JoinedRec(std::get<1>(pairs)...);
                (*consume_joined)(joined_key, joined_rec);
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