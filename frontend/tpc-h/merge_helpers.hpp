#pragma once
#include <cstddef>
#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <variant>
#include <vector>
#include "Exceptions.hpp"
#include "Units.hpp"
#include "view_templates.hpp"
#include "logger.hpp"

template <typename JK, typename JR, typename... Rs>
struct JoinState {
   bool logging = false;
   template <typename Tuple, typename Func>
   static void for_each(Tuple& tup, Func func)
   {
      std::apply([&](auto&... elems) { (..., func(elems)); }, tup);
   }

   JK cached_jk = JK::max();
   std::tuple<std::vector<std::pair<typename Rs::Key, Rs>>...> cached_records = {};
   std::queue<std::pair<typename JR::Key, JR>> cached_joined_records = {};

   long joined = 0;

   const std::function<void(const typename JR::Key&, const JR&)> consume_joined;
   const std::string msg;

   JoinState(
       const std::string& msg,
       const std::function<void(const typename JR::Key&, const JR&)>& consume_joined = [](const typename JR::Key&, const JR&) {})
       : consume_joined(consume_joined), msg(msg)
   {
   }

   ~JoinState()
   {
      if (
          // cached_jk % 1000 == 1 || // sampling
          joined > 10000)
         std::cout << "~JoinState: joined " << (double)joined / 1000 << "k records. Ended at JK " << cached_jk
                   << std::endl;
   }

   void refresh(const JK& next_jk)
   {
      // assert(next_jk.match(cached_jk) != 0);
      int curr_joined = join_and_clear(next_jk, std::index_sequence_for<Rs...>{});
      update_print_produced(curr_joined);
   }

   template <size_t... Is>
   void assemble_joined_records(std::vector<std::tuple<std::pair<typename Rs::Key, Rs>...>>& cartesian_product,
                                unsigned long* batch_size,
                                size_t len_cartesian_product,
                                std::index_sequence<Is...>)
   {
      (..., ([&] {
          auto& vec = std::get<Is>(cached_records);
          size_t repeat = *batch_size / vec.size(), batch = len_cartesian_product / *batch_size;
          for (size_t b = 0; b < batch; ++b)
             for (size_t r = 0; r < repeat; ++r)
                for (size_t j = 0; j < vec.size(); ++j) {
                   auto& pairs = cartesian_product.at(b * *batch_size + j * repeat + r);
                   std::get<Is>(pairs) = vec.at(j);
                }
          *batch_size /= vec.size();
       })());
   }

   int join_current()
   {
      size_t len_cartesian_product = 1;  // how many records the cached records can join and produce
      for_each(cached_records, [&](const auto& v) { len_cartesian_product *= v.size(); });
      if (len_cartesian_product == 0)
         return 0;

      // assign the cached records to the cartesian product to be joined
      std::vector<std::tuple<std::pair<typename Rs::Key, Rs>...>> cartesian_product(len_cartesian_product);
      unsigned long batch_size = len_cartesian_product;
      assemble_joined_records(cartesian_product, &batch_size, len_cartesian_product, std::index_sequence_for<Rs...>{});

      // actually joining the records
      for (auto& cartesian_product_instance : cartesian_product) {
         typename JR::Key joined_key;
         JR joined_rec;
         std::apply(
             [&joined_key, &joined_rec](auto&... pairs) {
                joined_key = typename JR::Key{std::get<0>(pairs)...};
                joined_rec = JR{std::get<1>(pairs)...};
             },
             cartesian_product_instance);
         cached_joined_records.emplace(joined_key, joined_rec);
      }
      return static_cast<int>(len_cartesian_product);
   }

   std::optional<std::pair<typename JR::Key, JR>> next()
   {
      if (cached_joined_records.empty()) {
         return std::nullopt;
      }
      auto joined_pair = cached_joined_records.front();
      cached_joined_records.pop();
      consume_joined(joined_pair.first, joined_pair.second);
      return joined_pair;
   }

   bool has_next() const { return !cached_joined_records.empty(); }

   template <size_t... Is>
   int join_and_clear(const JK& next_jk, std::index_sequence<Is...>)
   {
      int joined_cnt = 0;
      (..., ([&] {
          auto& vec = std::get<Is>(cached_records);
          using VecElem = typename std::remove_reference_t<decltype(vec)>::value_type;
          using RecordType = std::tuple_element_t<1, VecElem>;
          if (next_jk.match(SKBuilder<JK>::template get<RecordType>(cached_jk)) != 0) {
             joined_cnt += join_current();
             vec.clear();
          }
       }()));
      cached_jk = next_jk;
      return joined_cnt;
   }

   long last_logged = 0;

   void update_print_produced(int curr_joined)
   {
      joined += curr_joined;
      if (logging && joined > last_logged + 1000 && FLAGS_log_progress) {
         double progress = (double)joined / 1000;
         std::cout << "\r" << msg << ": joined " << progress << "k records at JK " << cached_jk << "------------------------------------";
         last_logged = joined;
      }
      if (cached_joined_records.size() > 1000 && cached_joined_records.size() > static_cast<size_t>(curr_joined)) {
         throw std::runtime_error(
             "JoinState: too many joined records in the queue. Are you calling JoinState::next()? JoinState is only supposed to be a pipeline "
             "instead of a storage!");
      }
   }

   template <typename Record, size_t I>
   void emplace(const typename Record::Key& key, const Record& rec)
   {
      JK jk = SKBuilder<JK>::create(key, rec);
      std::get<I>(cached_records).emplace_back(key, rec);
      cached_jk = jk;
   }

   template <size_t... Is>
   void emplace(const std::variant<typename Rs::Key...>& key, const std::variant<Rs...>& rec, std::index_sequence<Is...>)
   {
      (([&]() {
          if (std::holds_alternative<typename Rs::Key>(key)) {
             assert(std::holds_alternative<Rs>(rec));
             emplace<Rs, Is>(std::get<typename Rs::Key>(key), std::get<Rs>(rec));
          }
       })(),
       ...);
   }

   void emplace(const std::variant<typename Rs::Key...>& key, const std::variant<Rs...>& rec) { emplace(key, rec, std::index_sequence_for<Rs...>{}); }
};

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
struct HeapMergeHelper {
   static constexpr size_t nways = sizeof...(Rs);

   std::vector<std::function<HeapEntry<JK>()>> sources;
   std::vector<std::function<void(HeapEntry<JK>&)>> consumes;

   std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<>> heap;
   long sifted = 0;

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