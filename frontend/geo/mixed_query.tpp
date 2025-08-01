#pragma once

#include <iostream>
#include <optional>
#include "views.hpp"
#include "workload.hpp"

// SELECT nationkey, statekey, countykey, citykey, city_name, COUNT(*) as customer_count
// FROM city, customer
// WHERE ... -- all keys equal && outer join
// GROUP BY nationkey, statekey, countykey, citykey, city_name

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_view()
{
   logger.reset();
   std::cout << "GeoJoin::mixed_query_by_view()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   // scan mixed_view
   long long produced = 0;
   mixed_view.scan(
       mixed_view_t::Key{sort_key_t{}},
       [&](const mixed_view_t::Key&, const mixed_view_t&) {
          if (produced % 1000 == 0 && FLAGS_log_progress)
             std::cout << "\rEnumerating mixed_view " << produced / 1000 << "k records...";
          produced++;
          return true;
       },
       []() {});
   std::cout << "\rEnumerated mixed_view with " << produced << " records." << std::endl;
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "mixed", "mat_view", get_view_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_view()
{
   sort_key_t sk{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0};
   // point lookup in mixed_view
   mixed_view.scan(
       mixed_view_t::Key{sk},
       [&](const mixed_view_t::Key&, const mixed_view_t& v) {
          mixed_point_customer_sum += std::get<4>(v.payloads).customer_count;
          return false;
       },
       []() {});
}

template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedScannerCounter {
   std::unique_ptr<MergedScannerType<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>> scanner;
   long long produced = 0;
   using K = std::variant<nation2_t::Key, states_t::Key, county_t::Key, city_t::Key, customer_count_t::Key>;
   using V = std::variant<nation2_t, states_t, county_t, city_t, customer_count_t>;

   MergedScannerCounter(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, sort_key_t sk = sort_key_t::max())
       : scanner(merged.template getScanner<sort_key_t, view_t>())
   {
      if (sk != sort_key_t::max()) {
         scanner->template seek<city_t>(city_t::Key{sk});
      }
   }

   // all methods required by premerged join

   std::optional<std::pair<K, V>> buffered_output = std::nullopt;

   std::optional<std::pair<K, V>> next(sort_key_t last_sk = sort_key_t::max(), int customer_count = 0)
   {
      if (buffered_output.has_value()) {
         auto ret = buffered_output;
         buffered_output = std::nullopt;
         return ret;
      }
      auto kv = scanner->next();
      // get type from variant
      if (!kv.has_value()) {
         return std::nullopt;
      }
      auto [k, v] = *kv;
      sort_key_t curr_sk = SKBuilder<sort_key_t>::create(k, v);
      std::optional<K> output_k = std::nullopt;
      std::visit(overloaded{[&](const nation2_t::Key& nk) { output_k = K{nk}; }, [&](const states_t::Key& sk) { output_k = K{sk}; },
                            [&](const county_t::Key& ck) { output_k = K{ck}; }, [&](const city_t::Key& cik) { output_k = K{cik}; },
                            [&](const customer2_t::Key&) {
                               assert(last_sk == sort_key_t::max() || SKBuilder<sort_key_t>::get<customer_count_t>(curr_sk) == last_sk);
                            }},
                 k);
      if (!output_k.has_value()) {
         return next(SKBuilder<sort_key_t>::get<customer_count_t>(curr_sk), ++customer_count);
      }
      std::optional<V> output_v = std::nullopt;
      std::visit(overloaded{[&](const nation2_t& n) { output_v = V{n}; }, [&](const states_t& s) { output_v = V{s}; },
                            [&](const county_t& c) { output_v = V{c}; }, [&](const city_t& ci) { output_v = V{ci}; },
                            [&](const customer2_t&) { assert(false); }},
                 v);
      produced++;
      if (customer_count > 0) {
         buffered_output = std::make_pair(output_k.value(), output_v.value());
         return std::make_pair(K{customer_count_t::Key{last_sk}}, V{customer_count_t{customer_count}});
      } else {
         return std::make_pair(output_k.value(), output_v.value());
      }
   }

   std::optional<std::pair<K, V>> last_in_page()
   {
      auto kv = scanner->last_in_page();
      if (!kv.has_value()) {
         return std::nullopt;
      }
      auto [k, v] = *kv;
      std::optional<K> output_k = std::nullopt;
      std::optional<V> output_v = std::nullopt;
      std::visit(overloaded{[&](const nation2_t::Key& nk) { output_k = K{nk}; }, [&](const states_t::Key& sk) { output_k = K{sk}; },
                            [&](const county_t::Key& ck) { output_k = K{ck}; }, [&](const city_t::Key& cik) { output_k = K{cik}; },
                            [&](const customer2_t::Key&) {}},
                 k);
      if (!output_k.has_value()) {
         return std::nullopt;  // resort to scanning
      }
      std::visit(overloaded{[&](const nation2_t& n) { output_v = V{n}; }, [&](const states_t& s) { output_v = V{s}; },
                            [&](const county_t& c) { output_v = V{c}; }, [&](const city_t& ci) { output_v = V{ci}; }, [&](const customer2_t&) {}},
                 v);
      return std::make_pair(output_k.value(), output_v.value());
   }

   int go_to_last_in_page() { return scanner->go_to_last_in_page(); }

   template <typename R>
   void seek(const typename R::Key& key)
   {
      if constexpr (std::is_same_v<R, customer_count_t>) {
         // customer_count_t is not a real record type, so we don't seek it
         scanner->template seek<city_t>(city_t::Key{key.nationkey, key.statekey, key.countykey, key.citykey});
      } else {
         scanner->template seek<R>(key);
      }
   }
};

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedCounter {
   MergedScannerCounter<MergedAdapterType, MergedScannerType> scanner_counter;
   PremergedJoin<MergedScannerCounter<MergedAdapterType, MergedScannerType>,
                 sort_key_t,
                 mixed_view_t,
                 nation2_t,
                 states_t,
                 county_t,
                 city_t,
                 customer_count_t>
       joiner;
   AdapterType<mixed_view_t>* mixed_view_ptr = nullptr;

   sort_key_t seek_key;
   const sort_key_t seek_max = sort_key_t::max();
   MergedCounter(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, sort_key_t sk = sort_key_t::max())
       : scanner_counter(merged), joiner(scanner_counter), seek_key(sk)
   {
   }

   MergedCounter(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged,
                 AdapterType<mixed_view_t>& mixed_view,
                 sort_key_t sk = sort_key_t::max())
       : scanner_counter(merged),
         joiner(scanner_counter,
                [this](const mixed_view_t::Key& k, const mixed_view_t& v) {
                   if (mixed_view_ptr) {
                      mixed_view_ptr->insert(k, v);
                   }
                }),
         mixed_view_ptr(&mixed_view),
         seek_key(sk)
   {
   }

   void run() { joiner.run(); }

   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next()
   {
      auto ret = joiner.next(seek_key);
      if (seek_key != seek_max) {
         seek_key = seek_max;  // reset seek_key after the first result
      }
      return ret;
   }

   sort_key_t current_jk() const { return joiner.current_jk(); }
   long produced() const { return joiner.produced(); }
   long consumed() const { return scanner_counter.produced; }

   bool went_past(const sort_key_t& sk) const { return joiner.went_past(sk); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_merged()
{
   logger.reset();
   std::cout << "GeoJoin::mixed_query_by_merged()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   MergedCounter<AdapterType, MergedAdapterType, MergedScannerType> counter(merged);
   counter.run();
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "mixed", "merged_idx", get_merged_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_merged()
{
   sort_key_t sk{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0};
   MergedCounter<AdapterType, MergedAdapterType, MergedScannerType> counter(merged, sk);
   auto kv = counter.next();
   mixed_point_tx_count++;
   if (kv != std::nullopt) {
      mixed_point_customer_sum += std::get<4>(kv->second.payloads).customer_count;
   }
}

template <template <typename> class AdapterType, template <typename> class ScannerType>
struct BaseCounter {
   std::unique_ptr<ScannerType<nation2_t>> nation;
   std::unique_ptr<ScannerType<states_t>> states;
   std::unique_ptr<ScannerType<county_t>> county;
   std::unique_ptr<ScannerType<city_t>> city;
   std::unique_ptr<ScannerType<customer2_t>> customer;

   std::optional<BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t>> joiner_ns;
   std::optional<BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t>> joiner_nsc;
   std::optional<BinaryMergeJoin<sort_key_t, nscci_t, nsc_t, city_t>> joiner_nscci;
   std::optional<BinaryMergeJoin<sort_key_t, mixed_view_t, nscci_t, customer_count_t>> final_joiner;

   BaseCounter(AdapterType<nation2_t>& n,
               AdapterType<states_t>& s,
               AdapterType<county_t>& c,
               AdapterType<city_t>& ci,
               AdapterType<customer2_t>& cu,
               sort_key_t sk = sort_key_t::max())
       : nation(n.getScanner()), states(s.getScanner()), county(c.getScanner()), city(ci.getScanner()), customer(cu.getScanner())
   {
      if (sk != sort_key_t::max()) {
         seek(sk);
      }
      joiner_ns.emplace([this]() { return nation->next(); }, [this]() { return states->next(); });
      joiner_nsc.emplace([this]() { return joiner_ns->next(); }, [this]() { return county->next(); });
      joiner_nscci.emplace([this]() { return joiner_nsc->next(); }, [this]() { return city->next(); });
      final_joiner.emplace([this]() { return joiner_nscci->next(); },
                           [this]() -> std::optional<std::pair<customer_count_t::Key, customer_count_t>> {
                              int customer_count = 0;
                              std::optional<customer_count_t::Key> curr_key = std::nullopt;
                              while (true) {
                                 auto kv = customer->next();
                                 if (!kv.has_value()) {
                                    return std::nullopt;
                                 }
                                 auto [k, v] = *kv;
                                 if (curr_key == std::nullopt) {
                                    curr_key = customer_count_t::Key{k};
                                 } else if (curr_key->nationkey != k.nationkey || curr_key->statekey != k.statekey ||
                                            curr_key->countykey != k.countykey || curr_key->citykey != k.citykey) {
                                    customer->after_seek = true;  // rescan this customer
                                    return std::make_pair(*curr_key, customer_count_t{customer_count});
                                 }
                                 customer_count++;
                              }
                           });
   }

   void run() { final_joiner->run(); }

   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next() { return final_joiner->next(); }

   sort_key_t current_jk() const { return final_joiner->jk_to_join(); }

   long produced() const { return final_joiner->produced(); }

   void seek(const sort_key_t& sk)
   {
      [[maybe_unused]] auto n_ret = nation->seek(nation2_t::Key{sk});
      [[maybe_unused]] auto s_ret = states->seek(states_t::Key{sk});
      [[maybe_unused]] auto c_ret = county->seek(county_t::Key{sk});
      [[maybe_unused]] auto ci_ret = city->seek(city_t::Key{sk});
      [[maybe_unused]] auto cu_ret = customer->seek(customer2_t::Key{sk});
   }

   bool went_past(const sort_key_t& sk) { return final_joiner->went_past(sk); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_base()
{
   logger.reset();
   std::cout << "GeoJoin::mixed_query_by_base()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   BaseCounter<AdapterType, ScannerType> counter(nation, states, county, city, customer2);
   counter.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "mixed", "base_idx", get_indexes_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_base()
{
   BaseCounter<AdapterType, ScannerType> counter(
       nation, states, county, city, customer2,
       sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
   auto kv = counter.next();
   mixed_point_tx_count++;
   if (kv != std::nullopt) {
      mixed_point_customer_sum += std::get<4>(kv->second.payloads).customer_count;
   }
}

}  // namespace geo_join