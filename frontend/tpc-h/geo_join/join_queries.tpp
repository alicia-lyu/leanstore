#pragma once
#include <cassert>
#include <chrono>
#include <cstddef>

#include "../merge.hpp"
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{
template <template <typename> class AdapterType, template <typename> class ScannerType>
struct BaseJoiner {
   std::unique_ptr<ScannerType<nation2_t>> nation_scanner;
   std::unique_ptr<ScannerType<states_t>> states_scanner;
   std::unique_ptr<ScannerType<county_t>> county_scanner;
   std::unique_ptr<ScannerType<city_t>> city_scanner;
   std::unique_ptr<ScannerType<customer2_t>> customer2_scanner;

   std::optional<BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t>> joiner_ns;
   std::optional<BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t>> joiner_nsc;
   std::optional<BinaryMergeJoin<sort_key_t, nscci_t, nsc_t, city_t>> joiner_nscci;
   std::optional<BinaryMergeJoin<sort_key_t, view_t, nscci_t, customer2_t>> final_joiner;

   BaseJoiner(AdapterType<nation2_t>& nation,
              AdapterType<states_t>& states,
              AdapterType<county_t>& county,
              AdapterType<city_t>& city,
              AdapterType<customer2_t>& customer2,
              sort_key_t seek_key = sort_key_t::max())
       : nation_scanner(nation.getScanner()),
         states_scanner(states.getScanner()),
         county_scanner(county.getScanner()),
         city_scanner(city.getScanner()),
         customer2_scanner(customer2.getScanner())
   {
      if (seek_key != sort_key_t::max()) {
         seek(seek_key);
      }
      joiner_ns.emplace([this]() { return nation_scanner->next(); }, [this]() { return states_scanner->next(); });
      joiner_nsc.emplace([this]() { return joiner_ns->next(); }, [this]() { return county_scanner->next(); });
      joiner_nscci.emplace([this]() { return joiner_nsc->next(); }, [this]() { return city_scanner->next(); });
      final_joiner.emplace([this]() { return joiner_nscci->next(); }, [this]() { return customer2_scanner->next(); });
   }

   void run() { final_joiner->run(); }

   std::optional<std::pair<view_t::Key, view_t>> next() { return final_joiner->next(); }

   sort_key_t current_jk() const { return final_joiner->current_jk(); }

   long produced() const { return final_joiner->produced(); }

   void seek(const sort_key_t& sk)
   {
      nation_scanner->seek(nation2_t::Key{sk.nationkey});
      states_scanner->seek(states_t::Key{sk.nationkey, sk.statekey});
      county_scanner->seek(county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
      city_scanner->seek(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey});
      customer2_scanner->seek(customer2_t::Key{sk});
   }
};
template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedJoiner {
   std::unique_ptr<MergedScannerType<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>> merged_scanner;
   std::optional<PremergedJoin<MergedScannerType, sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>> joiner;
   sort_key_t seek_key;

   MergedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, sort_key_t seek_key = sort_key_t::max())
       : merged_scanner(merged.template getScanner<sort_key_t, view_t>()), seek_key(seek_key)
   {
      merged_scanner->template seek<nation2_t>(nation2_t::Key{seek_key.nationkey});
      joiner.emplace(*merged_scanner);
   }

   void run() { joiner->run(); }

   std::optional<std::pair<view_t::Key, view_t>> next()
   {
      if (produced() == 0) {
         auto ret = joiner->next(seek_key);  // seek before the first result
         if (ret != std::nullopt)
            return ret;
      }
      return joiner->next();
   }

   sort_key_t current_jk() const { return joiner->current_jk(); }
   long produced() const { return joiner->produced(); }
   long consumed() const { return merged_scanner->produced; }
};

template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct Merged2Joiner {
   std::unique_ptr<MergedScannerType<sort_key_t, ns_t, nation2_t, states_t>> merged_scanner_ns;
   std::unique_ptr<MergedScannerType<sort_key_t, ccc_t, county_t, city_t, customer2_t>> merged_scanner_ccc;
   std::optional<PremergedJoin<MergedScannerType, sort_key_t, ns_t, nation2_t, states_t>> joiner_ns;
   std::optional<PremergedJoin<MergedScannerType, sort_key_t, ccc_t, county_t, city_t, customer2_t>> joiner_ccc;
   std::optional<BinaryMergeJoin<sort_key_t, view_t, ns_t, ccc_t>> joiner_view;
   sort_key_t seek_key;
   const sort_key_t seek_max;

   Merged2Joiner(MergedAdapterType<nation2_t, states_t>& merged_ns,
                 MergedAdapterType<county_t, city_t, customer2_t>& merged_ccc,
                 sort_key_t seek_key = sort_key_t::max())
       : merged_scanner_ns(merged_ns.template getScanner<sort_key_t, ns_t>()),
         merged_scanner_ccc(merged_ccc.template getScanner<sort_key_t, ccc_t>()),
         seek_key(seek_key), seek_max(sort_key_t::max())
   {
      merged_scanner_ns->template seek<nation2_t>(nation2_t::Key{seek_key.nationkey});
      merged_scanner_ccc->template seek<county_t>(county_t::Key{seek_key.nationkey, seek_key.statekey, seek_key.countykey});
      joiner_ns.emplace(*merged_scanner_ns);
      joiner_ccc.emplace(*merged_scanner_ccc);
      joiner_view.emplace([this]() { return joiner_ns->next(this->seek_key); }, [this]() { return joiner_ccc->next(this->seek_key); });
   }

   void run()
   {
      next(); // update seek_key
      joiner_view->run();
   }

   std::optional<std::pair<view_t::Key, view_t>> next()
   {
      auto ret = joiner_view->next();
      if (seek_key != seek_max)
         seek_key = seek_max;  // reset seek_key after the first result
      return ret;
   }

   sort_key_t current_jk() const { return joiner_view->current_jk(); }
   long produced() const { return joiner_view->produced(); }
};
// -------------------------------------------------------------
// ------------------------ QUERIES -----------------------------

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::query_by_view()  // scan through the join_view
{
   // warm up group-by view which is later used
   std::cout << "Warming up group-by view..." << std::endl;
   city_count_per_county.scan(
       city_count_per_county_t::Key{0, 0, 0}, [&](const city_count_per_county_t::Key&, const city_count_per_county_t&) { return true; }, []() {});
   logger.reset();
   std::cout << "GeoJoin::query_by_view()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   [[maybe_unused]] long produced = 0;
   sort_key_t start_sk = sort_key_t{0, 0, 0, 0, 0};
   join_view.scan(
       view_t::Key{start_sk},
       [&](const view_t::Key&, const view_t&) {
          TPCH::inspect_produced("Enumerating materialized join_view: ", produced);
          return true;
       },
       [&]() {});
   std::cout << "\rEnumerating materialized join_view: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "join", "view", get_view_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::query_by_merged()
{
   logger.reset();
   std::cout << "GeoJoin::query_by_merged()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   MergedJoiner<MergedAdapterType, MergedScannerType> merged_joiner(merged);
   merged_joiner.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "join", "merged", get_merged_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::query_by_base()
{
   logger.reset();
   std::cout << "GeoJoin::query_by_base()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   BaseJoiner<AdapterType, ScannerType> base_joiner(nation, states, county, city, customer2);

   base_joiner.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "join", "base", get_indexes_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::query_by_2merged()
{
   logger.reset();
   std::cout << "GeoJoin::query_by_2merged()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   Merged2Joiner<MergedAdapterType, MergedScannerType> merged2_joiner(ns, ccc);
   merged2_joiner.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "join", "2merged", get_2merged_size());
}

// -------------------------------------------------------------
// ---------------------- RANGE QUERIES ------------------------
inline void update_sk(sort_key_t& sk, const sort_key_t& found_k)
{
   if (sk.nationkey != 0)
      sk.nationkey = found_k.nationkey;
   if (sk.statekey != 0)
      sk.statekey = found_k.statekey;
   if (sk.countykey != 0)
      sk.countykey = found_k.countykey;
   if (sk.citykey != 0)
      sk.citykey = found_k.citykey;
   if (sk.custkey != 0)
      sk.custkey = found_k.custkey;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
size_t GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_view(Integer nationkey,
                                                                                                    Integer statekey,
                                                                                                    Integer countykey,
                                                                                                    Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};
   bool start = true;
   size_t produced = 0;
   join_view.scan(
       view_t::Key{sk},
       [&](const view_t::Key& vk, const view_t&) {
          if (start) {
             update_sk(sk, vk.jk);
             start = false;
          }
          if (vk.jk.match(sk) != 0) {
             return false;
          }
          produced++;
          return true;
       },
       []() {});
   // std::cout << "range_query_by_view produced " << produced << " records for sk: " << sk << std::endl;
   return produced;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
size_t GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_merged(Integer nationkey,
                                                                                                      Integer statekey,
                                                                                                      Integer countykey,
                                                                                                      Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};

   MergedJoiner<MergedAdapterType, MergedScannerType> merged_joiner(merged, sk);
   auto kv = merged_joiner.next();
   if (!kv.has_value()) {
      return 0;  // no record is scanned (too large a key)
   }
   update_sk(sk, kv->first.jk);

   while ((merged_joiner.current_jk().match(sk) == 0) || merged_joiner.current_jk() == sort_key_t::max()) {
      auto ret = merged_joiner.next();
      if (ret == std::nullopt)
         break;
   }
   size_t produced = merged_joiner.produced();
   // std::cout << "range_query_by_merged produced " << produced << " records for sk: " << sk << std::endl;
   return produced;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
size_t GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_base(Integer nationkey,
                                                                                                    Integer statekey,
                                                                                                    Integer countykey,
                                                                                                    Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};

   BaseJoiner<AdapterType, ScannerType> base_joiner(nation, states, county, city, customer2, sk);
   auto kv = base_joiner.next();
   if (!kv.has_value()) {
      return 0;  // no record is scanned (too large a key)
   }
   update_sk(sk, kv->first.jk);

   while ((base_joiner.current_jk().match(sk) == 0) || base_joiner.current_jk() == sort_key_t::max()) {
      auto ret = base_joiner.next();
      if (ret == std::nullopt)
         break;
   }
   size_t produced = base_joiner.produced();
   // std::cout << "range_query_by_base produced " << produced << " records for sk: " << sk << std::endl;
   return produced;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
size_t GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_2merged(Integer nationkey,
                                                                                                       Integer statekey,
                                                                                                       Integer countykey,
                                                                                                       Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};

   Merged2Joiner<MergedAdapterType, MergedScannerType> merged2_joiner(ns, ccc, sk);
   auto kv = merged2_joiner.next();
   if (!kv.has_value()) {
      return 0;  // no record is scanned (too large a key)
   }
   update_sk(sk, kv->first.jk);

   while ((merged2_joiner.current_jk().match(sk) == 0) || merged2_joiner.current_jk() == sort_key_t::max()) {
      auto ret = merged2_joiner.next();
      if (ret == std::nullopt)
         break;
   }
   size_t produced = merged2_joiner.produced();
   // std::cout << "range_query_by_2merged produced " << produced << " records for sk: " << sk << std::endl;
   return produced;
}

}  // namespace geo_join