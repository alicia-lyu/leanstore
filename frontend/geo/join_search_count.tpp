#pragma once
#include <cassert>
#include <chrono>
#include <cstddef>

#include "../shared/merge-join/binary_merge_join.hpp"
#include "../shared/merge-join/hash_join.hpp"
#include "../shared/merge-join/premerged_join.hpp"
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

   sort_key_t current_jk() const { return final_joiner->jk_to_join(); }

   long produced() const { return final_joiner->produced(); }

   void seek(const sort_key_t& sk)
   {
      auto n_ret = nation_scanner->seek(nation2_t::Key{sk});
      auto s_ret = states_scanner->seek(states_t::Key{sk});
      auto c_ret = county_scanner->seek(county_t::Key{sk});
      auto ci_ret = city_scanner->seek(city_t::Key{sk});
      auto cu_ret = customer2_scanner->seek(customer2_t::Key{sk});
      if ((!n_ret || !s_ret || !c_ret || !ci_ret || !cu_ret) && sk.nationkey != 25) {  // HARDCODED max nationkey
         std::cerr << "WARNING: BaseJoiner::seek() failed to seek to " << sk << "nation: " << n_ret << ", states: " << s_ret << ", county: " << c_ret
                   << ", city: " << ci_ret << ", customer2: " << cu_ret << std::endl;
      }
   }

   bool went_past(const sort_key_t& sk) { return final_joiner->went_past(sk); }
};

template <template <typename> class AdapterType, template <typename> class ScannerType>
struct HashJoiner {
   std::unique_ptr<ScannerType<nation2_t>> nation_scanner;
   std::unique_ptr<ScannerType<states_t>> states_scanner;
   std::unique_ptr<ScannerType<county_t>> county_scanner;
   std::unique_ptr<ScannerType<city_t>> city_scanner;
   std::unique_ptr<ScannerType<customer2_t>> customer2_scanner;
   std::optional<HashJoin<sort_key_t, ns_t, nation2_t, states_t>> joiner_ns;
   std::optional<HashJoin<sort_key_t, nsc_t, ns_t, county_t>> joiner_nsc;
   std::optional<HashJoin<sort_key_t, nscci_t, nsc_t, city_t>> joiner_nscci;
   std::optional<HashJoin<sort_key_t, view_t, nscci_t, customer2_t>> final_joiner;

   HashJoiner(AdapterType<nation2_t>& nation,
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
      joiner_ns.emplace([this]() { return nation_scanner->next(); }, [this]() { return states_scanner->next(); }, seek_key);
      joiner_nsc.emplace([this]() { return joiner_ns->next(); }, [this]() { return county_scanner->next(); }, seek_key);
      joiner_nscci.emplace([this]() { return joiner_nsc->next(); }, [this]() { return city_scanner->next(); }, seek_key);
      final_joiner.emplace([this]() { return joiner_nscci->next(); }, [this]() { return customer2_scanner->next(); }, seek_key);
   }

   void run() { final_joiner->run(); }

   std::optional<std::pair<view_t::Key, view_t>> next() { return final_joiner->next(); }

   long produced() const { return final_joiner->produced(); }

   void seek(const sort_key_t& sk)
   {
      nation_scanner->seek(nation2_t::Key{sk});
      states_scanner->seek(states_t::Key{sk});
      county_scanner->seek(county_t::Key{sk});
      city_scanner->seek(city_t::Key{sk});
      customer2_scanner->seek(customer2_t::Key{sk});
   }
};

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedJoiner {
   std::unique_ptr<MergedScannerType<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>> merged_scanner;
   std::optional<PremergedJoin<MergedScannerType<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>,
                               sort_key_t,
                               view_t,
                               nation2_t,
                               states_t,
                               county_t,
                               city_t,
                               customer2_t>>
       joiner;
   sort_key_t seek_key;
   const sort_key_t seek_max = sort_key_t::max();

   MergedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, sort_key_t seek_key = sort_key_t::max())
       : merged_scanner(merged.template getScanner<sort_key_t, view_t>()), seek_key(seek_key)
   {
      // seek handled by PremergedJoin
      joiner.emplace(*merged_scanner);
   }

   MergedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, AdapterType<view_t>& join_view)
       : merged_scanner(merged.template getScanner<sort_key_t, view_t>()), seek_key(sort_key_t::max())
   {
      // seek handled by PremergedJoin
      joiner.emplace(*merged_scanner, join_view);
   }

   void run() { joiner->run(); }

   std::optional<std::pair<view_t::Key, view_t>> next()
   {
      auto ret = joiner->next(seek_key);
      if (seek_key != seek_max) {
         seek_key = seek_max;  // reset seek_key after the first result
      }
      return ret;
   }

   sort_key_t current_jk() const { return joiner->current_jk(); }
   long produced() const { return joiner->produced(); }
   long consumed() const { return merged_scanner->produced; }

   bool went_past(const sort_key_t& sk) const { return joiner->went_past(sk); }
};
// -------------------------------------------------------------
// ------------------------ QUERIES -----------------------------

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::query_by_view()  // scan through the join_view
{
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
   std::cout << "Enumerated materialized join_view: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "join", "mat_view", get_view_size());
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

   MergedJoiner<AdapterType, MergedAdapterType, MergedScannerType> merged_joiner(merged);
   merged_joiner.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "join", "merged_idx", get_merged_size());
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
   logger.log(t, "join", "base_idx", get_indexes_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::query_hash()
{
   logger.reset();
   std::cout << "GeoJoin::query_hash()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   HashJoiner<AdapterType, ScannerType> hash_joiner(nation, states, county, city, customer2);

   hash_joiner.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "join", "hash", get_indexes_size());
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
long GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_view(Integer nationkey,
                                                                                                  Integer statekey,
                                                                                                  Integer countykey,
                                                                                                  Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};
   bool start = true;
   long produced = 0;
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
long GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_merged(Integer nationkey,
                                                                                                    Integer statekey,
                                                                                                    Integer countykey,
                                                                                                    Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};

   MergedJoiner<AdapterType, MergedAdapterType, MergedScannerType> merged_joiner(merged, sk);
   auto kv = merged_joiner.next();
   if (!kv.has_value()) {
      return 0;  // no record is scanned (too large a key)
   }
   update_sk(sk, kv->first.jk);

   while (!merged_joiner.went_past(sk)) {
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
long GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_base(Integer nationkey,
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

   while (!base_joiner.went_past(sk)) {
      auto ret = base_joiner.next();
      if (ret == std::nullopt)
         break;
   }
   long produced = base_joiner.produced();
   // std::cout << "range_query_by_base produced " << produced << " records for sk: " << sk << std::endl;
   return produced;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
long GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_hash(Integer nationkey,
                                                                                               Integer statekey,
                                                                                               Integer countykey,
                                                                                               Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};

   HashJoiner<AdapterType, ScannerType> hash_joiner(nation, states, county, city, customer2, sk);
   auto kv = hash_joiner.next();
   if (!kv.has_value()) {
      return 0;  // no record is scanned (too large a key)
   }
   update_sk(sk, kv->first.jk);

   hash_joiner.run();
   long produced = hash_joiner.produced();
   // std::cout << "range_query_by_base produced " << produced << " records for sk: " << sk << std::endl;
   return produced;
}

}  // namespace geo_join