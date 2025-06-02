#pragma once
#include <cassert>
#include <chrono>

#include "../merge.hpp"
#include "load.hpp"
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
   std::optional<PremergedJoin<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>> joiner;
   sort_key_t seek_key;

   MergedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, sort_key_t seek_key = sort_key_t::max())
       : merged_scanner(merged.template getScanner<sort_key_t, view_t>()), seek_key(seek_key)
   {
      joiner.emplace(*merged_scanner);
   }

   void run() { joiner->run(); }

   std::optional<std::pair<view_t::Key, view_t>> next() { return joiner->next(seek_key); }

   sort_key_t current_jk() const { return joiner->current_jk(); }

   long produced() const { return joiner->produced(); }

   long consumed() const { return merged_scanner->produced; }
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

// -------------------------------------------------------------
// ---------------------- POINT QUERIES --------------------------
// Find all joined rows for the same join key

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_query_by_view()
{
   auto nationkey = workload.getNationID();
   auto statekey = params::get_statekey();
   auto countykey = params::get_countykey();
   auto citykey = params::get_citykey();

   point_query_by_view(nationkey, statekey, countykey, citykey);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_query_by_view(Integer nationkey,
                                                                                                  Integer statekey,
                                                                                                  Integer countykey,
                                                                                                  Integer citykey)
{
   sort_key_t sk = sort_key_t{0, 0, 0, 0, 0};
   [[maybe_unused]] int produced = 0;
   join_view.scan(
       view_t::Key{nationkey, statekey, countykey, citykey, 0},
       [&](const view_t::Key& vk, const view_t&) {
          produced++;
          if (sk == sort_key_t{0, 0, 0, 0, 0}) {
             sk = vk.jk;
          } else if (sk.nationkey != vk.jk.nationkey || sk.statekey != vk.jk.statekey || sk.countykey != vk.jk.countykey ||
                     sk.citykey != vk.jk.citykey) {
             return false;
          }
          return true;
       },
       []() {});
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_query_by_merged()
{
   sort_key_t sk = sort_key_t{workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey(), 0};

   auto scanner = merged.template getScanner<sort_key_t, view_t>();

   city_t::Key* cik = nullptr;
   city_t* civ = nullptr;

   bool ret = scanner->template seekTyped<city_t>(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey});
   if (!ret)
      return;
   auto kv = scanner->current();
   cik = std::get_if<city_t::Key>(&kv->first);
   civ = std::get_if<city_t>(&kv->second);
   assert(cik != nullptr && civ != nullptr);

   county_t* cv = nullptr;
   scanner->template seek<county_t>(county_t::Key{cik->nationkey, cik->statekey, cik->countykey});
   kv = scanner->current();
   cv = std::get_if<county_t>(&kv->second);

   states_t* sv = nullptr;
   scanner->template seek<states_t>(states_t::Key{cik->nationkey, cik->statekey});
   kv = scanner->current();
   sv = std::get_if<states_t>(&kv->second);

   nation2_t* nv = nullptr;
   scanner->template seek<nation2_t>(nation2_t::Key{cik->nationkey});
   kv = scanner->current();
   nv = std::get_if<nation2_t>(&kv->second);

   assert(nv != nullptr && sv != nullptr && cv != nullptr && cik != nullptr);

   // Find all customers of this city
   scanner->template seek<city_t>(city_t::Key{cik->nationkey, cik->statekey, cik->countykey, cik->citykey});
   while (true) {
      auto cu_kv = scanner->next();
      if (cu_kv == std::nullopt)
         break;  // no more records
      customer2_t::Key* cu_k = std::get_if<customer2_t::Key>(&cu_kv->first);
      if (cu_k == nullptr)
         continue;  // not a customer2_t record
      customer2_t* cu_v = std::get_if<customer2_t>(&cu_kv->second);
      assert(cu_v != nullptr);
      view_t::Key vk = view_t::Key{cik->nationkey, cik->statekey, cik->countykey, cik->citykey, cu_k->custkey};
      view_t vv = view_t{*nv, *sv, *cv, *civ, *cu_v};
      // do something with vk and vv
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_query_by_base()
{
   std::optional<city_t::Key> cik = std::nullopt;
   std::optional<city_t> civ = std::nullopt;
   city.scan(
       city_t::Key{workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey()},
       [&](const city_t::Key& k, const city_t& v) {
          cik = k;
          civ = v;
          return false;
       },
       []() {});
   if (!cik.has_value() || !civ.has_value())  // no record is scanned (too large a key)
      return;

   std::optional<nation2_t> nv = std::nullopt;
   std::optional<states_t> sv = std::nullopt;
   std::optional<county_t> cv = std::nullopt;
   nation.lookup1(nation2_t::Key{cik->nationkey}, [&](const nation2_t& n) { nv = n; });
   states.lookup1(states_t::Key{cik->nationkey, cik->statekey}, [&](const states_t& s) { sv = s; });
   county.lookup1(county_t::Key{cik->nationkey, cik->statekey, cik->countykey}, [&](const county_t& c) { cv = c; });

   customer2.scan(
       customer2_t::Key{cik->nationkey, cik->statekey, cik->countykey, cik->citykey, 0},
       [&](const customer2_t::Key& k, const customer2_t& v) {
          if (k.nationkey != cik->nationkey || k.statekey != cik->statekey || k.countykey != cik->countykey || k.citykey != cik->citykey) {
             return false;  // stop scanning if the key does not match
          }
          // do something with vk and vv
          view_t::Key vk = view_t::Key{cik->nationkey, cik->statekey, cik->countykey, cik->citykey, k.custkey};
          view_t vv = view_t{*nv, *sv, *cv, *civ, v};
          return true;  // continue scanning
       },
       []() {});
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
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_view(Integer nationkey,
                                                                                                  Integer statekey,
                                                                                                  Integer countykey,
                                                                                                  Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};
   bool start = true;
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
          return true;
       },
       []() {});
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_merged(Integer nationkey,
                                                                                                    Integer statekey,
                                                                                                    Integer countykey,
                                                                                                    Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};

   MergedJoiner<MergedAdapterType, MergedScannerType> merged_joiner(merged, sk);
   auto kv = merged_joiner.next();
   if (!kv.has_value()) {
      return;  // no record is scanned (too large a key)
   }
   update_sk(sk, kv->first.jk);

   while ((merged_joiner.current_jk().match(sk) == 0) || merged_joiner.current_jk() == sort_key_t::max()) {
      auto ret = merged_joiner.next();
      if (ret == std::nullopt)
         break;
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_base(Integer nationkey,
                                                                                                  Integer statekey,
                                                                                                  Integer countykey,
                                                                                                  Integer citykey)
{
   sort_key_t sk = sort_key_t{nationkey, statekey, countykey, citykey, 0};

   BaseJoiner<AdapterType, ScannerType> base_joiner(nation, states, county, city, customer2, sk);
   auto kv = base_joiner.next();
   if (!kv.has_value()) {
      return;  // no record is scanned (too large a key)
   }
   update_sk(sk, kv->first.jk);

   while ((base_joiner.current_jk().match(sk) == 0) || base_joiner.current_jk() == sort_key_t::max()) {
      auto ret = base_joiner.next();
      if (ret == std::nullopt)
         break;
   }
}
}  // namespace geo_join