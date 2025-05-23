#pragma once
#include <chrono>

#include "load.hpp"
#include "workload.hpp"
#include "../merge.hpp"

namespace geo_join
{
template <template <typename> class AdapterType, template <typename> class ScannerType>
struct BaseJoiner {
   std::unique_ptr<ScannerType<nation2_t>> nation_scanner;
   std::unique_ptr<ScannerType<states_t>> states_scanner;
   std::unique_ptr<ScannerType<county_t>> county_scanner;
   std::unique_ptr<ScannerType<city_t>> city_scanner;

   std::optional<BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t>> joiner_ns;
   std::optional<BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t>> joiner_nsc;
   std::optional<BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t>> final_joiner;

   BaseJoiner(AdapterType<nation2_t>& nation,
              AdapterType<states_t>& states,
              AdapterType<county_t>& county,
              AdapterType<city_t>& city,
              sort_key_t seek_key = sort_key_t::max())
       : nation_scanner(nation.getScanner()),
         states_scanner(states.getScanner()),
         county_scanner(county.getScanner()),
         city_scanner(city.getScanner())
   {
      if (seek_key != sort_key_t::max()) {
         seek(seek_key);
      }
      joiner_ns.emplace([this]() { return nation_scanner->next(); }, [&]() { return states_scanner->next(); });
      joiner_nsc.emplace([this]() { return joiner_ns->next(); }, [&]() { return county_scanner->next(); });
      final_joiner.emplace([this]() { return joiner_nsc->next(); }, [&]() { return city_scanner->next(); });
   }

   void run() { final_joiner->run(); }

   std::optional<std::pair<view_t::Key, view_t>> next() { return final_joiner->next(); }

   sort_key_t current_jk() const { return final_joiner->current_jk; }

   long produced() const { return final_joiner->produced; }

   void seek(const sort_key_t& sk)
   {
      nation_scanner->seek(nation2_t::Key{sk.nationkey});
      states_scanner->seek(states_t::Key{sk.nationkey, sk.statekey});
      county_scanner->seek(county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
      city_scanner->seek(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey});
   }
};
template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedJoiner {
   std::unique_ptr<MergedScannerType<nation2_t, states_t, county_t, city_t>> merged_scanner;
   std::optional<PremergedJoin<sort_key_t, view_t, nation2_t, states_t, county_t, city_t>> joiner;

   MergedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t>& merged, sort_key_t seek_key = sort_key_t::max())
       : merged_scanner(merged.getScanner())
   {
      if (seek_key != sort_key_t::max()) {
         seek(seek_key);
      }
      joiner.emplace(*merged_scanner);
   }

   void run() { joiner->run(); }

   int next() { return joiner->next(); }

   sort_key_t current_jk() const { return joiner->current_jk; }

   long produced() const { return joiner->produced; }

   long consumed() const { return merged_scanner->produced; }

   void seek(const sort_key_t& sk) { merged_scanner->template seek<nation2_t>(nation2_t::Key{sk.nationkey}); }
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
   sort_key_t start_sk = sort_key_t{0, 0, 0, 0};
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
   logger.log(t, "query", "view", get_view_size());
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
   logger.log(t, "query", "merged", get_merged_size());
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

   BaseJoiner<AdapterType, ScannerType> base_joiner(nation, states, county, city);

   base_joiner.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "query", "base", get_indexes_size());
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
   sort_key_t sk = sort_key_t{0, 0, 0, 0};
   [[maybe_unused]] int produced = 0;
   join_view.scan(
       view_t::Key{nationkey, statekey, countykey, citykey},
       [&](const view_t::Key& vk, const view_t&) {
          produced++;
          if (sk == sort_key_t{0, 0, 0, 0}) {
             sk = vk.jk;
          } else if (sk != vk.jk) {
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
   sort_key_t sk = sort_key_t{workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey()};

   auto scanner = merged.getScanner();

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
   view_t::Key vk = view_t::Key{cik->nationkey, cik->statekey, cik->countykey, cik->citykey};
   view_t vv = view_t{*nv, *sv, *cv, *civ};
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

   // while (!nv.has_value() || !sv.has_value() || !cv.has_value()) {
   // }
   [[maybe_unused]] view_t::Key vk = view_t::Key{cik->nationkey, cik->statekey, cik->countykey, cik->citykey};
   [[maybe_unused]] view_t vv = view_t{*nv, *sv, *cv, *civ};
}

// -------------------------------------------------------------
// ---------------------- RANGE QUERIES ------------------------
// Find all joined rows for the same nationkey

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_view()
{
   logger.reset();
   std::cout << "GeoJoin::range_query_by_view()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   // auto nationkey = workload.getNationID();
   [[maybe_unused]] long produced = 0;
   join_view.scan(
       view_t::Key{range_join_n, 0, 0, 0},
       [&](const view_t::Key& k, const view_t&) {
          if (k.jk.nationkey != range_join_n)
             return false;
          TPCH::inspect_produced("Range querying materialized join_view: ", produced);
          return true;
       },
       []() {});

   std::cout << "\rRange querying materialized join_view for nation " << range_join_n << " : " << (double)produced / 1000
             << "k------------------------------------" << std::endl;
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "range-query", "view", get_view_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_merged()
{
   logger.reset();
   std::cout << "GeoJoin::range_query_by_merged()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   sort_key_t seek_key{range_join_n, 0, 0, 0};

   MergedJoiner<MergedAdapterType, MergedScannerType> merged_joiner(merged, seek_key);
   while (merged_joiner.current_jk().nationkey == range_join_n || merged_joiner.current_jk() == sort_key_t::max()) {
      auto ret = merged_joiner.next();
      if (ret == -1)
         break;
   }

   std::cout << "\rRange querying merged for nation " << range_join_n << " : produced " << merged_joiner.produced()
             << " records------------------------------------" << std::endl;

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "range-query", "merged", get_merged_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::range_query_by_base()
{
   logger.reset();
   std::cout << "GeoJoin::range_query_by_base()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   sort_key_t seek_key{range_join_n, 0, 0, 0};

   BaseJoiner<AdapterType, ScannerType> base_joiner(nation, states, county, city, seek_key);

   while (base_joiner.current_jk().nationkey == range_join_n || base_joiner.current_jk() == sort_key_t::max()) {
      auto ret = base_joiner.next();
      if (ret == std::nullopt)
         break;
   }

   std::cout << "\rRange querying base for nation " << range_join_n << " produced " << base_joiner.produced()
             << " records------------------------------------" << std::endl;
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "range-query", "base", get_indexes_size());
}
}  // namespace geo_join