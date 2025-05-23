#pragma once

#include <optional>
#include "../merge.hpp"
#include "views.hpp"
#include "workload.hpp"

// county name + city count per county

namespace geo_join
{
template <template <typename> class ScannerType, template <typename> class AdapterType>
struct ViewMixedJoiner {
   std::unique_ptr<ScannerType<county_t>> county_scanner;
   std::unique_ptr<ScannerType<city_count_per_county_t>> city_count_scanner;
   std::optional<BinaryMergeJoin<mixed_view_t::Key, mixed_view_t, county_t, city_count_per_county_t>> joiner;

   ViewMixedJoiner(AdapterType<county_t>& county,
                   AdapterType<city_count_per_county_t>& city_count_per_county,
                   const mixed_view_t::Key& seek_key = mixed_view_t::Key::max())
       : county_scanner(county.getScanner()), city_count_scanner(city_count_per_county.getScanner())
   {
      if (seek_key != mixed_view_t::Key::max()) {
         seek(seek_key);
      }
      joiner.emplace([&]() { return county_scanner->next(); }, [&]() { return city_count_scanner->next(); });
   }

   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next() { return joiner->next(); }

   void run() { joiner->run(); }

   void seek(const mixed_view_t::Key& sk)
   {
      county_scanner->seek(county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
      city_count_scanner->seek(city_count_per_county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
   }
};

template <template <typename> class ScannerType, template <typename> class AdapterType>
struct BaseMixedJoiner {
   std::unique_ptr<ScannerType<county_t>> county_scanner;
   std::unique_ptr<ScannerType<city_t>> city_scanner;
   std::optional<BinaryMergeJoin<mixed_view_t::Key, mixed_view_t, county_t, city_count_per_county_t>> joiner;

   BaseMixedJoiner(AdapterType<county_t>& county, AdapterType<city_t>& city, const mixed_view_t::Key& seek_key = mixed_view_t::Key::max())
       : county_scanner(county.getScanner()), city_scanner(city.getScanner())
   {
      if (seek_key != mixed_view_t::Key::max()) {
         seek(seek_key);
      }
      joiner.emplace([&]() { return county_scanner->next(); },
                     [&]() {
                        auto cnt = 0;
                        city_count_per_county_t::Key curr_key{0, 0, 0};
                        while (true) {
                           auto kv = city_scanner->next();
                           if (kv == std::nullopt)
                              break;
                           city_t::Key& k = kv->first;
                           if (cnt == 0)
                              curr_key = city_count_per_county_t::Key{k};
                           else if (curr_key != city_count_per_county_t::Key{k}) {
                              city_scanner->prev();  // go back so that this key is not skipped
                              return std::make_optional(std::make_pair(curr_key, city_count_per_county_t{cnt}));
                           }
                           cnt++;
                        }
                     });
   }

   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next() { return joiner->next(); }

   void run() { joiner->run(); }

   void seek(const mixed_view_t::Key& sk)
   {
      county_scanner->seek(county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
      city_scanner->seek(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, 0});
   }
};

template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedMixedJoiner {
   std::unique_ptr<MergedScannerType<nation2_t, states_t, county_t, city_t>> scanner;

   MergedMixedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t>& merged, const mixed_view_t::Key& seek_key = mixed_view_t::Key::max())
       : scanner(merged.getScanner())
   {
      if (seek_key != mixed_view_t::Key::max()) {
         seek(seek_key);
      }
   }

   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next()
   {
      int curr_city_cnt = 0;
      mixed_view_t::Key curr_key{0, 0, 0};  // updated once encountered a new county
      Varchar<25> curr_county_name;
      while (true) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            break;
         std::optional<mixed_view_t::Key> next_key = std::nullopt;
         std::optional<mixed_view_t::Key> vk = std::nullopt;
         std::optional<mixed_view_t> vv = std::nullopt;
         std::visit(overloaded{[&](const nation2_t::Key&) {}, [&](const states_t::Key&) {},
                               [&](const county_t::Key& ak) {
                                  if (curr_key != mixed_view_t::Key{ak}) {
                                     next_key = mixed_view_t::Key{ak};
                                  }
                               },
                               [&](const city_t::Key&) {}},
                    kv->first);
         std::visit(overloaded{[&](const nation2_t&) {}, [&](const states_t&) {},
                               [&](const county_t& av) {
                                  vk = curr_key;
                                  vv = {curr_county_name, curr_city_cnt};
                                  curr_county_name = av.name;
                                  curr_city_cnt = 0;
                                  curr_key = next_key.value();
                               },
                               [&](const city_t&) { curr_city_cnt++; }},
                    kv->second);
         if (vk != std::nullopt && vv != std::nullopt && vk.value() != mixed_view_t::Key{0, 0, 0}) {
            auto ret = std::make_pair(vk.value(), vv.value());
            return ret;
         }
      }
   }

   void run()
   {
      while (true) {
         auto kv = next();
         if (kv == std::nullopt)
            break;
      }
   }

   void seek(const mixed_view_t::Key& sk) { scanner->template seek<county_t>(county_t::Key{sk.nationkey, sk.statekey, sk.countykey}); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_view()
{
   logger.reset();
   std::cout << "GeoJoin::mixed_query_by_view()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   ViewMixedJoiner<ScannerType, AdapterType> joiner(county, city_count_per_county);
   joiner.run();
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "query", "view", get_view_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_view()
{
   ViewMixedJoiner<ScannerType, AdapterType> joiner(county, city_count_per_county,
                                                    mixed_view_t::Key{workload.getNationID(), params::get_statekey(), params::get_countykey()});
   [[maybe_unused]] auto kv = joiner.next();
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_merged()
{
   logger.reset();
   std::cout << "GeoJoin::mixed_query_by_merged()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   MergedMixedJoiner<MergedAdapterType, MergedScannerType> joiner(merged);
   joiner.run();
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "query", "merged", get_merged_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_merged()
{
   MergedMixedJoiner<MergedAdapterType, MergedScannerType> joiner(
       merged, mixed_view_t::Key{workload.getNationID(), params::get_statekey(), params::get_countykey()});
   [[maybe_unused]] auto kv = joiner.next();
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_base()
{
   logger.reset();
   std::cout << "GeoJoin::mixed_query_by_base()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();

   BaseMixedJoiner<ScannerType, AdapterType> joiner(county, city);
   joiner.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "query", "base", get_indexes_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_base()
{
   BaseMixedJoiner<ScannerType, AdapterType> joiner(county, city,
                                                    mixed_view_t::Key{workload.getNationID(), params::get_statekey(), params::get_countykey()});
   [[maybe_unused]] auto kv = joiner.next();
}

}  // namespace geo_join