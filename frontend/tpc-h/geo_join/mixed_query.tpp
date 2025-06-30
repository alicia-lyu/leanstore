#pragma once

#include <iostream>
#include <optional>
#include "Exceptions.hpp"
#include "views.hpp"
#include "workload.hpp"

// SELECT nationkey, statekey, countykey, citykey, city_name, COUNT(*) as customer_count
// FROM city, customer
// WHERE ... -- all keys equal
// GROUP BY nationkey, statekey, countykey, citykey, city_name
// TODO: outer join?

// include empty cities TODO outer join

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
   sort_key_t curr_sk = sort_key_t{0, 0, 0, 0, 0};
   Integer customer_count = 0;
   long long produced = 0;
   join_view.scan(
       view_t::Key{curr_sk},
       [&](const view_t::Key& vk, const view_t& vv) {
          if (curr_sk != vk.jk) {
             produced++;
             if (produced % 10000 == 0 && FLAGS_log_progress) {
                std::cout << "\rmixed_query_by_view() produced: " << produced;
             }
             city_t c = std::get<3>(vv.payloads);
             [[maybe_unused]] mixed_view_t mv{c.name, customer_count};
             curr_sk = vk.jk;
          }
          customer_count++;
          return true;
       },
       []() {});
   std::cout << "mixed_query_by_view() produced: " << produced << std::endl;
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "mixed", "view", get_view_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_view()
{
   sort_key_t sk{workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey(), 0};
   int customer_count = 0;
   join_view.scan(
       view_t::Key{sk},
       [&](const view_t::Key& vk, const view_t& vv) {
          if (sk != vk.jk) {
             city_t c = std::get<3>(vv.payloads);
             [[maybe_unused]] mixed_view_t mv{c.name, customer_count};
             return false;  // stop after first agg
          } else {
             customer_count++;
          }
          return true;
       },
       []() {});
}

template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedCounter {
   std::unique_ptr<MergedScannerType<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>> scanner;
   long long produced = 0;
   MergedCounter(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, sort_key_t sk = sort_key_t::max())
       : scanner(merged.template getScanner<sort_key_t, view_t>())
   {
      if (sk != sort_key_t::max()) {
         scanner->seekJK(sk);
      }
   }

   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next()
   {
      Varchar<25> city_name;
      int customer_count = 0;
      sort_key_t sk = sort_key_t::max();
      while (true) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            return std::nullopt;

         sort_key_t curr_sk = SKBuilder<sort_key_t>::create(kv->first, kv->second);
         if (sk != sort_key_t::max() && curr_sk.match(sk) != 0) {
            if (std::holds_alternative<city_t>(kv->second)) {
               scanner->after_seek = true;  // rescan this city
            }
            if (customer_count > 0) {
               mixed_view_t mv{city_name, customer_count};
               produced++;
               return std::make_pair(mixed_view_t::Key{sk}, mv);
            } else {
               return next();  // TODO outer join?
            }
         }

         std::visit(overloaded{[&](const nation2_t&) {}, [&](const states_t&) {}, [&](const county_t&) {},
                               [&](const city_t& cv) {
                                  sk = curr_sk;
                                  city_name = cv.name;
                               },
                               [&](const customer2_t&) { customer_count++; }},
                    kv->second);
      }
   }

   long long last_logged = 0;
   void run()
   {
      while (true) {
         auto kv = next();
         if (kv == std::nullopt)
            break;
         if (produced - last_logged > 10000 && FLAGS_log_progress) {
            std::cout << "\rMergedCounter::run() produced: " << produced;
            last_logged = produced;
         }
      }
      std::cout << "MergedCounter::run() produced: " << produced << std::endl;
   }
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
   MergedCounter<MergedAdapterType, MergedScannerType> counter(merged);
   counter.run();
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "mixed", "merged", get_merged_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_merged()
{
   MergedCounter<MergedAdapterType, MergedScannerType> counter(
       merged, sort_key_t{workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey(), 0});
   [[maybe_unused]] auto kv = counter.next();
}

template <template <typename> class AdapterType, template <typename> class ScannerType>
struct BaseCounter {
   std::unique_ptr<ScannerType<city_t>> city_scanner;
   std::unique_ptr<ScannerType<customer2_t>> customer_scanner;
   long long produced = 0;
   BaseCounter(AdapterType<city_t>& city, AdapterType<customer2_t>& customer, sort_key_t sk = sort_key_t::max())
       : city_scanner(city.getScanner()), customer_scanner(customer.getScanner())
   {
      if (sk != sort_key_t::max()) {
         city_scanner->seek(city_t::Key{sk});
         customer_scanner->seek(customer2_t::Key{sk});
      }
   }
   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next()
   {
      auto city_kv = city_scanner->next();
      if (city_kv == std::nullopt)
         return std::nullopt;
      auto& [cik, civ] = *city_kv;
      sort_key_t ci_sk = SKBuilder<sort_key_t>::create(cik, civ);
      int customer_count = 0;
      while (true) {
         auto customer_kv = customer_scanner->next();
         if (customer_kv == std::nullopt)
            break;
         auto& [cuk, cuv] = *customer_kv;
         sort_key_t cu_sk = SKBuilder<sort_key_t>::create(cuk, cuv);
         if (ci_sk.match(cu_sk) < 0) {            // this customer belongs to the next city
            customer_scanner->after_seek = true;  // rescan this customer
            break;
         } else if (ci_sk.match(cu_sk) == 0) {  // this customer belongs to the city
            customer_count++;
         } else {
            UNREACHABLE();
         }
      }
      if (customer_count > 0) {
         produced++;
         return std::make_pair(mixed_view_t::Key{ci_sk}, mixed_view_t{civ.name, customer_count});
      } else {
         return next();  // TODO outer join?
      }
   }

   long long last_logged = 0;
   void run()
   {
      while (true) {
         auto kv = next();
         if (kv == std::nullopt)
            break;
         if (produced - last_logged > 10000 && FLAGS_log_progress) {
            std::cout << "\rBaseCounter::run() produced: " << produced;
            last_logged = produced;
         }
      }
      std::cout << "BaseCounter::run() produced: " << produced << std::endl;
   }
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

   BaseCounter<AdapterType, ScannerType> counter(city, customer2);
   counter.run();

   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "mixed", "base", get_indexes_size());
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_base()
{
   BaseCounter<AdapterType, ScannerType> counter(
       city, customer2, sort_key_t{workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey(), 0});
   [[maybe_unused]] auto kv = counter.next();
}

template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct Merged2Counter {
   std::unique_ptr<MergedScannerType<sort_key_t, ccc_t, county_t, city_t, customer2_t>> ccc_scanner;
   long long produced = 0;

   Merged2Counter(MergedAdapterType<county_t, city_t, customer2_t>& ccc, sort_key_t sk = sort_key_t::max())
       : ccc_scanner(ccc.template getScanner<sort_key_t, ccc_t>())
   {
      if (sk != sort_key_t::max()) {
         ccc_scanner->seekJK(sk);
      }
   }

   std::optional<std::pair<mixed_view_t::Key, mixed_view_t>> next()
   {
      int customer_count = 0;
      Varchar<25> city_name;
      sort_key_t sk = sort_key_t::max();
      while (true) {
         auto kv = ccc_scanner->next();
         if (kv == std::nullopt)
            return std::nullopt;
         auto curr_sk = SKBuilder<sort_key_t>::create(kv->first, kv->second);

         if (sk != sort_key_t::max() && sk.match(curr_sk) != 0) {
            if (std::holds_alternative<city_t>(kv->second)) {
               ccc_scanner->after_seek = true;  // rescan this city
            }
            if (customer_count > 0) {
               produced++;
               return std::make_pair(mixed_view_t::Key{sk}, mixed_view_t{city_name, customer_count});
            } else {
               return next();  // TODO outer join?
            }
         }

         std::visit(overloaded{[&](const county_t&) {},
                               [&](const city_t& cv) {
                                  sk = curr_sk;
                                  city_name = cv.name;
                               },
                               [&](const customer2_t&) { customer_count++; }},
                    kv->second);
      }
   }

   long long last_logged = 0;
   void run()
   {
      while (true) {
         auto kv = next();
         if (kv == std::nullopt)
            break;
         if (produced - last_logged > 10000 && FLAGS_log_progress) {
            std::cout << "\rMerged2Counter::run() produced: " << produced;
            last_logged = produced;
         }
      }
      std::cout << "Merged2Counter::run() produced: " << produced << std::endl;
   }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::mixed_query_by_2merged()
{
   logger.reset();
   std::cout << "GeoJoin::mixed_query_by_2merged()" << std::endl;
   auto start = std::chrono::high_resolution_clock::now();
   Merged2Counter<MergedAdapterType, MergedScannerType> counter(ccc);
   counter.run();
   auto end = std::chrono::high_resolution_clock::now();
   auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
   logger.log(t, "mixed", "2merged", get_2merged_size());
}
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::point_mixed_query_by_2merged()
{
   Merged2Counter<MergedAdapterType, MergedScannerType> counter(
       ccc, mixed_view_t::Key{workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey(), 0});
   [[maybe_unused]] auto kv = counter.next();
}

}  // namespace geo_join