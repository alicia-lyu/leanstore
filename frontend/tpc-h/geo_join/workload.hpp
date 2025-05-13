#pragma once

#include <chrono>
#include <optional>
#include <variant>

#include "../merge.hpp"
#include "../tpch_workload.hpp"
#include "Exceptions.hpp"
#include "views.hpp"

namespace geo_join
{

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
class GeoJoin
{
   using TPCH = TPCHWorkload<AdapterType>;
   TPCH& workload;
   using MergedTree = MergedAdapterType<region2_t, nation2_t, states_t, county_t, city_t>;
   MergedTree& merged;
   AdapterType<region2_t>& region;
   AdapterType<nation2_t>& nation;
   AdapterType<states_t>& states;
   AdapterType<county_t>& county;
   AdapterType<city_t>& city;
   AdapterType<view_t>& view;

   Logger& logger;

  public:
   GeoJoin(TPCH& workload, MergedTree& m, AdapterType<view_t>& v, AdapterType<states_t>& s, AdapterType<county_t>& c, AdapterType<city_t>& ci)
       : workload(workload),
         merged(m),
         view(v),
         region(workload.region),
         nation(workload.nation),
         states(s),
         county(c),
         city(ci),
         logger(workload.logger)
   {
   }

   // -------------------------------------------------------------
   // ---------------------- POINT LOOKUPS ------------------------
   // point lookups: one per view, one per all base tables

   void point_lookups_for_base()
   {
      auto regionkey = workload.getRegionID();
      auto nationkey = workload.getNationID();
      auto statekey = workload.getStateID();
      auto countykey = workload.getCountyID();
      auto citykey = workload.getCityID();

      region.scan(region2_t::Key{regionkey}, [](const region2_t::Key&, const region2_t&) { return false; }, []() {});
      nation.scan(nation2_t::Key{regionkey, nationkey}, [](const nation2_t::Key&, const nation2_t&) { return false; }, []() {});
      states.scan(states_t::Key{regionkey, statekey}, [](const states_t::Key&, const states_t&) { return false; }, []() {});
      county.scan(county_t::Key{statekey, countykey}, [](const county_t::Key&, const county_t&) { return false; }, []() {});
      city.scan(city_t::Key{countykey, citykey}, [](const city_t::Key&, const city_t&) { return false; }, []() {});

      point_lookups_of_rest();

      return std::make_tuple(regionkey, nationkey, statekey, countykey, citykey);
   }

   void point_lookups_for_view()
   {
      auto [regionkey, nationkey, statekey, countykey, citykey] = point_lookups_for_base();
      view.scan(view_t::Key{regionkey, nationkey, statekey, countykey, citykey}, [](const view_t::Key&, const view_t&) { return false; }, []() {});
   }

   void point_lookups_for_merged()
   {
      auto regionkey = workload.getRegionID();
      auto nationkey = workload.getNationID();
      auto statekey = workload.getStateID();
      auto countykey = workload.getCountyID();
      auto citykey = workload.getCityID();
      sort_key_t sk{regionkey, nationkey, statekey, countykey, citykey};

      auto scanner = merged.getScanner();
      scanner->seekJK(sort_key_t{regionkey, 0, 0, 0, 0});

      bool end = false;

      while (!end) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;

         std::visit(
             [&](const auto& ak) {
                if (ak.jk > sk)
                   end = true;
             },
             k);
      }
      point_lookups_of_rest();
   }

   void point_lookups_of_rest()
   {
      workload.part.scan(part_t::Key{workload.getPartID()}, [](const part_t::Key&, const part_t&) { return false; }, []() {});
      workload.supplier.scan(supplier_t::Key{workload.getSupplierID()}, [](const supplier_t::Key&, const supplier_t&) { return false; }, []() {});
      workload.partsupp.scan(
          partsupp_t::Key{workload.getPartID(), workload.getSupplierID()}, [](const partsupp_t::Key&, const partsupp_t&) { return false; }, []() {});
      workload.customer.scan(customerh_t::Key{workload.getCustomerID()}, [](const customerh_t::Key&, const customerh_t&) { return false; }, []() {});
      workload.orders.scan(orders_t::Key{workload.getOrderID()}, [](const orders_t::Key&, const orders_t&) { return false; }, []() {});
      workload.lineitem.scan(lineitem_t::Key{workload.getOrderID(), 1}, [](const lineitem_t::Key&, const lineitem_t&) { return false; }, []() {});
   }

   // -------------------------------------------------------------
   // ------------------------ QUERIES -----------------------------

   void query_by_view()  // scan through the view
   {
      logger.reset();
      std::cout << "GeoJoin::query_by_view()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      sort_key_t start_sk = sort_key_t{0, 0, 0, 0, 0};
      view.scan(
          view_t::Key{start_sk},
          [&](const view_t::Key&, const view_t&) {
             TPCH::inspect_produced("Enumerating materialized view: ", produced);
             return true;
          },
          [&]() {});
      std::cout << "\rEnumerating materialized view: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "query-external-select-view");
   }

   void query_by_merged()
   {
      logger.reset();
      std::cout << "GeoJoin::query_by_merged()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      auto scanner = merged.getScanner();
      scanner->template scanJoin<sort_key_t, view_t>();

      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "query-merged");
   }

   void query_by_base()
   {
      logger.reset();
      std::cout << "GeoJoin::query_by_base()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      auto region_scanner = region.getScanner();
      auto nation_scanner = nation.getScanner();
      auto states_scanner = states.getScanner();
      auto county_scanner = county.getScanner();
      auto city_scanner = city.getScanner();

      BinaryMergeJoin<sort_key_t, rn_t, region2_t, nation2_t> binary_join1([&]() { return region_scanner->next(); },
                                                                                    [&]() { return nation_scanner->next(); });

      BinaryMergeJoin<sort_key_t, rns_t, rn_t, states_t> binary_join2([&]() { return binary_join1.next(); },
                                                                                           [&]() { return states_scanner->next(); });

      BinaryMergeJoin<sort_key_t, rnsc_t, rns_t, county_t> binary_join3([&]() { return binary_join2.next(); },
                                                                                           [&]() { return county_scanner->next(); });

      BinaryMergeJoin<sort_key_t, view_t, rnsc_t, city_t> binary_join4([&]() { return binary_join3.next(); },
                                                                                       [&]() { return city_scanner->next(); });

      binary_join4.run();

      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "query-base");
   }

   // -------------------------------------------------------------
   // ---------------------- POINT QUERIES --------------------------
   // Find all joined rows for the same join key

   void point_query_by_view()
   {
      auto regionkey = workload.getRegionID();
      auto nationkey = workload.getNationID();
      auto statekey = workload.getStateID();
      auto countykey = workload.getCountyID();
      auto citykey = workload.getCityID();

      view.scan(view_t::Key{regionkey, nationkey, statekey, countykey, citykey}, [&](const view_t::Key&, const view_t&) { return false; }, []() {});
   }

   void point_query_by_merged()
   {
      auto countykey = workload.getCountyID();
      auto citykey = workload.getCityID();

      auto scanner = merged.getScanner();
      bool ret =
          scanner->template seekTyped<city_t>(city_t::Key{workload.getRegionID(), workload.getNationID(), workload.getStateID(), countykey, citykey});
      if (!ret)
         return;

      sort_key_t target_sk;
      auto target_kv = scanner->current();
      assert(target_kv != std::nullopt);
      auto& [k, v] = *target_kv;
      std::visit(overloaded{[&](const region2_t::Key&) { UNREACHABLE(); }, [&](const nation2_t::Key&) { UNREACHABLE(); },
                            [&](const states_t::Key&) { UNREACHABLE(); }, [&](const county_t::Key&) { UNREACHABLE(); },
                            [&](const city_t::Key& ak) { target_sk = ak.get_jk(); }},
                 k);

      std::optional<region2_t> rv = std::nullopt;
      std::optional<nation2_t> nv = std::nullopt;
      std::optional<states_t> sv = std::nullopt;
      std::optional<county_t> cv = std::nullopt;
      std::optional<city_t> civ = std::nullopt;

      std::visit(overloaded{[&](const region2_t&) { UNREACHABLE(); }, [&](const nation2_t&) { UNREACHABLE(); },
                            [&](const states_t&) { UNREACHABLE(); }, [&](const county_t&) { UNREACHABLE(); },
                            [&](const city_t& ci) { civ = ci; }},
                 v);

      while (!rv.has_value() || !nv.has_value() || !sv.has_value() || !cv.has_value() || !civ.has_value()) {
         auto kv = scanner->prev();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;

         std::visit(
             overloaded{[&](const region2_t::Key& rk) { assert(rk.get_jk().match(target_sk)); },
                        [&](const nation2_t::Key& nk) { assert(nk.get_jk().match(target_sk)); },
                        [&](const states_t::Key& sk) { assert(sk.get_jk().match(target_sk)); },
                        [&](const county_t::Key& ck) { assert(ck.get_jk().match(target_sk)); }, [&](const city_t::Key&) { UNREACHABLE(); }},
             k);

         std::visit(overloaded{[&](const region2_t& av) { rv = av; }, [&](const nation2_t& av) { nv = av; },
                               [&](const states_t& av) { sv = av; }, [&](const county_t& av) { cv = av; },
                               [&](const city_t&) { UNREACHABLE(); }},
                    v);
      }

      view_t vv{rv.value(), nv.value(), sv.value(), cv.value(), civ.value()};
   }

   void point_query_by_base()
   {
      auto countykey = workload.getCountyID();
      auto citykey = workload.getCityID();
   }
};
}  // namespace geo_join