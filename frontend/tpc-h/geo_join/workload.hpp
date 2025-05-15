#pragma once

#include <chrono>
#include <optional>
#include <variant>

#include "../../shared/Adapter.hpp"
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
   using MergedTree = MergedAdapterType<nation2_t, states_t, county_t, city_t>;
   MergedTree& merged;
   AdapterType<nation2_t>& nation;
   AdapterType<states_t>& states;
   AdapterType<county_t>& county;
   AdapterType<city_t>& city;
   AdapterType<view_t>& view;

   Logger& logger;

  public:
   GeoJoin(TPCH& workload, MergedTree& m, AdapterType<view_t>& v, AdapterType<states_t>& s, AdapterType<county_t>& c, AdapterType<city_t>& ci)
       : workload(workload), merged(m), view(v), nation(workload.nation), states(s), county(c), city(ci), logger(workload.logger)
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
      sort_key_t start_sk = sort_key_t{0, 0, 0, 0};
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
      auto nation_scanner = nation.getScanner();
      auto states_scanner = states.getScanner();
      auto county_scanner = county.getScanner();
      auto city_scanner = city.getScanner();

      BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t> binary_join_ns([&]() { return nation_scanner->next(); },
                                                                            [&]() { return states_scanner->next(); });

      BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t> binary_join_nsc([&]() { return binary_join_ns.next(); },
                                                                         [&]() { return county_scanner->next(); });

      BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t> binary_join4([&]() { return binary_join_nsc.next(); },
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
      auto scanner = merged.getScanner();
      bool ret = scanner->template seekTyped<city_t>(
          city_t::Key{workload.getRegionID(), workload.getNationID(), workload.getStateID(), workload.getCountyID(), workload.getCityID()});
      if (!ret)
         return;

      sort_key_t target_sk;
      auto target_kv = scanner->current();
      assert(target_kv != std::nullopt);
      auto& [k, v] = *target_kv;
      std::visit(overloaded{[&](const nation2_t::Key&) { UNREACHABLE(); }, [&](const states_t::Key&) { UNREACHABLE(); },
                            [&](const county_t::Key&) { UNREACHABLE(); }, [&](const city_t::Key& ak) { target_sk = ak.get_jk(); }},
                 k);

      std::optional<nation2_t> nv = std::nullopt;
      std::optional<states_t> sv = std::nullopt;
      std::optional<county_t> cv = std::nullopt;
      std::optional<city_t> civ = std::nullopt;

      std::visit(overloaded{[&](const nation2_t&) { UNREACHABLE(); }, [&](const states_t&) { UNREACHABLE(); },
                            [&](const county_t&) { UNREACHABLE(); }, [&](const city_t& ci) { civ = ci; }},
                 v);

      while (!nv.has_value() || !sv.has_value() || !cv.has_value() || !civ.has_value()) {
         auto kv = scanner->prev();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;

         std::visit(overloaded{[&](const nation2_t::Key& nk) { assert(nk.get_jk().match(target_sk)); },
                               [&](const states_t::Key& sk) { assert(sk.get_jk().match(target_sk)); },
                               [&](const county_t::Key& ck) { assert(ck.get_jk().match(target_sk)); }, [&](const city_t::Key&) { UNREACHABLE(); }},
                    k);

         std::visit(overloaded{[&](const nation2_t& av) { nv = av; }, [&](const states_t& av) { sv = av; }, [&](const county_t& av) { cv = av; },
                               [&](const city_t&) { UNREACHABLE(); }},
                    v);
      }

      view_t vv{nv.value(), sv.value(), cv.value(), civ.value()};
   }

   void point_query_by_base()
   {
      auto regionkey = workload.getRegionID();
      auto nationkey = workload.getNationID();
      auto statekey = workload.getStateID();
      auto countykey = workload.getCountyID();
      auto citykey = workload.getCityID();

      city_t civ;

      city.scan(
          city_t::Key{regionkey, nationkey, statekey, countykey, citykey},
          [&](const city_t::Key& k, const city_t& v) {
             nationkey = k.nationkey;
             statekey = k.statekey;
             countykey = k.countykey;
             citykey = k.citykey;
             civ = v;
             return false;
          },
          []() {});

      county_t cv;
      county.lookup1(county_t::Key{statekey, countykey}, [&](const county_t& v) { cv = v; });

      states_t sv;
      states.lookup1(states_t::Key{regionkey, statekey}, [&](const states_t& v) { sv = v; });

      nation2_t nv;
      nation.lookup1(nation2_t::Key{regionkey, nationkey}, [&](const nation2_t& v) { nv = v; });

      view_t vv{nv, sv, cv, civ};
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------
   // insert a new state, a new county, a new city

   void maintain_view() {}

   // -------------------------------------------------------------
   // ---------------------- LOADING -----------------------------

   static constexpr size_t STATE_MAX = 100;   // max 50 states in a nation
   static constexpr size_t COUNTY_MAX = 100;  // max 1000 counties in a state
   static constexpr size_t CITY_MAX = 100;    // max 10000 cities in a county

   void load_scci()
   {
      // county id starts from 1 in each s tate
      // city id starts from 1 in each county
      for (int n = 1; n <= workload.NATION_COUNT; n++) {
         // state id starts from 1 in each nation
         int state_cnt = urand(1, STATE_MAX);
         UpdateDescriptorGenerator1(nation_update_desc, nation2_t, last_statekey);
         nation.update1(
             nation2_t::Key{n},
             [&](nation2_t& n) {
                n.last_statekey = state_cnt;
                return true;
             },
             nation_update_desc);
         for (int s = 1; s <= state_cnt; s++) {
            int county_cnt = urand(1, COUNTY_MAX);
            states.insert(states_t::Key{n, s}, states_t{randomastring<25>(1, 25), randomastring<152>(0, 152), county_cnt});
            for (int c = 1; c <= county_cnt; c++) {
               int city_cnt = urand(1, CITY_MAX);
               county.insert(county_t::Key{n, s, c}, county_t{randomastring<25>(1, 25), randomastring<152>(0, 152), city_cnt});
               for (int ci = 1; ci <= city_cnt; ci++) {
                  city.insert(city_t::Key{n, s, c, ci}, city_t{randomastring<25>(1, 25), randomastring<152>(0, 152)});
               }
            }
         }
      }
   }
};
}  // namespace geo_join