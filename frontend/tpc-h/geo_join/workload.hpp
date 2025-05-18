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

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
class GeoJoin
{
   using TPCH = TPCHWorkload<AdapterType>;
   TPCH& workload;
   using MergedTree = MergedAdapterType<nation2_t, states_t, county_t, city_t>;
   MergedTree& merged;
   AdapterType<view_t>& view;

   AdapterType<nation2_t>& nation;
   AdapterType<states_t>& states;
   AdapterType<county_t>& county;
   AdapterType<city_t>& city;

   Logger& logger;

  public:
   GeoJoin(TPCH& workload, MergedTree& m, AdapterType<view_t>& v, AdapterType<states_t>& s, AdapterType<county_t>& c, AdapterType<city_t>& ci)
       : workload(workload),
         merged(m),
         view(v),
         nation(reinterpret_cast<AdapterType<nation2_t>&>(workload.nation)),
         states(s),
         county(c),
         city(ci),
         logger(workload.logger)
   {
   }

   // -------------------------------------------------------------
   // ---------------------- POINT LOOKUPS ------------------------
   // point lookups: one per view, one per all base tables

   std::tuple<Integer, Integer, Integer, Integer> point_lookups_for_base()
   {
      auto nationkey = workload.getNationID();
      auto statekey = getStateKey();
      auto countykey = getCountyKey();
      auto citykey = getCityKey();

      nation.scan(nation2_t::Key{nationkey}, [](const nation2_t::Key&, const nation2_t&) { return false; }, []() {});
      states.scan(states_t::Key{nationkey, statekey}, [](const states_t::Key&, const states_t&) { return false; }, []() {});
      county.scan(county_t::Key{nationkey, statekey, countykey}, [](const county_t::Key&, const county_t&) { return false; }, []() {});
      city.scan(city_t::Key{nationkey, statekey, countykey, citykey}, [](const city_t::Key&, const city_t&) { return false; }, []() {});

      point_lookups_of_rest();

      return std::make_tuple(nationkey, statekey, countykey, citykey);
   }

   void point_lookups_for_view()
   {
      auto [nationkey, statekey, countykey, citykey] = point_lookups_for_base();
      view.scan(view_t::Key{nationkey, statekey, countykey, citykey}, [](const view_t::Key&, const view_t&) { return false; }, []() {});
   }

   void point_lookups_for_merged()
   {
      auto nationkey = workload.getNationID();
      auto statekey = getStateKey();
      auto countykey = getCountyKey();
      auto citykey = getCityKey();
      sort_key_t sk{nationkey, statekey, countykey, citykey};

      auto scanner = merged.getScanner();
      scanner->seekJK(sort_key_t{0, 0, 0, 0});

      bool end = false;

      while (!end) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;

         std::visit(overloaded{[&](const nation2_t::Key& ak) {
                                  if (ak.nationkey > sk.nationkey)
                                     end = true;
                               },
                               [&](const states_t::Key& ak) {
                                  if (ak.nationkey > sk.nationkey)
                                     end = true;
                                  else if (ak.statekey > sk.statekey)
                                     end = true;
                               },
                               [&](const county_t::Key& ak) {
                                  if (ak.nationkey > sk.nationkey)
                                     end = true;
                                  else if (ak.statekey > sk.statekey)
                                     end = true;
                                  else if (ak.countykey > sk.countykey)
                                     end = true;
                               },
                               [&](const city_t::Key& ak) {
                                  if (ak.nationkey > sk.nationkey)
                                     end = true;
                                  else if (ak.statekey > sk.statekey)
                                     end = true;
                                  else if (ak.countykey > sk.countykey)
                                     end = true;
                                  else if (ak.citykey > sk.citykey)
                                     end = true;
                               }},
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

      BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t> binary_join([&]() { return binary_join_nsc.next(); },
                                                                     [&]() { return city_scanner->next(); });

      binary_join.run();

      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "query-base");
   }

   // -------------------------------------------------------------
   // ---------------------- POINT QUERIES --------------------------
   // Find all joined rows for the same join key

   void point_query_by_view()
   {
      auto nationkey = workload.getNationID();
      auto statekey = getStateKey();
      auto countykey = getCountyKey();
      auto citykey = getCityKey();

      view.scan(view_t::Key{nationkey, statekey, countykey, citykey}, [&](const view_t::Key&, const view_t&) { return false; }, []() {});
   }

   static void find_base_v(std::variant<nation2_t, states_t, county_t, city_t>& v,
                           std::optional<nation2_t>& nv,
                           std::optional<states_t>& sv,
                           std::optional<county_t>& cv,
                           std::optional<city_t>& civ)
   {
      std::visit(overloaded{[&](const nation2_t& av) { nv = av; }, [&](const states_t& av) { sv = av; }, [&](const county_t& av) { cv = av; },
                            [&](const city_t& av) { civ = av; }},
                 v);
   }

   template <typename R>
   static void seek_merged(std::shared_ptr<MergedScannerType<nation2_t, states_t, county_t, city_t>> scanner,
                           const typename R::Key& k,
                           std::optional<nation2_t>& nv,
                           std::optional<states_t>& sv,
                           std::optional<county_t>& cv,
                           std::optional<city_t>& civ)
   {
      bool found = scanner->template seek<R>(k);
      assert(found);
      auto kv = scanner->current();
      assert(kv != std::nullopt);
      auto& [_, v] = *kv;
      find_base_v(v, nv, sv, cv, civ);
   }

   void point_query_by_merged()
   {
      std::shared_ptr<MergedScannerType<nation2_t, states_t, county_t, city_t>> scanner = std::move(merged.getScanner());
      std::optional<nation2_t> nv = std::nullopt;
      std::optional<states_t> sv = std::nullopt;
      std::optional<county_t> cv = std::nullopt;
      std::optional<city_t> civ = std::nullopt;
      // find sort key and city
      bool ret = scanner->template seekTyped<city_t>(city_t::Key{workload.getNationID(), getStateKey(), getCountyKey(), getCityKey()});
      if (!ret)
         return;
      sort_key_t target_sk;
      auto target_kv = scanner->current();
      assert(target_kv != std::nullopt);
      auto& [k, v] = *target_kv;
      std::visit(overloaded{[&](const nation2_t::Key&) { UNREACHABLE(); }, [&](const states_t::Key&) { UNREACHABLE(); },
                            [&](const county_t::Key&) { UNREACHABLE(); }, [&](const city_t::Key& ak) { target_sk = ak.get_jk(); }},
                 k);
      find_base_v(v, nv, sv, cv, civ);

      seek_merged<county_t>(scanner, county_t::Key{target_sk.nationkey, target_sk.statekey, target_sk.countykey}, nv, sv, cv, civ);
      seek_merged<states_t>(scanner, states_t::Key{target_sk.nationkey, target_sk.statekey}, nv, sv, cv, civ);
      seek_merged<nation2_t>(scanner, nation2_t::Key{target_sk.nationkey}, nv, sv, cv, civ);

      view_t vv{nv.value(), sv.value(), cv.value(), civ.value()};
   }

   void point_query_by_base()
   {
      std::optional<city_t::Key> cik = std::nullopt;
      std::optional<city_t> civ = std::nullopt;

      city.scan(
          city_t::Key{workload.getNationID(), getStateKey(), getCountyKey(), getCityKey()},
          [&](const city_t::Key& k, const city_t& v) {
             cik = k;
             civ = v;
             return false;
          },
          []() {});
      if (civ.has_value() && cik.has_value()) {
         std::optional<county_t> cv = std::nullopt;
         county.lookup1(county_t::Key{cik->nationkey, cik->statekey, cik->countykey}, [&](const county_t& v) { cv = v; });

         std::optional<states_t> sv = std::nullopt;
         states.lookup1(states_t::Key{cik->nationkey, cik->statekey}, [&](const states_t& v) { sv = v; });

         std::optional<nation2_t> nv = std::nullopt;
         nation.lookup1(nation2_t::Key{cik->nationkey}, [&](const nation2_t& v) { nv = v; });
         if (nv.has_value() && sv.has_value() && cv.has_value() && civ.has_value()) {
            view_t::Key vk{cik->nationkey, cik->statekey, cik->countykey, cik->citykey};
         }
      }
   }

   // -------------------------------------------------------------
   // ---------------------- RANGE QUERIES ------------------------
   // Find all joined rows for the same nationkey

   void range_query_by_view()
   {
      auto nationkey = workload.getNationID();
      [[maybe_unused]] long produced = 0;
      view.scan(
          view_t::Key{nationkey, 0, 0, 0},
          [&](const view_t::Key& k, const view_t&) {
             if (k.jk.nationkey != nationkey)
                return false;
             TPCH::inspect_produced("Range querying materialized view: ", produced);
             return true;
          },
          []() {});
   }

   void range_query_by_merged()
   {
      auto scanner = merged.getScanner();
      int n = workload.getNationID();
      scanner->template seekForPrev<nation2_t>(nation2_t::Key{n});
      int curr_statekey, curr_countykey, curr_citykey;
      std::optional<nation2_t> nv = std::nullopt;
      std::optional<states_t> sv = std::nullopt;
      std::optional<county_t> cv = std::nullopt;
      std::optional<city_t> civ = std::nullopt;
      bool end = false;
      [[maybe_unused]] long produced = 0;

      while (!end) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         std::visit(overloaded{[&](const nation2_t::Key& nk) {
                                  if (nk.nationkey != n)
                                     end = true;
                               },
                               [&](const states_t::Key& sk) {
                                  assert(sk.nationkey == n);
                                  cv = std::nullopt;
                                  civ = std::nullopt;
                                  curr_statekey = sk.statekey;
                               },
                               [&](const county_t::Key& ck) {
                                  assert(ck.nationkey == n && ck.statekey == curr_statekey);
                                  civ = std::nullopt;
                                  curr_countykey = ck.countykey;
                               },
                               [&](const city_t::Key& ci) {
                                  assert(ci.nationkey == n && ci.statekey == curr_statekey && ci.countykey == curr_countykey);
                                  curr_citykey = ci.citykey;
                               }},
                    k);
         find_base_v(v, nv, sv, cv, civ);
         if (nv.has_value() && sv.has_value() && cv.has_value() && civ.has_value()) {
            view_t::Key vk{n, curr_statekey, curr_countykey, curr_citykey};
            view_t vv{nv.value(), sv.value(), cv.value(), civ.value()};
            TPCH::inspect_produced("Range querying merged: ", produced);
         }
      }
   }

   template <typename R>
   static std::optional<std::pair<typename R::Key, R>> conditional_next(std::shared_ptr<ScannerType<R>> scanner, int n)
   {
      auto kv = scanner->next();
      if (kv == std::nullopt)
         return std::nullopt;
      auto& [k, v] = *kv;
      if (k.nationkey != n)
         return std::nullopt;
      return kv;
   }

   template <typename JK, typename JR, typename... Rs>
   static std::optional<std::pair<typename JR::Key, JR>> conditional_next(BinaryMergeJoin<JK, JR, Rs...>& join, int n)
   {
      auto kv = join.next();
      if (kv == std::nullopt)
         return std::nullopt;
      auto& [k, v] = *kv;
      if (k.jk.nationkey != n)
         return std::nullopt;
      return kv;
   }

   void range_query_by_base()
   {
      auto n = workload.getNationID();

      std::shared_ptr<ScannerType<nation2_t>> nation_scanner = std::move(nation.getScanner());
      std::shared_ptr<ScannerType<states_t>> states_scanner = std::move(states.getScanner());
      std::shared_ptr<ScannerType<county_t>> county_scanner = std::move(county.getScanner());
      std::shared_ptr<ScannerType<city_t>> city_scanner = std::move(city.getScanner());

      BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t> binary_join_ns([&]() { return conditional_next<nation2_t>(nation_scanner, n); },
                                                                            [&]() { return conditional_next<states_t>(states_scanner, n); });
      BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t> binary_join_nsc(
          [&]() { return conditional_next<sort_key_t, ns_t, nation2_t, states_t>(binary_join_ns, n); },
          [&]() { return conditional_next<county_t>(county_scanner, n); });
      BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t> binary_join(
          [&]() { return conditional_next<sort_key_t, nsc_t, ns_t, county_t>(binary_join_nsc, n); },
          [&]() { return conditional_next<city_t>(city_scanner, n); });
      binary_join.run();
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------
   // insert a new county and multiple cities // TODO: no new state

   auto maintain_base()
   {
      int n = workload.getNationID();
      int s;
      nation.lookup1(nation2_t::Key{n}, [&](const nation2_t& n) { s = urand(1, n.last_statekey); });
      UpdateDescriptorGenerator1(states_update_desc, states_t, last_countykey);
      int c;
      states.update1(states_t::Key{n, s}, [&](states_t& s) { c = ++s.last_countykey; }, states_update_desc);
      int city_cnt = urand(1, CITY_MAX);
      county.insert(county_t::Key{n, s, c}, county_t::generateRandomRecord(city_cnt));
      for (int i = 1; i <= city_cnt; i++) {
         city.insert(city_t::Key{n, s, c, i}, city_t::generateRandomRecord());
      }
      return std::make_tuple(n, s, c, city_cnt);
   }

   void maintain_merged()
   {
      int n = workload.getNationID();
      int s;
      // UpdateDescriptorGenerator1(nation_update_desc, nation2_t, last_statekey);
      merged.template lookup1<nation2_t>(nation2_t::Key{n}, [&](const nation2_t& n) { s = urand(1, n.last_statekey); });
      int c;
      UpdateDescriptorGenerator1(states_update_desc, states_t, last_countykey);
      merged.template update1<states_t>(states_t::Key{n, s}, [&](states_t& s) { c = ++s.last_countykey; }, states_update_desc);
      int city_cnt = urand(1, CITY_MAX);
      merged.insert(county_t::Key{n, s, c}, county_t::generateRandomRecord(city_cnt));
      for (int i = 1; i <= city_cnt; i++) {
         merged.insert(city_t::Key{n, s, c, i}, city_t::generateRandomRecord());
      }
   }

   void update_state_in_view(int n, int s, std::function<void(states_t&)> update_fn)
   {
      std::vector<view_t::Key> keys;
      view.scan(
          view_t::Key{n, s, 0, 0},
          [&](const view_t::Key& k, const view_t&) {
             if (k.jk.nationkey != n || k.jk.statekey != s)
                return false;
             keys.push_back(k);
             return true;
          },
          []() {});
      for (auto& k : keys) {
         view.update1(k, [&](view_t& v) { update_fn(std::get<1>(v.payloads)); });
      }
   }

   void maintain_view()
   {
      int n, s, c, city_cnt;
      std::tie(n, s, c, city_cnt) = maintain_base();
      std::function<void(states_t&)> update_fn = [&](states_t& s) { s.last_countykey = c; };
      update_state_in_view(n, s, update_fn);
      for (int i = 1; i <= city_cnt; i++) {
         view.insert(view_t::Key{n, s, c, i}, view_t::generateRandomRecord(s, c, i));
      }
   }

   // -------------------------------------------------------------
   // ---------------------- LOADING -----------------------------

   static constexpr size_t STATE_MAX = 80;   // in a nation
   static constexpr size_t COUNTY_MAX = 50;  // in a state
   static constexpr size_t CITY_MAX = 20;    // in a county

   static int getStateKey() { return urand(1, STATE_MAX); }

   static int getCountyKey() { return urand(1, COUNTY_MAX); }

   static int getCityKey() { return urand(1, CITY_MAX); }

   void load()
   {
      workload.load();
      // county id starts from 1 in each s tate
      // city id starts from 1 in each county
      for (int n = 1; n <= workload.NATION_COUNT; n++) {
         // state id starts from 1 in each nation
         int state_cnt = urand(1, STATE_MAX);
         UpdateDescriptorGenerator1(nation_update_desc, nation2_t, last_statekey);
         auto nk = nation2_t::Key{n};
         nation2_t nv;
         auto update_fn = [&](nation2_t& n) {
            n.last_statekey = state_cnt;
            nv = n;
         };
         nation.update1(nk, update_fn, nation_update_desc);
         merged.insert(nk, nv);
         for (int s = 1; s <= state_cnt; s++) {
            std::cout << "\rLoading nation " << n << "/" << workload.NATION_COUNT << ", state " << s << "/" << state_cnt << "...";
            int county_cnt = urand(1, COUNTY_MAX);
            states.insert(states_t::Key{n, s}, states_t::generateRandomRecord(county_cnt));
            merged.insert(states_t::Key{n, s}, states_t::generateRandomRecord(county_cnt));
            for (int c = 1; c <= county_cnt; c++) {
               int city_cnt = urand(1, CITY_MAX);
               county.insert(county_t::Key{n, s, c}, county_t::generateRandomRecord(city_cnt));
               merged.insert(county_t::Key{n, s, c}, county_t::generateRandomRecord(city_cnt));
               for (int ci = 1; ci <= city_cnt; ci++) {
                  city.insert(city_t::Key{n, s, c, ci}, city_t::generateRandomRecord());
                  merged.insert(city_t::Key{n, s, c, ci}, city_t::generateRandomRecord());
                  view.insert(view_t::Key{n, s, c, ci}, view_t::generateRandomRecord(state_cnt, county_cnt, city_cnt));
               }
            }
         }
      }
      log_sizes();
   }

   void log_sizes()
   {
      workload.log_sizes();
      double indexes_size = nation.size() + states.size() + county.size() + city.size();
      std::map<std::string, double> sizes = {{"view", view.size() + indexes_size},
                                             {"indexes", indexes_size},
                                             {"nation", nation.size()},
                                             {"states", states.size()},
                                             {"county", county.size()},
                                             {"city", city.size()},
                                             {"merged", merged.size()}};
      logger.log_sizes(sizes);
   }
};
}  // namespace geo_join