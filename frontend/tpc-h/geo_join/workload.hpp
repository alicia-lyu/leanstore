#pragma once

#include <chrono>

#include "../../shared/Adapter.hpp"
#include "../merge.hpp"
#include "../tpch_workload.hpp"
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
      logger.log(t, ColumnName::ELAPSED, "query-view");
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

      point_query_by_view(nationkey, statekey, countykey, citykey);
   }

   void point_query_by_view(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      sort_key_t sk = sort_key_t{0, 0, 0, 0};
      int produced = 0;
      view.scan(
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
      
      if (produced != 2) {
         __builtin_debugtrap();
      }
   }

   void point_query_by_merged()
   {
      sort_key_t sk = sort_key_t{workload.getNationID(), getStateKey(), getCountyKey(), getCityKey()};

      auto scanner = merged.getScanner();

      city_t::Key* cik = nullptr;
      city_t* civ = nullptr;

      scanner->template seekTyped<city_t>(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey});
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

   void point_query_by_base()
   {
      sort_key_t sk = sort_key_t{workload.getNationID(), getStateKey(), getCountyKey(), getCityKey()};

      auto nation_scanner = nation.getScanner();
      nation_scanner->seekForPrev(nation2_t::Key{sk.nationkey});
      auto states_scanner = states.getScanner();
      states_scanner->seekForPrev(states_t::Key{sk.nationkey, sk.statekey});
      auto county_scanner = county.getScanner();
      county_scanner->seekForPrev(county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
      auto city_scanner = city.getScanner();
      city_scanner->seekForPrev(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey});

      BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t> binary_join_ns([&]() { return nation_scanner->next(); },
                                                                            [&]() { return states_scanner->next(); });
      BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t> binary_join_nsc([&]() { return binary_join_ns.next(); },
                                                                         [&]() { return county_scanner->next(); });
      BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t> binary_join([&]() { return binary_join_nsc.next(); },
                                                                     [&]() { return city_scanner->next(); });
      binary_join.next();
   }

   // -------------------------------------------------------------
   // ---------------------- RANGE QUERIES ------------------------
   // Find all joined rows for the same nationkey

   void range_query_by_view()
   {
      logger.reset();
      std::cout << "GeoJoin::range_query_by_view()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

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

      std::cout << "\rRange querying materialized view for nation " << nationkey << " : " << (double)produced / 1000
                << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "range-query-view");
   }

   void range_query_by_merged()
   {
      logger.reset();
      std::cout << "GeoJoin::range_query_by_merged()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      auto scanner = merged.getScanner();
      int n = workload.getNationID();
      scanner->template seekForPrev<nation2_t>(nation2_t::Key{n});
      scanner->prev();

      sort_key_t curr_jk = sort_key_t{n, 0, 0, 0};
      long produced = 0;
      long curr_produced = 0;
      int jk_cnt = 0;
      while (curr_jk.nationkey == n) {
         std::tie(curr_jk, curr_produced) = scanner->template next_jk<sort_key_t, view_t>();
         __builtin_debugtrap();
         produced += curr_produced;
         jk_cnt++;
      }
      std::cout << "\rRange querying merged for nation " << n << " : jk_cnt " << jk_cnt << ", produced" << produced << " records------------------------------------" << std::endl;

      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "range-query-merged");
   }

   void range_query_by_base()
   {
      logger.reset();
      std::cout << "GeoJoin::range_query_by_base()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      auto n = workload.getNationID();

      auto nation_scanner = nation.getScanner();
      nation_scanner->seekForPrev(nation2_t::Key{n});
      nation_scanner->prev();
      auto states_scanner = states.getScanner();
      states_scanner->seekForPrev(states_t::Key{n, 0});
      states_scanner->prev();
      auto county_scanner = county.getScanner();
      county_scanner->seekForPrev(county_t::Key{n, 0, 0});
      county_scanner->prev();
      auto city_scanner = city.getScanner();
      city_scanner->seekForPrev(city_t::Key{n, 0, 0, 0});
      city_scanner->prev();

      BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t> binary_join_ns([&]() { return nation_scanner->next(); },
                                                                            [&]() { return states_scanner->next(); });
      BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t> binary_join_nsc([&]() { return binary_join_ns.next(); },
                                                                         [&]() { return county_scanner->next(); });
      BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t> binary_join([&]() { return binary_join_nsc.next(); },
                                                                     [&]() { return city_scanner->next(); });
      while (binary_join.current_jk.nationkey == n || binary_join.current_jk == sort_key_t::max()) {
         binary_join.next();
      }

      std::cout << "\rRange querying base for nation " << n << " produced " << binary_join.produced << " records------------------------------------"
                << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "range-query-base");
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