#pragma once

#include <cstddef>
#include <optional>
#include <variant>
#include "../tpc-h/workload.hpp"
#include "load.hpp"
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
   using MergedTree = MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>;

   MergedTree& merged;
   AdapterType<view_t>& join_view;

   MergedAdapterType<nation2_t, states_t>& ns;
   MergedAdapterType<county_t, city_t, customer2_t>& ccc;

   AdapterType<nation2_t>& nation;
   AdapterType<states_t>& states;
   AdapterType<county_t>& county;
   AdapterType<city_t>& city;
   AdapterType<customer2_t>& customer2;

   Params params;

   long long ns_sum = 0;
   long long ns_count = 0;

   long long nsc_sum = 0;
   long long nsc_count = 0;

   long long nscci_sum = 0;
   long long nscci_count = 0;

   long long mixed_point_customer_sum = 0;
   long long mixed_point_tx_count = 0;

   Logger& logger;

   std::vector<sort_key_t> inserted;  // in maintenance

  public:
   GeoJoin(TPCH& workload,
           MergedTree& m,
           AdapterType<view_t>& v,
           MergedAdapterType<nation2_t, states_t>& ns,
           MergedAdapterType<county_t, city_t, customer2_t>& ccc,
           AdapterType<nation2_t>& n,
           AdapterType<states_t>& s,
           AdapterType<county_t>& c,
           AdapterType<city_t>& ci,
           AdapterType<customer2_t>& customer2)
       : workload(workload),
         merged(m),
         join_view(v),
         ns(ns),
         ccc(ccc),
         nation(n),
         states(s),
         county(c),
         city(ci),
         customer2(customer2),
         logger(workload.logger)
   {
      if (FLAGS_tpch_scale_factor > 1000) {
         throw std::runtime_error("GeoJoin does not support scale factor larger than 1000");
      }
      TPCH::CUSTOMER_SCALE *= 200;  // already linear to scale factor
   }

   void reset_sum_counts()
   {
      std::cout << "Average ns produced: " << (ns_count > 0 ? (double)ns_sum / ns_count : 0) << std::endl;
      std::cout << "Average nsc produced: " << (nsc_count > 0 ? (double)nsc_sum / nsc_count : 0) << std::endl;
      std::cout << "Average nscci produced: " << (nscci_count > 0 ? (double)nscci_sum / nscci_count : 0) << std::endl;
      std::cout << "Average customer count in mixed point tx: "
                << (mixed_point_tx_count > 0 ? (double)mixed_point_customer_sum / mixed_point_tx_count : 0) << std::endl;
      ns_sum = 0;
      ns_count = 0;
      nsc_sum = 0;
      nsc_count = 0;
      nscci_sum = 0;
      nscci_count = 0;
   }

   ~GeoJoin() { reset_sum_counts(); }

   std::optional<sort_key_t> find_random_geo_key_in_base()
   {
      int n = params.get_nationkey();
      int s = params.get_statekey();
      int c = params.get_countykey();
      int ci = params.get_citykey();

      bool found = false;

      city.scan(
          city_t::Key{n, s, c, ci},
          [&](const city_t::Key& k, const city_t&) {
             n = k.nationkey;
             s = k.statekey;
             c = k.countykey;
             ci = k.citykey;
             found = true;
             return false;  // stop after the first match
          },
          []() {});
      if (!found)
         return std::nullopt;

      return std::make_optional(sort_key_t{n, s, c, ci, 0});
   }

   std::optional<sort_key_t> find_random_geo_key_in_view()
   {
      sort_key_t sk = {params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0};

      bool found = false;
      join_view.scan(
          sk,
          [&](const view_t::Key& k, const view_t&) {
             sk = k.jk;
             sk.custkey = 0;
             found = true;
             return false;  // stop after the first match
          },
          []() {});
      if (!found)  // too large a key
         return std::nullopt;
      return std::make_optional(sk);
   }

   std::optional<sort_key_t> find_random_geo_key_in_merged()
   {
      int n = params.get_nationkey();
      int s = params.get_statekey();
      int c = params.get_countykey();
      int ci = params.get_citykey();

      auto scanner = merged.template getScanner<sort_key_t, view_t>();
      bool ret = scanner->template seekTyped<city_t>(city_t::Key{n, s, c, ci});
      if (!ret)
         return std::nullopt;

      auto kv = scanner->next();
      assert(kv.has_value());
      city_t::Key* cik = std::get_if<city_t::Key>(&kv->first);
      assert(cik != nullptr);
      return sort_key_t{cik->nationkey, cik->statekey, cik->countykey, cik->citykey, 0};
   }

   std::optional<sort_key_t> find_random_geo_key_in_2merged()
   {
      int n = params.get_nationkey();
      int s = params.get_statekey();
      int c = params.get_countykey();
      int ci = params.get_citykey();

      auto scanner = ccc.template getScanner<sort_key_t, ccc_t>();
      bool ret = scanner->template seekTyped<city_t>(city_t::Key{n, s, c, ci});
      if (!ret)
         return std::nullopt;
      auto kv = scanner->next();
      assert(kv.has_value());
      city_t::Key* cik = std::get_if<city_t::Key>(&kv->first);
      assert(cik != nullptr);
      return sort_key_t{cik->nationkey, cik->statekey, cik->countykey, cik->citykey, 0};
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
      workload.orders.scan(orders_t::Key{workload.getOrderID()}, [](const orders_t::Key&, const orders_t&) { return false; }, []() {});
      workload.lineitem.scan(lineitem_t::Key{workload.getOrderID(), 1}, [](const lineitem_t::Key&, const lineitem_t&) { return false; }, []() {});
   }

   // -------------------------------------------------------------------
   // ------------------------ JOIN QUERIES -----------------------------

   void query_by_view();  // scan through the view

   void query_by_merged();

   void query_by_base();

   void query_by_2merged();

   // -------------------------------------------------------------------
   // ---------------------- POINT JOIN QUERIES --------------------------
   // Find all joined rows for the same join key

   // -------------------------------------------------------------------
   // ---------------------- RANGE JOIN QUERIES ------------------------
   // Find all joined rows for the same nationkey

   size_t range_query_by_view(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);
   size_t range_query_by_merged(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);
   size_t range_query_by_base(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);
   size_t range_query_by_2merged(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);

   // Find all joined rows for the same nationkey, statekeyZ
   void ns_view()
   {
      ns_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }
   void ns_merged()
   {
      ns_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }
   void ns_base()
   {
      ns_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }

   void ns_by_2merged()
   {
      ns_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }

   // Find all joined rows for the same nationkey, statekey, countykey
   void nsc_view()
   {
      nsc_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }
   void nsc_merged()
   {
      nsc_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }
   void nsc_base()
   {
      nsc_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }

   void nsc_by_2merged()
   {
      nsc_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }

   void nscci_by_view()
   {
      nscci_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void nscci_by_merged()
   {
      nscci_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void nscci_by_base()
   {
      nscci_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void nscci_by_2merged()
   {
      nscci_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------

   std::pair<customer2_t::Key, customer2_t> maintain_base();

   void maintain_merged();

   void maintain_view();

   void maintain_2merged();

   void cleanup_base()
   {
      for (const sort_key_t& sk : inserted) {
         customer2_t::Key cust_key{sk};
         customer2.erase(cust_key);
      }
   }

   void cleanup_merged()
   {
      for (const sort_key_t& sk : inserted) {
         customer2_t::Key cust_key{sk};
         merged.template erase<customer2_t>(cust_key);
      }
   }

   void cleanup_view()
   {
      for (const sort_key_t& sk : inserted) {
         view_t::Key vk{sk};
         join_view.erase(vk);
         customer2_t::Key cust_key{sk};
         customer2.erase(cust_key);
      }
   }

   void cleanup_2merged()
   {
      for (const sort_key_t& sk : inserted) {
         customer2_t::Key cust_key{sk};
         ccc.template erase<customer2_t>(cust_key);
      }
   }

   // -------------------------------------------------------------
   // ---------------------- LOADING -----------------------------

   LoadState load_state;

   void load();

   void seq_load();

   void load_1state(int n, int s);

   void load_1county(int n, int s, int c);

   void load_1city(int n, int s, int c, int ci);

   void load_1customer(int n, int s, int c, int ci, int cu, bool insert_view);

   double get_view_size()
   {
      double indexes_size = get_indexes_size();
      return indexes_size + join_view.size();  // + city_count_per_county.size();
   }

   double get_indexes_size() { return nation.size() + states.size() + county.size() + city.size() + customer2.size(); }

   double get_merged_size() { return merged.size(); }

   double get_2merged_size() { return ns.size() + ccc.size(); }

   void log_sizes();

   // -------------------------------------------------------------
   // ---------------------- JOIN + GROUP-BY ----------------------

   void mixed_query_by_view();
   void mixed_query_by_merged();
   void mixed_query_by_base();
   void mixed_query_by_2merged();

   void point_mixed_query_by_view();
   void point_mixed_query_by_merged();
   void point_mixed_query_by_base();
   void point_mixed_query_by_2merged();
};
}  // namespace geo_join
// #include "groupby_query.tpp"  // IWYU pragma: keep
#include "join_queries.tpp"  // IWYU pragma: keep
#include "load.tpp"          // IWYU pragma: keep
#include "maintain.tpp"      // IWYU pragma: keep
#include "mixed_query.tpp"   // IWYU pragma: keep