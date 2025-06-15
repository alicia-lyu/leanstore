#pragma once

#include <cstddef>
#include <optional>
#include <variant>
#include "../tpch_workload.hpp"
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

   long long ns_sum = 0;
   long long ns_count = 0;

   long long nsc_sum = 0;
   long long nsc_count = 0;

   long long nscci_sum = 0;
   long long nscci_count = 0;

   Logger& logger;

  public:
   GeoJoin(TPCH& workload,
           MergedTree& m,
           AdapterType<view_t>& v,
           MergedAdapterType<nation2_t, states_t>& ns,
           MergedAdapterType<county_t, city_t, customer2_t>& ccc,
           AdapterType<states_t>& s,
           AdapterType<county_t>& c,
           AdapterType<city_t>& ci,
           AdapterType<customer2_t>& customer2)
       : workload(workload),
         merged(m),
         join_view(v),
         ns(ns),
         ccc(ccc),
         nation(reinterpret_cast<AdapterType<nation2_t>&>(workload.nation)),
         states(s),
         county(c),
         city(ci),
         customer2(customer2),
         logger(workload.logger)
   {
      TPCH::CUSTOMER_SCALE *= 200;
   }

   ~GeoJoin()
   {
      std::cout << "Average ns produced: " << (ns_count > 0 ? (double)ns_sum / ns_count : 0) << std::endl;
      std::cout << "Average nsc produced: " << (nsc_count > 0 ? (double)nsc_sum / nsc_count : 0) << std::endl;
      std::cout << "Average nscci produced: " << (nscci_count > 0 ? (double)nscci_sum / nscci_count : 0) << std::endl;
   }

   std::optional<sort_key_t> find_random_geo_key_in_base()
   {
      int n = workload.getNationID();
      int s = params::get_statekey();
      int c = params::get_countykey();
      int ci = params::get_citykey();

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
      auto sk = {workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey(), 0};

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
      if (!found) // too large a key
         return std::nullopt;
      return std::make_optional(sk);
   }

   std::optional<sort_key_t> find_random_geo_key_in_merged()
   {
      int n = workload.getNationID();
      int s = params::get_statekey();
      int c = params::get_countykey();
      int ci = params::get_citykey();

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
      int n = workload.getNationID();
      int s = params::get_statekey();
      int c = params::get_countykey();
      int ci = params::get_citykey();

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
      workload.customer.scan(customerh_t::Key{workload.getCustomerID()}, [](const customerh_t::Key&, const customerh_t&) { return false; }, []() {});
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
      ns_sum += range_query_by_view(workload.getNationID(), params::get_statekey(), 0, 0);
      ns_count++;
   }
   void ns_merged()
   {
      ns_sum += range_query_by_merged(workload.getNationID(), params::get_statekey(), 0, 0);
      ns_count++;
   }
   void ns_base()
   {
      ns_sum += range_query_by_base(workload.getNationID(), params::get_statekey(), 0, 0);
      ns_count++;
   }

   void ns_by_2merged() {
      ns_sum += range_query_by_2merged(workload.getNationID(), params::get_statekey(), 0, 0);
      ns_count++;
   }

   // Find all joined rows for the same nationkey, statekey, countykey
   void nsc_view()
   {
      nsc_sum += range_query_by_view(workload.getNationID(), params::get_statekey(), params::get_countykey(), 0);
      nsc_count++;
   }
   void nsc_merged()
   {
      nsc_sum += range_query_by_merged(workload.getNationID(), params::get_statekey(), params::get_countykey(), 0);
      nsc_count++;
   }
   void nsc_base()
   {
      nsc_sum += range_query_by_base(workload.getNationID(), params::get_statekey(), params::get_countykey(), 0);
      nsc_count++;
   }

   void nsc_by_2merged() {
      nsc_sum += range_query_by_2merged(workload.getNationID(), params::get_statekey(), params::get_countykey(), 0);
      nsc_count++;
   }

   void nscci_by_view()
   {
      nscci_sum += range_query_by_view(workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey());
      nscci_count++;
   }

   void nscci_by_merged()
   {
      nscci_sum += range_query_by_merged(workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey());
      nscci_count++;
   }

   void nscci_by_base()
   {
      nscci_sum += range_query_by_base(workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey());
      nscci_count++;
   }

   void nscci_by_2merged() {
      nscci_sum += range_query_by_2merged(workload.getNationID(), params::get_statekey(), params::get_countykey(), params::get_citykey());
      nscci_count++;
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------

   auto maintain_base();

   void maintain_merged();

   void maintain_view();

   void maintain_2merged();

   // -------------------------------------------------------------
   // ---------------------- LOADING -----------------------------
   // TODO: load group view

   void load();

   double get_view_size();

   double get_indexes_size();

   double get_merged_size();

   double get_2merged_size();

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

   // --------------------------------------------------------------
   // ---------------------- GROUP-BY ------------------------------

   void agg_in_view();
   void agg_by_merged();
   void agg_by_base();
   void agg_by_2merged();

   void point_agg_by_view();
   void point_agg_by_merged();
   void point_agg_by_base();
   void point_agg_by_2merged();
};
}  // namespace geo_join
// #include "groupby_query.tpp"  // IWYU pragma: keep
#include "join_queries.tpp"   // IWYU pragma: keep
#include "load.tpp"           // IWYU pragma: keep
#include "maintain.tpp"       // IWYU pragma: keep
#include "mixed_query.tpp"    // IWYU pragma: keep