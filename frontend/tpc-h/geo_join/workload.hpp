#pragma once

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
   AdapterType<view_t>& join_view;
   AdapterType<city_count_per_county_t>& city_count_per_county;

   AdapterType<nation2_t>& nation;
   AdapterType<states_t>& states;
   AdapterType<county_t>& county;
   AdapterType<city_t>& city;

   Logger& logger;

   int range_join_n;

  public:
   GeoJoin(TPCH& workload,
           MergedTree& m,
           AdapterType<view_t>& v,
           AdapterType<city_count_per_county_t>& g,
           AdapterType<states_t>& s,
           AdapterType<county_t>& c,
           AdapterType<city_t>& ci)
       : workload(workload),
         merged(m),
         join_view(v),
         city_count_per_county(g),
         nation(reinterpret_cast<AdapterType<nation2_t>&>(workload.nation)),
         states(s),
         county(c),
         city(ci),
         logger(workload.logger),
         range_join_n(workload.getNationID())
   {
      std::cout << "GeoJoin::GeoJoin(): range_join_n = " << range_join_n << std::endl;
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

   // -------------------------------------------------------------------
   // ---------------------- POINT JOIN QUERIES --------------------------
   // Find all joined rows for the same join key

   void point_query_by_view();

   void point_query_by_view(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);

   void point_query_by_merged();

   void point_query_by_base();

   // -------------------------------------------------------------------
   // ---------------------- RANGE JOIN QUERIES ------------------------
   // Find all joined rows for the same nationkey

   void range_query_by_view();

   void range_query_by_merged();

   void range_query_by_base();

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------
   // insert a new county and multiple cities // TODO: update group view

   auto maintain_base();

   void maintain_merged();

   void update_state_in_view(int n, int s, std::function<void(states_t&)> update_fn);

   void maintain_view();

   // -------------------------------------------------------------
   // ---------------------- LOADING -----------------------------
   // TODO: load group view

   void load();

   double get_view_size();

   double get_indexes_size();

   double get_merged_size();

   void log_sizes();

   void log_sizes_other();

   // -------------------------------------------------------------
   // ---------------------- JOIN + GROUP-BY ----------------------

   void mixed_query_by_view();
   void mixed_query_by_merged();
   void mixed_query_by_base();

   void point_mixed_query_by_view();
   void point_mixed_query_by_merged();
   void point_mixed_query_by_base();

   // --------------------------------------------------------------
   // ---------------------- GROUP-BY ------------------------------

   void agg_in_view();
   void agg_by_merged();
   void agg_by_base();

   void point_agg_by_view();
   void point_agg_by_merged();
   void point_agg_by_base();
};
}  // namespace geo_join
#include "groupby_query.tpp"  // IWYU pragma: keep
#include "join_queries.tpp"   // IWYU pragma: keep
#include "load.tpp"           // IWYU pragma: keep
#include "maintain.tpp"       // IWYU pragma: keep
#include "mixed_query.tpp"    // IWYU pragma: keep