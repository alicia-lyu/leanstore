#pragma once

#include <cstddef>
#include "../tpc-h/workload.hpp"
#include "leanstore/Config.hpp"
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

   AdapterType<mixed_view_t>& mixed_view;
   AdapterType<view_t>& join_view;

   MergedAdapterType<nation2_t, states_t>& ns;
   MergedAdapterType<county_t, city_t, customer2_t>& ccc;

   AdapterType<nation2_t>& nation;
   AdapterType<states_t>& states;
   AdapterType<county_t>& county;
   AdapterType<city_t>& city;
   AdapterType<customer2_t>& customer2;

   Params params;

   double indexes_size = 0.0;
   double view_size = 0.0;
   double merged_size = 0.0;
   double merged2_size = 0.0;

   long long ns_sum = 0;
   long long ns_count = 0;

   long long nsc_sum = 0;
   long long nsc_count = 0;

   long long nscci_sum = 0;
   long long nscci_count = 0;

   long long mixed_point_customer_sum = 0;
   long long mixed_point_tx_count = 0;

   long long find_rdkey_scans = 0;

   Logger& logger;

   std::vector<sort_key_t> to_insert;

  public:
   GeoJoin(TPCH& workload,
           MergedTree& m,
           AdapterType<mixed_view_t>& mixed_view,
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
         mixed_view(mixed_view),
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
      std::cout << "Total find_rdkey_scans: " << find_rdkey_scans << std::endl;
      ns_sum = 0;
      ns_count = 0;
      nsc_sum = 0;
      nsc_count = 0;
      nscci_sum = 0;
      nscci_count = 0;
      find_rdkey_scans = 0;
   }

   ~GeoJoin() { reset_sum_counts(); }

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
   void ns5join_view()
   {
      ns_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }
   void ns5join_merged()
   {
      ns_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }
   void ns5join_base()
   {
      ns_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }

   void ns5join_2merged()
   {
      ns_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }

   // Find all joined rows for the same nationkey, statekey, countykey
   void nsc5join_view()
   {
      nsc_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }
   void nsc5join_merged()
   {
      nsc_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }
   void nsc5join_base()
   {
      nsc_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }

   void nsc5join_2merged()
   {
      nsc_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }

   void nscci5join_view()
   {
      nscci_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void nscci5join_merged()
   {
      nscci_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void nscci5join_base()
   {
      nscci_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void nscci5join_2merged()
   {
      nscci_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------

   int last_customer_id_old;

   void select_to_insert()
   {
      last_customer_id_old = workload.last_customer_id;
      size_t num_customers = last_customer_id_old / 100;
      std::cout << "Doing a full scan of cities to randomly select " << num_customers << " for insertion...";
      to_insert.reserve(num_customers);  // insert 1% of customers
      auto scanner = city.getScanner();
      while (true) {
         auto kv = scanner->next();
         if (!kv.has_value()) {
            break;  // no more cities
         }
         auto [k, v] = *kv;
         sort_key_t sk = SKBuilder<sort_key_t>::create(k, v);
         size_t i = to_insert.size();
         if (i < num_customers) {  // First, fill to_insert with the first 1% of cities
            to_insert.push_back(sk);
         } else {  // Then, for each subsequent key i, replace a random element in the reservoir with the new key with probability k/i.
            size_t j = rand() % (i + 1);
            if (j < num_customers) {
               to_insert.at(j) = sk;
            }
         }
         if (i % 100 == 1 && FLAGS_log_progress) {
            std::cout << "\rScanned " << i + 1 << " cities..." << std::flush;
         }
      }
      std::cout << std::endl;
   }

   bool insertion_complete() { return maintain_processed >= to_insert.size(); }

   size_t maintain_processed = 0;

   void maintain_base();

   void maintain_merged();

   void maintain_view();

   void maintain_2merged();

   void cleanup_base()
   {
      std::cout << "Cleaning up " << to_insert.size() << " customers..." << std::endl;
      for (const sort_key_t& sk : to_insert) {
         customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
         customer2.erase(cust_key);
      }
      assert(last_customer_id_old == workload.last_customer_id);
   }

   void cleanup_merged()
   {
      std::cout << "Cleaning up " << to_insert.size() << " customers..." << std::endl;
      for (const sort_key_t& sk : to_insert) {
         customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
         merged.template erase<customer2_t>(cust_key);
      }
      assert(last_customer_id_old == workload.last_customer_id);
   }

   void cleanup_view()
   {
      std::cout << "Cleaning up " << to_insert.size() << " customers..." << std::endl;
      for (const sort_key_t& sk : to_insert) {
         sort_key_t cust_sk{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
         view_t::Key vk{cust_sk};
         join_view.erase(vk);
         customer2_t::Key cust_key{cust_sk};
         customer2.erase(cust_key);
      }
      assert(last_customer_id_old == workload.last_customer_id);
   }

   void cleanup_2merged()
   {
      std::cout << "Cleaning up " << to_insert.size() << " customers..." << std::endl;
      for (const sort_key_t& sk : to_insert) {
         customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
         ccc.template erase<customer2_t>(cust_key);
      }
      assert(last_customer_id_old == workload.last_customer_id);
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
      if (view_size == 0.0) {
         auto mixed_view_size = mixed_view.size();
         std::cout << "Mixed view size: " << mixed_view_size << std::endl;
         view_size = get_indexes_size() + join_view.size() + mixed_view_size;
      }
      return view_size;
   }

   double get_indexes_size()
   {
      // return nation.size() + states.size() + county.size() + city.size() + customer2.size();
      if (indexes_size == 0.0) {
         indexes_size = nation.size() + states.size() + county.size() + city.size() + customer2.size();
      }
      return indexes_size;
   }

   double get_merged_size()
   {
      if (merged_size == 0.0) {
         merged_size = merged.size();
      }
      return merged_size;
   }

   double get_2merged_size()
   {
      if (merged2_size == 0.0) {
         merged2_size = ns.size() + ccc.size();
      }
      return merged2_size;
   }

   void log_sizes();

   // -------------------------------------------------------------
   // ---------------------- JOIN + GROUP-BY ----------------------

   void mixed_query_by_view();
   void mixed_query_by_merged();
   void mixed_query_by_base();

   void point_mixed_query_by_view();
   void point_mixed_query_by_merged();
   void point_mixed_query_by_base();
};
}  // namespace geo_join
// #include "groupby_query.tpp"  // IWYU pragma: keep
#include "join_w_search.tpp"  // IWYU pragma: keep
#include "load.tpp"          // IWYU pragma: keep
#include "maintain.tpp"      // IWYU pragma: keep
#include "mixed_query.tpp"   // IWYU pragma: keep