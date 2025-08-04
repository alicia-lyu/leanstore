#pragma once

#include <cstddef>
#include "../shared/adapter-scanner/Adapter.hpp"
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
   AdapterType<ccc_t>& ccc_view;

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

   int insert_city_count = 0;

  public:
   GeoJoin(TPCH& workload,
           MergedTree& m,
           AdapterType<mixed_view_t>& mixed_view,
           AdapterType<view_t>& v,
           AdapterType<ccc_t>& ccc_view,
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
         ccc_view(ccc_view),
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

   ~GeoJoin()
   {
      reset_sum_counts();
      std::cout << "Inserted/erased customers till " << workload.last_customer_id << std::endl;
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

   void fill_and_replace(const sort_key_t& sk)
   {
      size_t i = to_insert.size();
      if (i < insert_city_count) {  // First, fill to_insert with the first 1% of cities
         to_insert.push_back(sk);
      } else {  // Then, for each subsequent key i, replace a random element in the reservoir with the new key with probability k/i.
         size_t j = rand() % (i + 1);
         if (j < insert_city_count) {
            to_insert.at(j) = sk;
         }
      }
      if (i % 100 == 1 && FLAGS_log_progress) {
         std::cout << "\rScanned " << i + 1 << " cities..." << std::flush;
      }
   }

   void select_merged_to_insert()
   {
      if (insert_city_count == 0) {
         insert_city_count = workload.last_customer_id / 20;  // 5% is hardcoded
      }
      last_customer_id_old = workload.last_customer_id;
      std::cout << "Doing a full scan of cities to randomly select " << insert_city_count << " for insertion...";
      to_insert.clear();
      to_insert.reserve(insert_city_count);  // insert 1% of customers
      auto scanner = merged.template getScanner<sort_key_t, view_t>();
      while (true) {
         auto kv = scanner->next();
         if (!kv.has_value()) {
            break;  // no more cities
         }
         auto [k, v] = *kv;
         sort_key_t sk = SKBuilder<sort_key_t>::create(k, v);
         if (sk.citykey == 0 || sk.custkey != 0) {
            continue;  // skip non-city records
         }
         fill_and_replace(sk);
      }
      std::cout << std::endl;
      reset_maintain_ptrs();
   }

   void select_to_insert()
   {
      if (insert_city_count == 0) {
         insert_city_count = workload.last_customer_id / 20;  // 5% is hardcoded
      }
      last_customer_id_old = workload.last_customer_id;
      std::cout << "Doing a full scan of cities to randomly select " << insert_city_count << " for insertion...";
      to_insert.clear();
      to_insert.reserve(insert_city_count);  // insert 1% of customers
      auto scanner = city.getScanner();
      while (true) {
         auto kv = scanner->next();
         if (!kv.has_value()) {
            break;  // no more cities
         }
         auto [k, v] = *kv;
         sort_key_t sk = SKBuilder<sort_key_t>::create(k, v);
         fill_and_replace(sk);
      }
      std::cout << std::endl;
      reset_maintain_ptrs();
   }

   bool insertion_complete() { return maintain_processed >= to_insert.size(); }

   size_t maintain_processed = 0;

   void maintain_base();

   void maintain_merged();

   void maintain_view();

   void maintain_2merged();

   size_t maintain_erased = 0;

   void adjust_maintain_ptrs()
   {
      maintain_processed = maintain_processed % to_insert.size();  // as long as erase is active, allow maintain_processed to wrap around
      // insertions go rounds and rounds with no regard for erases
      if (maintain_erased == to_insert.size()) {  // erase just also go rounds and rounds, as long as erased customer was
         // inserted (last_customer_id_old <= workload.last_customer_id)
         if (FLAGS_log_progress) {
            std::cout << "Resetting maintain_erased to 0. customer_ids that remain in db: " << last_customer_id_old << "--------"
                      << workload.last_customer_id << std::endl;
         }
         maintain_erased = 0;
      }
   }

   void reset_maintain_ptrs()
   {
      maintain_processed = 0;
      maintain_erased = 0;
      last_customer_id_old = workload.last_customer_id;
      std::cout << "Resetting maintain pointers. Last customer id: " << last_customer_id_old << std::endl;
   }

   int remaining_customers_to_erase() const { return workload.last_customer_id - last_customer_id_old; }

   bool erase_base()
   {
      if (last_customer_id_old >= workload.last_customer_id) {
         return false;  // no more customers to erase
      }
      const sort_key_t& sk = to_insert.at(maintain_erased++);
      customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
      bool ret = customer2.erase(cust_key);
      if (!ret)
         std::cerr << "Error erasing customer with key: " << cust_key << std::endl;
      adjust_maintain_ptrs();
      return true;
   }

   bool erase_merged()
   {
      if (last_customer_id_old >= workload.last_customer_id) {
         return false;  // no more customers to erase
      }
      const sort_key_t& sk = to_insert.at(maintain_erased++);
      customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
      bool ret = merged.template erase<customer2_t>(cust_key);
      if (!ret)
         std::cerr << "Error erasing customer with key: " << cust_key << std::endl;
      adjust_maintain_ptrs();
      return true;
   }

   bool erase_view()
   {
      if (last_customer_id_old >= workload.last_customer_id) {
         return false;  // no more customers to erase
      }
      const sort_key_t& sk = to_insert.at(maintain_erased++);
      customer2_t::Key cust_key{sk};
      cust_key.custkey = ++last_customer_id_old;
      view_t::Key vk{cust_key};
      bool ret_jv = join_view.erase(vk);
      bool ret_c = customer2.erase(cust_key);
      if (!ret_jv || !ret_c) {
         throw std::runtime_error("Error erasing customer in view, ret_jv: " + std::to_string(ret_jv) + ", ret_c: " + std::to_string(ret_c));
      }
      mixed_view_t::Key mixed_vk{sk};
      UpdateDescriptorGenerator1(mixed_view_decrementer, mixed_view_t, payloads);
      mixed_view.update1(mixed_vk, [](mixed_view_t& v) { std::get<4>(v.payloads).customer_count--; }, mixed_view_decrementer);
      adjust_maintain_ptrs();
      return true;
   }

   bool erase_2merged() { throw std::runtime_error("erase_2merged not implemented"); }

   void check_last_customer_id()
   {
      if (last_customer_id_old != workload.last_customer_id) {
         std::cerr << "Error: last_customer_id_old (" << last_customer_id_old << ") does not match workload.last_customer_id ("
                   << workload.last_customer_id << ")" << std::endl;
      }
   }

   void cleanup_base()
   {
      std::cout << "Cleaning up " << to_insert.size() << " customers..." << std::endl;
      for (; maintain_erased < to_insert.size(); maintain_erased++) {
         const sort_key_t& sk = to_insert.at(maintain_erased);
         customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
         customer2.erase(cust_key);
      }
      check_last_customer_id();
   }

   void cleanup_merged()
   {
      std::cout << "Cleaning up " << to_insert.size() << " customers..." << std::endl;
      for (; maintain_erased < to_insert.size(); maintain_erased++) {
         const sort_key_t& sk = to_insert.at(maintain_erased);
         customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
         merged.template erase<customer2_t>(cust_key);
      }
      check_last_customer_id();
   }

   void cleanup_view()
   {
      std::cout << "Cleaning up " << to_insert.size() << " customers..." << std::endl;
      for (; maintain_erased < to_insert.size(); maintain_erased++) {
         const sort_key_t& sk = to_insert.at(maintain_erased);
         customer2_t::Key cust_key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey, ++last_customer_id_old};
         customer2.erase(cust_key);
         view_t::Key vk{cust_key};
         join_view.erase(vk);
         mixed_view_t::Key mixed_vk{sk};
         UpdateDescriptorGenerator1(mixed_view_decrementer, mixed_view_t, payloads);
         mixed_view.update1(mixed_vk, [](mixed_view_t& v) { std::get<4>(v.payloads).customer_count--; }, mixed_view_decrementer);
      }
      check_last_customer_id();
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
#include "join_search_count.tpp"  // IWYU pragma: keep
// #include "join_instance_count.tpp"  // IWYU pragma: keep
#include "load.tpp"         // IWYU pragma: keep
#include "maintain.tpp"     // IWYU pragma: keep
#include "mixed_query.tpp"  // IWYU pragma: keep