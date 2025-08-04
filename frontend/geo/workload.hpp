#pragma once

#include <cstddef>
#include "../shared/adapter-scanner/Adapter.hpp"
#include "../tpc-h/workload.hpp"
#include "leanstore/Config.hpp"
#include "load.hpp"
#include "views.hpp"

namespace geo_join
{

template <typename E>
inline void scan_urand_next(std::vector<E>& container, std::function<E()> next_element)
{
   size_t i = container.size();
   if (i < container.capacity()) {  // First, fill city_reservoir with the first 1% of cities
      container.push_back(next_element());
   } else {  // Then, for each subsequent key i, replace a random element in the reservoir with the new key with probability k/i.
      size_t j = rand() % (i + 1);
      if (j < container.capacity()) {
         container.at(j) = next_element();
      }
   }
   if (i % 100 == 1 && FLAGS_log_progress) {
      std::cout << "\rScanned " << i + 1 << " cities..." << std::flush;
   }
};

struct MaintenanceState {
   std::vector<sort_key_t> city_reservoir;  // reservoir sampling for cities
   int& inserted_last_id_ref;
   int erased_last_id;
   size_t city_count;
   size_t processed_idx = 0;
   size_t erased_idx = 0;

   MaintenanceState(int& inserted_last_id_ref, size_t city_count)
       : inserted_last_id_ref(inserted_last_id_ref), erased_last_id(inserted_last_id_ref), city_count(city_count)
   {
      city_reservoir.reserve(city_count);
   }

   MaintenanceState(int& inserted_last_id_ref) : inserted_last_id_ref(inserted_last_id_ref), erased_last_id(inserted_last_id_ref), city_count(0) {}

   void adjust_ptrs();

   void reset_cities(size_t city_count)
   {
      city_reservoir.clear();
      if (this->city_count == city_count) {
         return;  // already reserved
      }
      city_reservoir.reserve(city_count);
      this->city_count = city_count;
   }

   void reset()
   {
      city_reservoir.clear();
      processed_idx = 0;
      erased_idx = 0;
      erased_last_id = inserted_last_id_ref;
   }
   bool insertion_complete() const { return processed_idx >= city_reservoir.size(); }
   size_t remaining_customers_to_erase() const { return inserted_last_id_ref - erased_last_id; }
   bool customer_to_erase() const { return remaining_customers_to_erase() > 0; }

   void select(const sort_key_t& sk);

   sort_key_t next_cust_to_erase();

   sort_key_t next_cust_to_insert();

   void cleanup(std::function<void(const sort_key_t&)> erase_func);
};

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
         logger(workload.logger),
         maintenance_state(workload.last_customer_id)
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
   void join_ns_view()
   {
      ns_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }
   void join_ns_merged()
   {
      ns_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }
   void join_ns_base()
   {
      ns_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }

   void join_ns_2merged()
   {
      ns_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), 0, 0);
      ns_count++;
   }

   // Find all joined rows for the same nationkey, statekey, countykey
   void join_nsc_view()
   {
      nsc_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }
   void join_nsc_merged()
   {
      nsc_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }
   void join_nsc_base()
   {
      nsc_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }

   void join_nsc_2merged()
   {
      nsc_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      nsc_count++;
   }

   void join_nscci_view()
   {
      nscci_sum += range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void join_nscci_merged()
   {
      nscci_sum += range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void join_nscci_base()
   {
      nscci_sum += range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   void join_nscci_2merged()
   {
      nscci_sum += range_query_by_2merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      nscci_count++;
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------
   MaintenanceState maintenance_state;

   void select_merged_to_insert();

   void select_to_insert();

   void maintain_base();

   void maintain_merged();

   void maintain_view();

   void maintain_2merged() { throw std::runtime_error("maintain_2merged not implemented"); }

   bool erase_base();

   bool erase_merged();

   bool erase_view();

   bool erase_2merged() { throw std::runtime_error("erase_2merged not implemented"); }

   void cleanup_base()
   {
      maintenance_state.cleanup([this](const customer2_t::Key& cust_key) { customer2.erase(cust_key); });
   }

   void cleanup_merged()
   {
      maintenance_state.cleanup([this](const customer2_t::Key& cust_key) { merged.template erase<customer2_t>(cust_key); });
   }

   void cleanup_view()
   {
      maintenance_state.cleanup([this](const customer2_t::Key& cust_key) {
         view_t::Key vk{cust_key};
         bool ret_jv = join_view.erase(vk);
         bool ret_c = customer2.erase(cust_key);
         if (!ret_jv || !ret_c) {
            throw std::runtime_error("Error erasing customer in view, ret_jv: " + std::to_string(ret_jv) + ", ret_c: " + std::to_string(ret_c));
         }
         mixed_view_t::Key mixed_vk{cust_key};
         UpdateDescriptorGenerator1(mixed_view_decrementer, mixed_view_t, payloads);
         mixed_view.update1(mixed_vk, [](mixed_view_t& v) { std::get<4>(v.payloads).customer_count--; }, mixed_view_decrementer);
      });
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

   long long ns_cust_sum = 0;
   long long ns_mixed_count = 0;
   long long nsc_cust_sum = 0;
   long long nsc_mixed_count = 0;
   long long nscci_cust_sum = 0;
   long long nscci_mixed_count = 0;

   long range_mixed_query_by_view(sort_key_t select_sk);
   long range_mixed_query_by_merged(sort_key_t select_sk);
   long range_mixed_query_by_base(sort_key_t select_sk);

   void mixed_ns_base()
   {
      ns_cust_sum += range_mixed_query_by_base(sort_key_t{params.get_nationkey(), params.get_statekey(), 0, 0, 0});
      ns_mixed_count++;
   }

   void mixed_nsc_base()
   {
      nsc_cust_sum += range_mixed_query_by_base(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0, 0});
      nsc_mixed_count++;
   }

   void mixed_nscci_base()
   {
      nscci_cust_sum += range_mixed_query_by_base(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
      nscci_mixed_count++;
   }

   void mixed_ns_view()
   {
      ns_cust_sum += range_mixed_query_by_view(sort_key_t{params.get_nationkey(), params.get_statekey(), 0, 0, 0});
      ns_mixed_count++;
   }

   void mixed_nsc_view()
   {
      nsc_cust_sum += range_mixed_query_by_view(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0, 0});
      nsc_mixed_count++;
   }

   void mixed_nscci_view()
   {
      nscci_cust_sum += range_mixed_query_by_view(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
      nscci_mixed_count++;
   }

   void mixed_ns_merged()
   {
      ns_cust_sum += range_mixed_query_by_merged(sort_key_t{params.get_nationkey(), params.get_statekey(), 0, 0, 0});
      ns_mixed_count++;
   }

   void mixed_nsc_merged()
   {
      nsc_cust_sum += range_mixed_query_by_merged(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0, 0});
      nsc_mixed_count++;
   }

   void mixed_nscci_merged()
   {
      nscci_cust_sum += range_mixed_query_by_merged(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
      nscci_mixed_count++;
   }
};
}  // namespace geo_join
// #include "groupby_query.tpp"  // IWYU pragma: keep
#include "join_search_count.tpp"  // IWYU pragma: keep
// #include "join_instance_count.tpp"  // IWYU pragma: keep
#include "load.tpp"         // IWYU pragma: keep
#include "maintain.tpp"     // IWYU pragma: keep
#include "mixed_query.tpp"  // IWYU pragma: keep