#pragma once

#include "../shared/adapter-scanner/Adapter.hpp"
#include "load.hpp"
#include "tpch_workload.hpp"
#include "views.hpp"
#include "workload_helpers.hpp"

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

   AdapterType<nation2_t>& nation;
   AdapterType<states_t>& states;
   AdapterType<county_t>& county;
   AdapterType<city_t>& city;
   AdapterType<customer2_t>& customer2;

   Params params;

   WorkloadStats stats;

   Logger& logger;

   std::vector<sort_key_t> to_insert;

  public:
   GeoJoin(TPCH& workload,
           MergedTree& m,
           AdapterType<mixed_view_t>& mixed_view,
           AdapterType<view_t>& v,
           AdapterType<nation2_t>& n,
           AdapterType<states_t>& s,
           AdapterType<county_t>& c,
           AdapterType<city_t>& ci,
           AdapterType<customer2_t>& customer2)
       : workload(workload),
         merged(m),
         mixed_view(mixed_view),
         join_view(v),
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

   ~GeoJoin() = default;

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

   // ------------------------ JOIN QUERIES -----------------------------

   long range_query_by_view(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);
   long range_query_by_merged(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);
   long range_query_by_base(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);
   long range_query_hash(Integer nationkey, Integer statekey, Integer countykey, Integer citykey);

   std::pair<int, bool> get_n(bool info_only = false) const
   {
      static std::vector<int> nation_keys;
      if (nation_keys.empty()) {
         nation_keys.resize(params.nation_count);
         std::iota(nation_keys.begin(), nation_keys.end(), 1);
         std::shuffle(nation_keys.begin(), nation_keys.end(), std::mt19937{std::random_device{}()});
      }
      static size_t n_i = 0;

      if (info_only) {
         return std::make_pair(0, n_i == nation_keys.size());
      }

      int lottery = std::rand() % 2;
      int n;
      if (lottery == 0) {
         n_i %= nation_keys.size();
         n = nation_keys.at(n_i); 
         n_i++; // can increment to nation_keys.size()
      } else {
         n = 1;  // hot nation
      }
      std::cout << "get_n(): returning nationkey = " << n << ", n_i = " << n_i << std::endl;
      return std::make_pair(n, n_i == nation_keys.size());
   }

   void join_n_hash()
   {
      long produced = range_query_hash(get_n().first, 0, 0, 0);
      stats.new_n_join(produced);
   }
   void join_n_view()
   {
      long produced = range_query_by_view(get_n().first, 0, 0, 0);
      stats.new_n_join(produced);
   }
   void join_n_merged()
   {
      long produced = range_query_by_merged(get_n().first, 0, 0, 0);
      stats.new_n_join(produced);
   }

   void join_n_base()
   {
      long produced = range_query_by_base(get_n().first, 0, 0, 0);
      stats.new_n_join(produced);
   }

   void join_ns_hash()
   {
      long produced = range_query_hash(params.get_nationkey(), params.get_statekey(), 0, 0);
      stats.new_ns_join(produced);
   }

   void join_nsc_hash()
   {
      long produced = range_query_hash(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      stats.new_nsc_join(produced);
   }

   void join_nscci_hash()
   {
      long produced = range_query_hash(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey());
      stats.new_nscci_join(produced);
   }

   // Find all joined rows for the same nationkey, statekey
   void join_ns_view()
   {
      long produced = range_query_by_view(params.get_nationkey(), params.get_statekey(), 0, 0);
      stats.new_ns_join(produced);
   }
   void join_ns_merged()
   {
      long produced = range_query_by_merged(params.get_nationkey(), params.get_statekey(), 0, 0);
      stats.new_ns_join(produced);
   }
   void join_ns_base()
   {
      long produced = range_query_by_base(params.get_nationkey(), params.get_statekey(), 0, 0);
      stats.new_ns_join(produced);
   }

   // Find all joined rows for the same nationkey, statekey, countykey
   void join_nsc_view()
   {
      long produced = range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      stats.new_nsc_join(produced);
   }
   void join_nsc_merged()
   {
      long produced = range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      stats.new_nsc_join(produced);
   }
   void join_nsc_base()
   {
      long produced = range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0);
      stats.new_nsc_join(produced);
   }

   // Find all joined rows for the same nationkey, statekey, countykey, citykey
   void join_nscci_view()
   {
      stats.new_nscci_join(range_query_by_view(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey()));
   }

   void join_nscci_merged()
   {
      stats.new_nscci_join(range_query_by_merged(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey()));
   }

   void join_nscci_base()
   {
      stats.new_nscci_join(range_query_by_base(params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey()));
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTAIN -----------------------------
   MaintenanceState maintenance_state;

   void select_merged_to_insert();

   void select_to_insert();  // select from city table

   void maintain_base();
   void maintain_merged();
   void maintain_view();

   bool erase_base();
   bool erase_merged();
   bool erase_view();

   void cleanup_base()
   {
      maintenance_state.cleanup([this](const sort_key_t& sk) { customer2.erase(customer2_t::Key{sk}); });
   }
   void cleanup_merged()
   {
      maintenance_state.cleanup([this](const sort_key_t& sk) { merged.template erase<customer2_t>(customer2_t::Key{sk}); });
   }
   void cleanup_view()
   {
      maintenance_state.cleanup([this](const sort_key_t& sk) {
         bool ret_jv = join_view.erase(view_t::Key{sk});
         bool ret_c = customer2.erase(customer2_t::Key{sk});
         if (!ret_jv || !ret_c) {
            std::stringstream ss;
            ss << "Error erasing customer in view, ret_jv: " << ret_jv << ", ret_c: " << ret_c << ", sk: " << sk;
            throw std::runtime_error(ss.str());
         }
         mixed_view_t::Key mixed_vk{sk};
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
   void load_1customer(int n, int s, int c, int ci, int cu);

   double get_view_size()
   {
      static auto mixed_view_size = mixed_view.size();  // local static: initialized once
      static auto indexes_size = get_indexes_size();
      static auto join_view_size = join_view.size();
      static auto view_size = mixed_view_size + indexes_size + join_view_size;
      return view_size;
   }

   double get_indexes_size()
   {
      static auto indexes_size = nation.size() + states.size() + county.size() + city.size() + customer2.size();
      return indexes_size;
   }

   double get_merged_size()
   {
      static auto merged_size = merged.size();
      return merged_size;
   }

   void log_sizes();

   // -------------------------------------------------------------
   // ---------------------- JOIN + GROUP-BY ----------------------

   long range_mixed_query_by_view(sort_key_t select_sk);
   long range_mixed_query_by_merged(sort_key_t select_sk);
   long range_mixed_query_by_base(sort_key_t select_sk);
   long range_mixed_query_hash(sort_key_t select_sk);

   void mixed_n_hash()
   {
      long cust_sum = range_mixed_query_hash(sort_key_t{get_n().first, 0, 0, 0, 0});
      stats.new_n_mixed(cust_sum);
   }

   void mixed_n_view()
   {
      long cust_sum = range_mixed_query_by_view(sort_key_t{get_n().first, 0, 0, 0, 0});
      stats.new_n_mixed(cust_sum);
   }

   void mixed_n_merged()
   {
      long cust_sum = range_mixed_query_by_merged(sort_key_t{get_n().first, 0, 0, 0, 0});
      stats.new_n_mixed(cust_sum);
   }

   void mixed_n_base()
   {
      long cust_sum = range_mixed_query_by_base(sort_key_t{get_n().first, 0, 0, 0, 0});
      stats.new_n_mixed(cust_sum);
   }

   void mixed_ns_hash()
   {
      long cust_sum = range_mixed_query_hash(sort_key_t{params.get_nationkey(), params.get_statekey(), 0, 0, 0});
      stats.new_ns_mixed(cust_sum);
   }

   void mixed_nsc_hash()
   {
      long cust_sum = range_mixed_query_hash(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0, 0});
      stats.new_nsc_mixed(cust_sum);
   }

   void mixed_nscci_hash()
   {
      long cust_sum =
          range_mixed_query_hash(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
      stats.new_nscci_mixed(cust_sum);
   }

   void mixed_ns_base()
   {
      long cust_sum = range_mixed_query_by_base(sort_key_t{params.get_nationkey(), params.get_statekey(), 0, 0, 0});
      stats.new_ns_mixed(cust_sum);
   }

   void mixed_nsc_base()
   {
      long cust_sum = range_mixed_query_by_base(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0, 0});
      stats.new_nsc_mixed(cust_sum);
   }

   void mixed_nscci_base()
   {
      long cust_sum =
          range_mixed_query_by_base(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
      stats.new_nscci_mixed(cust_sum);
   }

   void mixed_ns_view()
   {
      long cust_sum = range_mixed_query_by_view(sort_key_t{params.get_nationkey(), params.get_statekey(), 0, 0, 0});
      stats.new_ns_mixed(cust_sum);
   }

   void mixed_nsc_view()
   {
      long cust_sum = range_mixed_query_by_view(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0, 0});
      stats.new_nsc_mixed(cust_sum);
   }

   void mixed_nscci_view()
   {
      long cust_sum =
          range_mixed_query_by_view(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
      stats.new_nscci_mixed(cust_sum);
   }

   void mixed_ns_merged()
   {
      long cust_sum = range_mixed_query_by_merged(sort_key_t{params.get_nationkey(), params.get_statekey(), 0, 0, 0});
      stats.new_ns_mixed(cust_sum);
   }

   void mixed_nsc_merged()
   {
      long cust_sum = range_mixed_query_by_merged(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), 0, 0});
      stats.new_nsc_mixed(cust_sum);
   }

   void mixed_nscci_merged()
   {
      long cust_sum =
          range_mixed_query_by_merged(sort_key_t{params.get_nationkey(), params.get_statekey(), params.get_countykey(), params.get_citykey(), 0});
      stats.new_nscci_mixed(cust_sum);
   }
};
}  // namespace geo_join
// #include "groupby_query.tpp"  // IWYU pragma: keep
#include "join_search_count.tpp"  // IWYU pragma: keep
// #include "join_instance_count.tpp"  // IWYU pragma: keep
#include "load.tpp"         // IWYU pragma: keep
#include "maintain.tpp"     // IWYU pragma: keep
#include "mixed_query.tpp"  // IWYU pragma: keep