#pragma once

#include "../shared/logger/logger.hpp"
#include "load.hpp"
#include "views.hpp"
#include "workload_helpers.hpp"

namespace geo_join
{

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct GeoJoinWrapper;  // forward declaration

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
class GeoJoin
{
   friend struct GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>;
   using MergedTree = MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>;

   // Effective customer count for this run. Driven by --geo_scale_factor at
   // construction time; also tracked as the running "last customer id"
   // counter consumed by the maintenance path. Replaces the former
   // TPCHWorkload::last_customer_id dependency.
   Integer last_customer_id;

   MergedTree& merged;

   AdapterType<nscci_t>& geo_view;
   AdapterType<customer_count_t>& cust_count_view;

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
   GeoJoin(Logger& logger,
           MergedTree& m,
           AdapterType<nscci_t>& geo_view,
           AdapterType<customer_count_t>& cust_count_view,
           AdapterType<view_t>& v,
           AdapterType<nation2_t>& n,
           AdapterType<states_t>& s,
           AdapterType<county_t>& c,
           AdapterType<city_t>& ci,
           AdapterType<customer2_t>& customer2)
       : last_customer_id(0),
         merged(m),
         geo_view(geo_view),
         cust_count_view(cust_count_view),
         join_view(v),
         nation(n),
         states(s),
         county(c),
         city(ci),
         customer2(customer2),
         logger(logger),
         maintenance_state(last_customer_id)
   {
      if (FLAGS_geo_scale_factor > 1000) {
         throw std::runtime_error("GeoJoin does not support scale factor larger than 1000");
      }
      // Customer count formula reproduces the pre-decouple effective number:
      //   TPCH::CUSTOMER_SCALE (150K/SF) * SF * 200 (legacy multiplier) == 30000 * SF.
      // At --geo_scale_factor=15 (paper sweep default), this yields 450K customers.
      last_customer_id = 30000 * FLAGS_geo_scale_factor;
   }

   ~GeoJoin() = default;

   // Resets last_customer_id after recovery from a persisted image (so the
   // maintenance path knows the upper bound of pre-loaded custkeys). Mirrors
   // the prior TPCHWorkload::recover_last_ids() role in geo's lifecycle.
   void recover_last_customer_id()
   {
      Integer max_id = 0;
      customer2.scanDesc(
          customer2_t::Key{std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(),
                            std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()},
          [&](const customer2_t::Key& k, const customer2_t&) {
             max_id = std::max(max_id, k.custkey);
             return false;
          },
          []() {});
      last_customer_id = max_id;
      std::cout << "Recovered last_customer_id = " << last_customer_id << std::endl;
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
         auto ret = std::make_pair(0, n_i == nation_keys.size());
         // std::cout << "get_n(info_only): returning n_i = " << n_i << std::endl;
         n_i %= nation_keys.size();
         return ret;
      }

      int lottery = std::rand() % 2;
      int n;
      if (lottery == 0) {
         n_i %= nation_keys.size();
         n = nation_keys.at(n_i);
         n_i++;  // can increment to nation_keys.size()
      } else {
         n = 1;  // hot nation
      }
      // std::cout << "get_n(): returning nationkey = " << n << ", n_i = " << n_i << std::endl;
      return std::make_pair(n, n_i == nation_keys.size());
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
         customer_count_t::Key cuc_k{sk};
         cust_count_view.update1(cuc_k, [](customer_count_t& v) { v.customer_count--; });
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
      // static auto mixed_view_size = mixed_view.size();  // local static: initialized once
      static auto geo_view_size = geo_view.size();
      static auto cust_count_view_size = cust_count_view.size();
      static auto indexes_size = get_indexes_size();
      static auto join_view_size = join_view.size();
      static auto view_size = geo_view_size + cust_count_view_size + indexes_size + join_view_size;
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
   long range_mixed_query_by_view(sort_key_t select_sk, bool distinct);
   long range_mixed_query_by_merged(sort_key_t select_sk, bool distinct);
   long range_mixed_query_by_base(sort_key_t select_sk, bool distinct);
   long range_mixed_query_hash(sort_key_t select_sk, bool distinct);

   // -------------------------------------------------------------
   // ---------------------- BG POINT-LOOKUP ----------------------
   // Read-only hierarchical lookup: customer -> city -> county -> state -> nation.
   // All 5 lookups share one TX (caller wraps in run_tx). Parent keys derive
   // from the customer key prefix, no inter-lookup data dependency.
   void point_lookup_hierarchy_base(const customer2_t::Key& ck);
   void point_lookup_hierarchy_merged(const customer2_t::Key& ck);

   // Reservoir-sample N valid customer2 keys for bg-thread random selection.
   // Called once at bg-thread startup; subsequent lookups pick uniformly from
   // the returned vector. _base scans the customer2 adapter; _merged filters
   // the merged scanner for customer2 records via WILDCARD_KEY check.
   std::vector<customer2_t::Key> sample_customer_keys_base(size_t N);
   std::vector<customer2_t::Key> sample_customer_keys_merged(size_t N);
};
}  // namespace geo_join
// #include "groupby_query.tpp"  // IWYU pragma: keep
#include "join_search_count.tpp"  // IWYU pragma: keep
// #include "join_instance_count.tpp"  // IWYU pragma: keep
#include "load.tpp"         // IWYU pragma: keep
#include "maintain.tpp"     // IWYU pragma: keep
#include "mixed_query.tpp"  // IWYU pragma: keep
#include "point_lookup.tpp" // IWYU pragma: keep
#include "geo_walk.tpp"     // IWYU pragma: keep