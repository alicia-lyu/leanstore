#pragma once
#include <string>
#include "workload.hpp"

namespace geo_join
{
template <typename GeoJoinWrapperType>
struct PerStructureWorkload {
   GeoJoinWrapperType workload;
   const std::string name;
   template <typename GeoJoin>
   PerStructureWorkload(GeoJoin& workload, const std::string& name) : workload(workload), name(name) {}
   std::string get_name() const { return name; }
   void join_n()
   {
      long produced = workload.join(workload.next_nation_to_scan(), 0, 0, 0);
      workload.new_n_join(produced);
   }
   void join_ns()
   {
      long produced = workload.join(workload.get_nationkey(), workload.get_statekey(), 0, 0);
      workload.new_ns_join(produced);
   }
   void join_nsc()
   {
      long produced = workload.join(workload.get_nationkey(), workload.get_statekey(), workload.get_countykey(), 0);
      workload.new_nsc_join(produced);
   }
   void mixed_n()
   {
      long cust_sum = workload.mixed(workload.next_nation_to_scan(), 0, 0, 0);
      workload.new_n_mixed(cust_sum);
   }
   void mixed_ns()
   {
      long cust_sum = workload.mixed(workload.get_nationkey(), workload.get_statekey(), 0, 0);
      workload.new_ns_mixed(cust_sum);
   }
   void mixed_nsc()
   {
      long cust_sum = workload.mixed(workload.get_nationkey(), workload.get_statekey(), workload.get_countykey(), 0);
      workload.new_nsc_mixed(cust_sum);
   }

   void distinct_n()
   {
      long distinct = workload.distinct(workload.next_nation_to_scan(), 0, 0, 0);
      workload.new_n_distinct(distinct);
   }
   void distinct_ns()
   {
      long distinct = workload.distinct(workload.get_nationkey(), workload.get_statekey(), 0, 0);
      workload.new_ns_distinct(distinct);
   }
   void distinct_nsc()
   {
      long distinct = workload.distinct(workload.get_nationkey(), workload.get_statekey(), workload.get_countykey(), 0);
      workload.new_nsc_distinct(distinct);
   }
   void insert1() { workload.insert1(); }
   bool erase1() { return workload.erase1(); }
   void cleanup_updates() { workload.cleanup_updates(); }
   double get_size() { return workload.get_size(); }
   bool insertion_complete() { return workload.insertion_complete(); }
   void bg_lookup() { workload.bg_lookup(); }
   int remaining_customers_to_erase() { return workload.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() { workload.reset_maintain_ptrs(); }
   void select_to_insert() { workload.select_to_insert(); }
   bool n_scan_finished() const { return workload.n_scan_finished(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct GeoJoinWrapper {
   GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload;

   GeoJoinWrapper(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : workload(workload) {}

   virtual ~GeoJoinWrapper() = default;

   Integer get_nationkey() { return workload.params.get_nationkey(); }
   Integer get_statekey() { return workload.params.get_statekey(); }
   Integer get_countykey() { return workload.params.get_countykey(); }
   Integer get_citykey() { return workload.params.get_citykey(); }
   Integer next_nation_to_scan() { return workload.get_n().first; }

   void new_n_join(long produced) { workload.stats.new_n_join(produced); };
   void new_ns_join(long produced) { workload.stats.new_ns_join(produced); };
   void new_nsc_join(long produced) { workload.stats.new_nsc_join(produced); };

   void new_n_mixed(long cust_sum) { workload.stats.new_n_mixed(cust_sum); };
   void new_ns_mixed(long cust_sum) { workload.stats.new_ns_mixed(cust_sum); };
   void new_nsc_mixed(long cust_sum) { workload.stats.new_nsc_mixed(cust_sum); };

   void new_n_distinct(long distinct) { workload.stats.new_n_distinct(distinct); };
   void new_ns_distinct(long distinct) { workload.stats.new_ns_distinct(distinct); };
   void new_nsc_distinct(long distinct) { workload.stats.new_nsc_distinct(distinct); };

   bool insertion_complete() { return workload.maintenance_state.insertion_complete(); }
   void bg_lookup() { workload.point_lookups_of_rest(); }
   int remaining_customers_to_erase() { return workload.maintenance_state.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() { workload.maintenance_state.reset(); }
   void select_to_insert() { workload.select_to_insert(); }
   bool n_scan_finished() const { return workload.get_n(true).second; }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct BaseGeoJoin : public GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType> {
   using GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::workload;

   BaseGeoJoin(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>(workload) {}

   long join(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_query_by_base(nationkey, statekey, countykey, citykey);
   }

   long mixed(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_by_base(sort_key_t{nationkey, statekey, countykey, citykey, 0}, false);
   }

   long distinct(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_by_base(sort_key_t{nationkey, statekey, countykey, citykey, 0}, true);
   }

   void insert1() { workload.maintain_base(); }

   bool erase1() { return workload.erase_base(); }
   void cleanup_updates() { workload.cleanup_base(); }
   double get_size() { return workload.get_indexes_size(); }

   void select_to_insert() { workload.select_to_insert(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct ViewGeoJoin : public GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType> {
   using GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::workload;

   ViewGeoJoin(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>(workload) {}

   long join(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_query_by_view(nationkey, statekey, countykey, citykey);
   }

   long mixed(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_by_view(sort_key_t{nationkey, statekey, countykey, citykey, 0}, false);
   }

   long distinct(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_by_view(sort_key_t{nationkey, statekey, countykey, citykey, 0}, true);
   }

   void insert1() { workload.maintain_view(); }

   bool erase1() { return workload.erase_view(); }
   void cleanup_updates() { workload.cleanup_view(); }
   double get_size() { return workload.get_view_size(); }

   void select_to_insert() { workload.select_to_insert(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct MergedGeoJoin : public GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType> {
   using GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::workload;
   MergedGeoJoin(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>(workload) {}

   long join(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_query_by_merged(nationkey, statekey, countykey, citykey);
   }
   long mixed(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_by_merged(sort_key_t{nationkey, statekey, countykey, citykey, 0}, false);
   }
   long distinct(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_by_merged(sort_key_t{nationkey, statekey, countykey, citykey, 0}, true);
   }
   void insert1() { workload.maintain_merged(); }
   bool erase1() { return workload.erase_merged(); }
   void cleanup_updates() { workload.cleanup_merged(); }
   double get_size() { return workload.get_merged_size(); }
   void select_to_insert() { workload.select_merged_to_insert(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct HashGeoJoin : public GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType> {
   using GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::workload;

   HashGeoJoin(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : GeoJoinWrapper<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>(workload) {}

   long join(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_query_hash(nationkey, statekey, countykey, citykey);
   }

   long mixed(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_hash(sort_key_t{nationkey, statekey, countykey, citykey, 0}, false);
   }

   long distinct(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
   {
      return workload.range_mixed_query_hash(sort_key_t{nationkey, statekey, countykey, citykey, 0}, true);
   }

   void insert1() { workload.maintain_base(); }
   bool erase1() { return workload.erase_base(); }
   void cleanup_updates() { workload.cleanup_base(); }
   double get_size() { return workload.get_indexes_size(); }
   void select_to_insert() { workload.select_to_insert(); }
};

}  // namespace geo_join