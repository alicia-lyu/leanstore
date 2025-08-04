#pragma once
#include <string>
#include "workload.hpp"

namespace geo_join
{

struct PerStructureWorkload {
   virtual ~PerStructureWorkload() = default;
   virtual std::string get_name() const = 0;
   virtual void join_ns() = 0;
   virtual void join_nsc() = 0;
   virtual void join_nscci() = 0;
   virtual void mixed_ns() = 0;
   virtual void mixed_nsc() = 0;
   virtual void mixed_nscci() = 0;
   virtual void insert1() = 0;
   virtual bool erase1() = 0;
   virtual void cleanup_updates() = 0;
   virtual double get_size() = 0;
   virtual bool insertion_complete() const = 0;
   virtual void bg_lookup() = 0;
   virtual int remaining_customers_to_erase() = 0;
   virtual void reset_maintain_ptrs() = 0;
   virtual void select_to_insert() = 0;
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct BaseWorkload : public PerStructureWorkload {
   GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload;
   BaseWorkload(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : workload(workload) {}

   std::string get_name() const override { return "base_idx"; }
   void join_ns() override { workload.join_ns_base(); };
   void join_nsc() override { workload.join_nsc_base(); };
   void join_nscci() override { workload.join_nscci_base(); };
   void mixed_ns() override { workload.mixed_ns_base(); };
   void mixed_nsc() override { workload.mixed_nsc_base(); };
   void mixed_nscci() override { workload.mixed_nscci_base(); };
   void insert1() override { workload.maintain_base(); };
   bool erase1() override { return workload.erase_base(); };
   void cleanup_updates() override { workload.cleanup_base(); }
   double get_size() override { return workload.get_indexes_size(); }
   bool insertion_complete() const override { return workload.maintenance_state.insertion_complete(); }
   void bg_lookup() override { workload.point_lookups_of_rest(); }
   int remaining_customers_to_erase() override { return workload.maintenance_state.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() override { workload.maintenance_state.reset(); }
   void select_to_insert() override { workload.select_to_insert(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct ViewWorkload : public PerStructureWorkload {
   GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload;
   ViewWorkload(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : workload(workload) {}
   std::string get_name() const override { return "mat_view"; }
   void join_ns() override { workload.join_ns_view(); };
   void join_nsc() override { workload.join_nsc_view(); };
   void join_nscci() override { workload.join_nscci_view(); };
   void mixed_ns() override { workload.mixed_ns_view(); };
   void mixed_nsc() override { workload.mixed_nsc_view(); };
   void mixed_nscci() override { workload.mixed_nscci_view(); };
   void insert1() override { workload.maintain_view(); };
   bool erase1() override { return workload.erase_view(); };
   void cleanup_updates() override { workload.cleanup_view(); }
   double get_size() override { return workload.get_view_size(); }
   bool insertion_complete() const override { return workload.maintenance_state.insertion_complete(); }
   void bg_lookup() override { workload.point_lookups_of_rest(); }
   int remaining_customers_to_erase() override { return workload.maintenance_state.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() override { workload.maintenance_state.reset(); }
   void select_to_insert() override { workload.select_to_insert(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct MergedWorkload : public PerStructureWorkload {
   GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload;
   MergedWorkload(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : workload(workload) {}
   std::string get_name() const override { return "merged_idx"; }
   void join_ns() override { workload.join_ns_merged(); };
   void join_nsc() override { workload.join_nsc_merged(); };
   void join_nscci() override { workload.join_nscci_merged(); };
   void mixed_ns() override { workload.mixed_ns_merged(); };
   void mixed_nsc() override { workload.mixed_nsc_merged(); };
   void mixed_nscci() override { workload.mixed_nscci_merged(); };
   void insert1() override { workload.maintain_merged(); };
   bool erase1() override { return workload.erase_merged(); };
   double get_size() override { return workload.get_merged_size(); }
   void cleanup_updates() override { workload.cleanup_merged(); }
   bool insertion_complete() const override { return workload.maintenance_state.insertion_complete(); }
   void bg_lookup() override { workload.point_lookups_of_rest(); }
   int remaining_customers_to_erase() override { return workload.maintenance_state.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() override { workload.maintenance_state.reset(); }
   void select_to_insert() override { workload.select_merged_to_insert(); }
};

}  // namespace geo_join