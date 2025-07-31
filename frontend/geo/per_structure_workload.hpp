#pragma once
#include <string>
#include "workload.hpp"

namespace geo_join
{

struct PerStructureWorkload {
   virtual ~PerStructureWorkload() = default;
   virtual std::string get_name() const = 0;
   virtual void join() = 0;
   virtual void ns5join() = 0;
   virtual void nsc5join() = 0;
   virtual void nscci5join() = 0;
   virtual void mixed() = 0;
   virtual void insert1() = 0;
   virtual void mixed_point() = 0;
   virtual bool erase1() = 0;
   virtual void cleanup_updates() = 0;
   virtual double get_size() = 0;
   virtual bool insertion_complete() const = 0;
   virtual void bg_lookup() = 0;
   virtual int remaining_customers_to_erase() = 0;
   virtual void reset_maintain_ptrs() = 0;
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct BaseWorkload : public PerStructureWorkload {
   GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload;
   BaseWorkload(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : workload(workload) {}

   std::string get_name() const override { return "base_idx"; }
   void join() override { workload.query_by_base(); };
   void ns5join() override { workload.ns5join_base(); };
   void nsc5join() override { workload.nsc5join_base(); };
   void nscci5join() override { workload.nscci5join_base(); };
   void mixed() override { workload.mixed_query_by_base(); };
   void insert1() override { workload.maintain_base(); };
   void mixed_point() override { workload.point_mixed_query_by_base(); };
   bool erase1() override { return workload.erase_base(); };
   void cleanup_updates() override { workload.cleanup_base(); }
   double get_size() override { return workload.get_indexes_size(); }
   bool insertion_complete() const override { return workload.insertion_complete(); }
   void bg_lookup() override { workload.point_lookups_of_rest(); }
   int remaining_customers_to_erase() override { return workload.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() override { workload.reset_maintain_ptrs(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct ViewWorkload : public PerStructureWorkload {
   GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload;
   ViewWorkload(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : workload(workload) {}
   std::string get_name() const override { return "mat_view"; }
   void join() override { workload.query_by_view(); };
   void ns5join() override { workload.ns5join_view(); };
   void nsc5join() override { workload.nsc5join_view(); };
   void nscci5join() override { workload.nscci5join_view(); };
   void mixed() override { workload.mixed_query_by_view(); };
   void insert1() override { workload.maintain_view(); };
   void mixed_point() override { workload.point_mixed_query_by_view(); };
   bool erase1() override { return workload.erase_view(); };
   void cleanup_updates() override { workload.cleanup_view(); }
   double get_size() override { return workload.get_view_size(); }
   bool insertion_complete() const override { return workload.insertion_complete(); }
   void bg_lookup() override { workload.point_lookups_of_rest(); }
   int remaining_customers_to_erase() override { return workload.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() override { workload.reset_maintain_ptrs(); }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
struct MergedWorkload : public PerStructureWorkload {
   GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload;
   MergedWorkload(GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>& workload) : workload(workload) {}
   std::string get_name() const override { return "merged_idx"; }
   void join() override { workload.query_by_merged(); };
   void ns5join() override { workload.ns5join_merged(); };
   void nsc5join() override { workload.nsc5join_merged(); };
   void nscci5join() override { workload.nscci5join_merged(); };
   void mixed() override { workload.mixed_query_by_merged(); };
   void insert1() override { workload.maintain_merged(); };
   void mixed_point() override { workload.point_mixed_query_by_merged(); };
   bool erase1() override { return workload.erase_merged(); };
   double get_size() override { return workload.get_merged_size(); }
   void cleanup_updates() override { workload.cleanup_merged(); }
   bool insertion_complete() const override { return workload.insertion_complete(); }
   void bg_lookup() override { workload.point_lookups_of_rest(); }
   int remaining_customers_to_erase() override { return workload.remaining_customers_to_erase(); }
   void reset_maintain_ptrs() override { workload.reset_maintain_ptrs(); }
};

}  // namespace geo_join