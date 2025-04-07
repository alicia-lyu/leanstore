#pragma once

// #include "Logger.hpp"
// #include "Merge.hpp"
#include <optional>
#include "../shared/Adapter.hpp"
#include "BasicGroupViews.hpp"
#include "Logger.hpp"
#include "TPCHWorkload.hpp"
#include "Tables.hpp"
#include "leanstore/KVInterface.hpp"
// #include "Tables.hpp"
// #include "BasicJoinViews.hpp"

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

namespace basic_group
{
template <template <typename> class AdapterType, class MergedAdapterType>
class BasicGroup
{
   using TPCH = TPCHWorkload<AdapterType, MergedAdapterType>;
   TPCH& workload;
   AdapterType<view_t>& view;
   AdapterType<count_partsupp_t>& count_partsupp;
   AdapterType<sum_supplycost_t>& sum_supplycost;
   AdapterType<partsupp_t>& partsupp;
   MergedAdapterType& mergedBasicGroup;

   Logger& logger;

  public:
   BasicGroup(TPCH& workload, MergedAdapterType& mbg, AdapterType<view_t>& v, AdapterType<partsupp_t>& ps)
       : workload(workload), mergedBasicGroup(mbg), view(v), partsupp(ps), logger(workload.logger)
   {
   }

   // point lookups

   // queries

   // maintenance
   void maintainTemplate(std::function<void(const partsupp_t::Key&, const partsupp_t&)> partsupp_insert_func,
                         std::function<void(const Integer, const leanstore::UpdateSameSizeInPlaceDescriptor)> count_increment_func,
                         std::function<void(const Integer, const Numeric, const leanstore::UpdateSameSizeInPlaceDescriptor)> sum_update_func,
                         std::string name)
   {
      std::cout << "BasicGroup::maintain " << name << std::endl;
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      // add one supplier (preexisting) for an existing part
      auto part_id = workload.getPartID();
      auto supplier_id = workload.getSupplierID();
      bool found = false;
      while (found == false) {
         partsupp.scan(
             partsupp_t::Key({part_id, supplier_id}),
             [&](const partsupp_t::Key& k, const partsupp_t&) {
                if (k.ps_partkey != part_id || k.ps_suppkey != supplier_id) {
                   found = true;
                } else {
                   supplier_id++;
                }
                return false;
             },
             []() {});
      }

      UpdateDescriptorGenerator1(countsupp_update_descriptor, count_partsupp_t, count);
      UpdateDescriptorGenerator1(sum_supplycost_update_descriptor, sum_supplycost_t, sum_supplycost);
      auto rec = partsupp_t::generateRandomRecord();
      partsupp_insert_func(partsupp_t::Key({part_id, supplier_id}), rec);
      count_increment_func(part_id, countsupp_update_descriptor);
      sum_update_func(part_id, rec.ps_supplycost, sum_supplycost_update_descriptor);
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "maintain-" + name);
   }

   void maintainIndex()
   {
      maintainTemplate([this](const partsupp_t::Key& k, const partsupp_t& v) { workload.partsupp.insert(k, v); },
                       [this](const Integer part_id, const leanstore::UpdateSameSizeInPlaceDescriptor update_descriptor) {
                          count_partsupp.update1(count_partsupp_t::Key({part_id}), [&](count_partsupp_t& rec) { rec.count++; }, update_descriptor);
                       },
                       [this](const Integer part_id, const Numeric supplycost, const leanstore::UpdateSameSizeInPlaceDescriptor update_descriptor) {
                          sum_supplycost.update1(
                              sum_supplycost_t::Key({part_id}), [&](sum_supplycost_t& rec) { rec.sum_supplycost += supplycost; }, update_descriptor);
                       },
                       "index");
   }

   void maintainMerged()
   {
      // Refer only to merged for partsupp. Partsupp is discarded.
      maintainTemplate([this](const partsupp_t::Key& k, const partsupp_t& v) { mergedBasicGroup.insert(k, v); },
                       [this](const Integer part_id, const leanstore::UpdateSameSizeInPlaceDescriptor update_descriptor) {
                          mergedBasicGroup.update1(count_partsupp_t::Key({part_id}), [&](count_partsupp_t& rec) { rec.count++; }, update_descriptor);
                       },
                       [this](const Integer part_id, const Numeric supplycost, const leanstore::UpdateSameSizeInPlaceDescriptor update_descriptor) {
                          mergedBasicGroup.update1(
                              sum_supplycost_t::Key({part_id}), [&](sum_supplycost_t& rec) { rec.sum_supplycost += supplycost; }, update_descriptor);
                       },
                       "merged");
   }

   void maintainView()
   {
      // partsupp is kept as a base table
      maintainTemplate([this](const partsupp_t::Key& k, const partsupp_t& v) { workload.partsupp.insert(k, v); },
                       [this](const Integer part_id, const leanstore::UpdateSameSizeInPlaceDescriptor update_descriptor) {
                          view.update1(view_t::Key({part_id}), [&](view_t& rec) { rec.count_partsupp++; }, update_descriptor);
                       },
                       [this](const Integer part_id, const Numeric supplycost, const leanstore::UpdateSameSizeInPlaceDescriptor update_descriptor) {
                          view.update1(view_t::Key({part_id}), [&](view_t& rec) { rec.sum_supplycost += supplycost; }, update_descriptor);
                       },
                       "view");
   }

   // loading
   void loadBaseTables() { workload.load(); }

   void loadAllOptions()
   {
      auto partsupp_scanner = workload.partsupp.getScanner();
      Integer count = 0;
      Numeric supplycost_sum = 0;
      Integer curr_partkey = 0;
      while (true) {
         auto kv = partsupp_scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         if (k.partkey == curr_partkey) {
            count++;
            supplycost_sum += v.ps_supplycost;
         } else {
            if (curr_partkey != 0) {
               view.insert(view_t::Key({curr_partkey}), view_t({count, supplycost_sum}));
               mergedBasicGroup.insert(count_partsupp_t::Key({curr_partkey}), count_partsupp_t({count}));
               mergedBasicGroup.insert(sum_supplycost_t::Key({curr_partkey}), sum_supplycost_t({supplycost_sum}));
               count_partsupp.insert(count_partsupp_t::Key({curr_partkey}), count_partsupp_t({count}));
               sum_supplycost.insert(sum_supplycost_t::Key({curr_partkey}), sum_supplycost_t({supplycost_sum}));
            }
            curr_partkey = k.partkey;
            count = 1;
            supplycost_sum = v.ps_supplycost;
         }
      }
   }
};
}  // namespace basic_group