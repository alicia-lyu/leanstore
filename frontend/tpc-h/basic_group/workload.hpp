#pragma once

#include <chrono>
#include <csignal>
#include <optional>
#include <stdexcept>
#include <variant>
#include "../../shared/Adapter.hpp"
#include "../logger.hpp"
#include "../tables.hpp"
#include "../tpch_workload.hpp"
#include "Exceptions.hpp"
#include "leanstore/KVInterface.hpp"
#include "views.hpp"

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

namespace basic_group
{
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          typename merged_view_option_t,
          typename merged_partsupp_option_t>
class BasicGroup
{
   using TPCH = TPCHWorkload<AdapterType>;
   TPCH& workload;
   using merged_t = MergedAdapterType<merged_view_option_t, merged_partsupp_option_t>;
   using merged_k_variant_t = std::variant<typename merged_view_option_t::Key, typename merged_partsupp_option_t::Key>;
   using merged_v_variant_t = std::variant<merged_view_option_t, merged_partsupp_option_t>;

   AdapterType<view_t>& view;
   AdapterType<partsupp_t>& partsupp;
   merged_t& mergedBasicGroup;

   Logger& logger;

  public:
   BasicGroup(TPCH& workload, merged_t& mbg, AdapterType<view_t>& v)
       : workload(workload), view(v), partsupp(workload.partsupp), mergedBasicGroup(mbg), logger(workload.logger)
   {
   }
   // -------------------------------------------------------------
   // ---------------------- POINT LOOKUPS ------------------------
   // point lookups: one on partsupp, one on view, one per other base tables
   void pointLookupsTemplate(std::function<void(Integer&, Integer&)> lookup_partsupp_find_valid_key,
                             std::function<void(const Integer)> lookup_aggregates)
   {
      // std::cout << "BasicGroup::pointLookupsTemplate for " << name << std::endl;
      Integer part_id = workload.getPartID();
      Integer supplier_id = workload.getSupplierID();
      lookup_partsupp_find_valid_key(part_id, supplier_id);
      lookup_aggregates(part_id);

      workload.part.lookup1(part_t::Key{part_id}, [&](const part_t&) {});
      workload.supplier.lookup1(supplier_t::Key{supplier_id}, [&](const supplier_t&) {});

      Integer customer_id = workload.getCustomerID();
      Integer order_id = workload.getOrderID();
      Integer nation_id = workload.getNationID();
      Integer region_id = workload.getRegionID();
      workload.customer.lookup1(customerh_t::Key{customer_id}, [&](const customerh_t&) {});
      workload.orders.lookup1(orders_t::Key{order_id}, [&](const orders_t&) {});
      workload.lineitem.lookup1(lineitem_t::Key{order_id, 1}, [&](const lineitem_t&) {});
      workload.nation.tryLookup(nation_t::Key{nation_id}, [&](const nation_t&) {});
      workload.region.lookup1(region_t::Key{region_id}, [&](const region_t&) {});
   }

   void pointLookupsForMerged()
   {
      pointLookupsTemplate(
          [this](Integer& part_id, Integer& supplier_id) {
             auto partsupp_scanner = mergedBasicGroup.getScanner();
             auto ret = partsupp_scanner->template seekTyped<merged_partsupp_option_t>(
                 typename merged_partsupp_option_t::Key(partsupp_t::Key{part_id, supplier_id}));
             assert(ret);
             auto kv = partsupp_scanner->current();
             auto& [k, v] = *kv;
             std::visit(overloaded{[&](const typename merged_partsupp_option_t::Key& actual_key) {
                                      part_id = actual_key.pk.ps_partkey;
                                      supplier_id = actual_key.pk.ps_suppkey;
                                   },
                                   [&](const typename merged_view_option_t::Key&) { UNREACHABLE(); }},
                        k);
          },
          [this](const Integer part_id) {
             mergedBasicGroup.template lookup1<merged_view_option_t>(typename merged_view_option_t::Key(part_id),
                                                                     [&](const merged_view_option_t&) {});
          });
   }

   void pointLookupsForView()
   {
      pointLookupsTemplate(
          [this](Integer& part_id, Integer& supplier_id) {
             auto partsupp_scanner = workload.partsupp.getScanner();
             partsupp_scanner->seek(partsupp_t::Key{part_id, supplier_id});
             auto kv = partsupp_scanner->current();
             assert(kv.has_value());
             auto& [k, v] = *kv;
             part_id = k.ps_partkey;
             supplier_id = k.ps_suppkey;
          },
          [this](const Integer part_id) {
             view.lookup1(view_t::Key({part_id}),
                          [&](const view_t& agg) { [[maybe_unused]] double avg_supplycost = agg.sum_supplycost / agg.count_partsupp; });
          });
   }

   // -----------------------------------------------------------
   // ---------------------- QUERIES ----------------------------
   // Enumerate all aggregates of all parts

   void queryByView()
   {
      // enumerate view
      logger.reset();
      std::cout << "BasicGroup::queryByView()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      view.scan(
          {},
          [&](const auto&, const auto&) {
             TPCH::inspect_produced("Enumerating materialized view: ", produced);
             return true;
          },
          [&]() {});
      std::cout << "\rEnumerating materialized view: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, "query", "view", get_view_size());
   }

   void queryByMerged()
   {
      logger.reset();
      std::cout << "BasicGroup::queryByMerged()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      auto scanner = mergedBasicGroup.getScanner();
      auto partkey = 1;
      auto start_key = typename merged_view_option_t::Key(partkey);
      if (std::is_same_v<decltype(start_key.jk), sort_key_variant_t>)
         scanner->template seekForPrev<merged_view_option_t>(start_key);
      while (true) {
         std::optional<std::pair<merged_k_variant_t, merged_v_variant_t>> kv;
         if (std::is_same_v<decltype(start_key.jk), sort_key_variant_t>)
            kv = scanner->next();
         else {
            bool ret = scanner->template seekTyped<merged_view_option_t>(typename merged_view_option_t::Key(++partkey));
            if (!ret)
               break;
            kv = scanner->current();
         }

         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         std::visit(overloaded{[](const merged_partsupp_option_t&) { UNREACHABLE(); },
                               [&](const merged_view_option_t& aggregates) {
                                  [[maybe_unused]] double avg_supplycost = aggregates.payload.sum_supplycost / aggregates.payload.count_partsupp;
                               }},
                    v);
         TPCH::inspect_produced("Enumerating merged: ", produced);
      }
      std::cout << "\rEnumerating merged: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, "query", "merged", get_merged_size());
   }

   // ----------------------------------------------------------------------------------
   // ---------------------- QUERIES w EXTERNAL SELECT CONDITIONS ----------------------
   // do not really make sense here.
   // A select condition that makes sense and uses the partsupp stored in the merged index essentially calls for a join.

   // -----------------------------------------------------------
   // ---------------------- POINT QUERIES ----------------------
   // Aggregates of the same part id
   void pointQueryByView()
   {
      view.scan(view_t::Key{workload.getPartID()}, [&](const view_t::Key&, const view_t&) { return false; }, [&]() {});
   }

   void pointQueryByMerged()
   {
      auto scanner = mergedBasicGroup.getScanner();
      scanner->template seekTyped<merged_view_option_t>(typename merged_view_option_t::Key(workload.getPartID()));
      auto kv = scanner->current();
      assert(kv.has_value());
      auto& [k, v] = *kv;
      std::visit(overloaded{[&](const merged_partsupp_option_t&) { UNREACHABLE(); },
                            [&](const merged_view_option_t& aggregates) {
                               [[maybe_unused]] double avg_supplycost = aggregates.payload.sum_supplycost / aggregates.payload.count_partsupp;
                            }},
                 v);
   }

   // -----------------------------------------------------------
   // ---------------------- MAINTAIN ---------------------------
   // add one supplier (preexisting) for an existing part

   void maintainTemplate(std::function<bool(const partsupp_t::Key&, const partsupp_t&)> partsupp_insert_func,
                         leanstore::UpdateSameSizeInPlaceDescriptor& agg_update_descriptor,
                         std::function<void(const Integer, const Numeric, leanstore::UpdateSameSizeInPlaceDescriptor&)>
                             agg_update_func)  // increment count, add second argument to supply cost
   {
      // auto [part_id, supplier_id] = get_part_supplier_id();

      auto rec = partsupp_t::generateRandomRecord();
      auto ret = false;
      Integer part_id, supplier_id;
      while (!ret)
      {
         part_id = workload.getPartID();
         supplier_id = workload.getSupplierID();  // WARNING: breaking referential integrity
         ret = partsupp_insert_func(partsupp_t::Key({part_id, supplier_id}), rec);
         // if already exists, try new keys
      }
      // count_increment_func(part_id, countsupp_update_descriptor);
      // sum_update_func(part_id, rec.ps_supplycost, sum_supplycost_update_descriptor);
      agg_update_func(part_id, rec.ps_supplycost, agg_update_descriptor);
   }

   void maintainMerged()
   {
      // Refer only to merged for partsupp. Partsupp is discarded.
      UpdateDescriptorGenerator2(agg_update_descriptor, merged_view_option_t, payload.count_partsupp, payload.sum_supplycost);

      maintainTemplate(
          [this](const partsupp_t::Key& k, const partsupp_t& v) {
             try {
                mergedBasicGroup.insert(typename merged_partsupp_option_t::Key(k), merged_partsupp_option_t(v));
             } catch (std::runtime_error& e) {
                return false;
             }
             return true;
          },
          agg_update_descriptor,
          [this](const Integer part_id, const Numeric supplycost, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) {
             mergedBasicGroup.template update1<merged_view_option_t>(
                 typename merged_view_option_t::Key(part_id),
                 [supplycost](merged_view_option_t& rec) {
                    rec.payload.count_partsupp++;
                    rec.payload.sum_supplycost += supplycost;
                 },
                 update_descriptor);
          });
   }

   void maintainView()
   {
      // partsupp is kept as a base table
      UpdateDescriptorGenerator2(agg_update_descriptor, view_t, count_partsupp, sum_supplycost);
      maintainTemplate(
          [this](const partsupp_t::Key& k, const partsupp_t& v) {
             try {
                workload.partsupp.insert(k, v);
             } catch (std::runtime_error& e) {
                return false;
             }
             return true;
          },
          agg_update_descriptor,
          [this](const Integer part_id, const Numeric supplycost, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) {
             view.update1(
                 view_t::Key({part_id}),
                 [supplycost](view_t& rec) {
                    rec.count_partsupp++;
                    rec.sum_supplycost += supplycost;
                 },
                 update_descriptor);
          });
   }

   // ---------------------------------------------------------
   // ---------------------- LOAD -----------------------------

   void loadBaseTables();

   void loadAllOptions();

   void insert_agg(Integer curr_partkey, Integer count, Numeric supplycost_sum);

   void load();

   double get_view_size();
   double get_merged_size();

   void log_sizes();
};
}  // namespace basic_group
#include "load.tpp"  // IWYU pragma: keep