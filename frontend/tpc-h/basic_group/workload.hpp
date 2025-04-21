#pragma once

#include <optional>
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

template <class... Ts>
struct overloaded : Ts... {
   using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

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

   Integer point_query_partkey;
   Integer maintenance_partkey;
   Integer maintenance_supplierkey;

  public:
   BasicGroup(TPCH& workload, merged_t& mbg, AdapterType<view_t>& v)
       : workload(workload),
         view(v),
         partsupp(workload.partsupp),
         mergedBasicGroup(mbg),
         logger(workload.logger),
         point_query_partkey(0),
         maintenance_partkey(0),
         maintenance_supplierkey(0)
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
      workload.nation.lookup1(nation_t::Key{nation_id}, [&](const nation_t&) {});
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

   // queries
   static void inspectIncrementProduced(const std::string& msg, long& produced)
   {
      if (produced % 1000 == 0) {
         std::cout << "\r" << msg << (double)produced / 1000 << "k------------------------------------";
      }
      produced++;
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
             inspectIncrementProduced("Enumerating materialized view: ", produced);
             return true;
          },
          [&]() {});
      std::cout << "\rEnumerating materialized view: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "query-view");
   }

   void queryByMerged()
   {
      logger.reset();
      std::cout << "BasicGroup::queryByMerged()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      auto scanner = mergedBasicGroup.getScanner();
      scanner->template seekForPrev<merged_view_option_t>(typename merged_view_option_t::Key(0));
      while (true) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         std::visit(overloaded{[](const merged_partsupp_option_t&) {
                                  // do nothing
                               },
                               [&](const merged_view_option_t& aggregates) {
                                  [[maybe_unused]] double avg_supplycost = aggregates.payload.sum_supplycost / aggregates.payload.count_partsupp;
                               }},
                    v);
         inspectIncrementProduced("Enumerating merged: ", produced);
      }
      std::cout << "\rEnumerating merged: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "query-merged");
   }

   // -----------------------------------------------------------
   // ---------------------- POINT QUERIES ----------------------
   // Aggregates of the same part id

   void pointQueryByView()
   {
      logger.reset();
      std::cout << "BasicGroup::pointQueryByView()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      view.scan(view_t::Key{point_query_partkey}, [&](const view_t::Key&, const view_t&) { return false; }, [&]() {});
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "point-query-view");
   }

   void pointQueryByMerged()
   {
      logger.reset();
      std::cout << "BasicGroup::pointQueryByMerged()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      auto scanner = mergedBasicGroup.getScanner();
      scanner->template seekTyped<merged_view_option_t>(typename merged_view_option_t::Key(point_query_partkey));
      auto kv = scanner->current();
      assert(kv.has_value());
      auto& [k, v] = *kv;
      std::visit(overloaded{[&](const merged_partsupp_option_t&) { UNREACHABLE(); },
                            [&](const merged_view_option_t& aggregates) {
                               [[maybe_unused]] double avg_supplycost = aggregates.payload.sum_supplycost / aggregates.payload.count_partsupp;
                            }},
                 v);
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "point-query-merged");
   }

   // -----------------------------------------------------------
   // ---------------------- MAINTAIN ---------------------------
   // add one supplier (preexisting) for an existing part

   void maintainTemplate(std::function<std::pair<Integer, Integer>()> get_part_supplier_id,  // find an id pair that does not exist
                         std::function<void(const partsupp_t::Key&, const partsupp_t&)> partsupp_insert_func,
                         leanstore::UpdateSameSizeInPlaceDescriptor& agg_update_descriptor,
                         std::function<void(const Integer, const Numeric, leanstore::UpdateSameSizeInPlaceDescriptor&)>
                             agg_update_func,  // increment count, add second argument to supply cost
                         std::string name)
   {
      std::cout << "BasicGroup::maintainTemplate for " << name << std::endl;
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      auto [part_id, supplier_id] = get_part_supplier_id();

      auto rec = partsupp_t::generateRandomRecord();
      partsupp_insert_func(partsupp_t::Key({part_id, supplier_id}), rec);
      // count_increment_func(part_id, countsupp_update_descriptor);
      // sum_update_func(part_id, rec.ps_supplycost, sum_supplycost_update_descriptor);
      agg_update_func(part_id, rec.ps_supplycost, agg_update_descriptor);
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "maintain-" + name);
   }

   void maintainMerged()
   {
      // Refer only to merged for partsupp. Partsupp is discarded.
      UpdateDescriptorGenerator2(agg_update_descriptor, merged_view_option_t, payload.count_partsupp, payload.sum_supplycost);

      maintainTemplate(
          [this]() {
             auto supposed_part_id = maintenance_partkey;
             auto supposed_supplier_id = maintenance_supplierkey;
             auto merged_scanner = mergedBasicGroup.getScanner();
             bool found = false;
             while (!found) {
                merged_scanner->template seekTyped<merged_partsupp_option_t>(
                    typename merged_partsupp_option_t::Key(partsupp_t::Key{supposed_part_id, supposed_supplier_id}));
                auto kv = merged_scanner->current();
                assert(kv.has_value());
                auto& [k, v] = *kv;
                std::visit(overloaded{[&](const typename merged_partsupp_option_t::Key& actual_key) {
                                         if (actual_key.pk.ps_partkey != supposed_part_id ||
                                             actual_key.pk.ps_suppkey != supposed_supplier_id) {  // supposed ids are new
                                            found = true;
                                         } else {  // actualkey == {supposed_part_id, supposed_supplier_id}
                                            // suppose the next id pairs are the following
                                            if (supposed_supplier_id == workload.last_supplier_id) {
                                               supposed_part_id++;
                                               supposed_supplier_id = 1;
                                            } else {
                                               supposed_supplier_id++;
                                            }
                                         }
                                      },
                                      [&](const typename merged_view_option_t::Key&) { UNREACHABLE(); }},
                           k);
             }
             return std::pair<Integer, Integer>(supposed_part_id, supposed_supplier_id);
          },
          [this](const partsupp_t::Key& k, const partsupp_t& v) {
             mergedBasicGroup.insert(typename merged_partsupp_option_t::Key(k), merged_partsupp_option_t(v));
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
          },
          "merged");
   }

   void maintainView()
   {
      // partsupp is kept as a base table
      UpdateDescriptorGenerator2(agg_update_descriptor, view_t, count_partsupp, sum_supplycost);
      maintainTemplate(
          [this]() {
             auto supposed_part_id = maintenance_partkey;
             auto supposed_supplier_id = maintenance_supplierkey;
             partsupp.scan(
                 partsupp_t::Key({supposed_part_id, supposed_supplier_id}),
                 [&](const partsupp_t::Key& k, const partsupp_t&) {
                    if (k.ps_partkey != supposed_part_id || k.ps_suppkey != supposed_supplier_id) {  // supposed ids are new
                       return false;
                    } else {  // actualkey == {supposed_part_id, supposed_supplier_id}
                              // suppose the next id pairs are the following
                       if (supposed_supplier_id == workload.last_supplier_id) {
                          supposed_part_id++;
                          supposed_supplier_id = 1;
                       } else {
                          supposed_supplier_id++;
                       }
                       return true;
                    }
                 },
                 []() {});
             return std::pair<Integer, Integer>(supposed_part_id, supposed_supplier_id);
          },
          [this](const partsupp_t::Key& k, const partsupp_t& v) { workload.partsupp.insert(k, v); }, agg_update_descriptor,
          [this](const Integer part_id, const Numeric supplycost, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) {
             view.update1(
                 view_t::Key({part_id}),
                 [supplycost](view_t& rec) {
                    rec.count_partsupp++;
                    rec.sum_supplycost += supplycost;
                 },
                 update_descriptor);
          },
          "view");
   }

   // ---------------------------------------------------------
   // ---------------------- LOAD -----------------------------

   void loadBaseTables() { workload.load(); }

   void loadAllOptions()
   {
      point_query_partkey = workload.getPartID();
      maintenance_partkey = workload.getPartID();
      maintenance_supplierkey = workload.getSupplierID();

      auto partsupp_scanner = workload.partsupp.getScanner();
      Integer count = 0;
      Numeric supplycost_sum = 0;
      Integer curr_partkey = 0;
      int num_part_keys = 0;
      while (true) {
         auto kv = partsupp_scanner->next();
         if (kv == std::nullopt) {
            insert_agg(curr_partkey, count, supplycost_sum);
            break;
         }
         auto& [k, v] = *kv;
         mergedBasicGroup.insert(typename merged_partsupp_option_t::Key(k), merged_partsupp_option_t(v));
         if (k.ps_partkey == curr_partkey) {
            count++;
            supplycost_sum += v.ps_supplycost;
         } else {
            if (curr_partkey != 0) {
               insert_agg(curr_partkey, count, supplycost_sum);
               std::cout << "\rLoading views and indexes for " << num_part_keys++ << " part keys------------------------------------";
            }

            curr_partkey = k.ps_partkey;
            count = 1;
            supplycost_sum = v.ps_supplycost;
         }
      }
   }

   void insert_agg(Integer curr_partkey, Integer count, Numeric supplycost_sum)
   {
      view.insert(view_t::Key({curr_partkey}), view_t({count, supplycost_sum}));
      mergedBasicGroup.insert(typename merged_view_option_t::Key(curr_partkey), merged_view_option_t(view_t{count, supplycost_sum}));
   }

   void logSize()
   {
      std::cout << "Logging size" << std::endl;
      std::ofstream size_csv;
      std::filesystem::create_directories(FLAGS_csv_path);
      size_csv.open(FLAGS_csv_path + "/size.csv", std::ios::app);
      if (size_csv.tellp() == 0) {
         size_csv << "table,size (MiB)" << std::endl;
      }
      std::cout << "table,size" << std::endl;
      std::vector<std::ostream*> out = {&std::cout, &size_csv};
      for (std::ostream* o : out) {
         *o << "view," << view.size() + partsupp.size() << std::endl;
         *o << "merged," << mergedBasicGroup.size() << std::endl;
      }
      size_csv.close();
   }
};
}  // namespace basic_group