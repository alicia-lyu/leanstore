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
          typename merged_count_option_t,
          typename merged_sum_option_t,
          typename merged_partsupp_option_t>
class BasicGroup
{
   using TPCH = TPCHWorkload<AdapterType>;
   TPCH& workload;
   using merged_t = MergedAdapterType<merged_count_option_t, merged_sum_option_t, merged_partsupp_option_t>;
   using merged_k_variant_t =
       std::variant<typename merged_count_option_t::Key, typename merged_sum_option_t::Key, typename merged_partsupp_option_t::Key>;
   using merged_v_variant_t = std::variant<merged_count_option_t, merged_sum_option_t, merged_partsupp_option_t>;

   AdapterType<view_t>& view;
   AdapterType<partsupp_t>& partsupp;
   merged_t& mergedBasicGroup;

   Logger& logger;

  public:
   BasicGroup(TPCH& workload, merged_t& mbg, AdapterType<view_t>& v)
       : workload(workload), view(v), partsupp(workload.partsupp), mergedBasicGroup(mbg), logger(workload.logger)

   {
   }

   // point lookups: one on partsupp, one per aggregate
   // one per other base tables
   void pointLookupsTemplate(std::function<void(Integer&, Integer&)> lookup_partsupp_find_valid_key,
                             std::function<void(const Integer)> lookup_count_partsupp,
                             std::function<void(const Integer)> lookup_sum_supplycost)
   {
      // std::cout << "BasicGroup::pointLookupsTemplate for " << name << std::endl;
      Integer part_id = workload.getPartID();
      Integer supplier_id = workload.getSupplierID();
      lookup_partsupp_find_valid_key(part_id, supplier_id);
      lookup_count_partsupp(part_id);
      lookup_sum_supplycost(part_id);

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
                                   [&](const typename merged_count_option_t::Key&) { UNREACHABLE(); },
                                   [&](const typename merged_sum_option_t::Key&) { UNREACHABLE(); }},
                        k);
          },
          [this](const Integer part_id) {
             mergedBasicGroup.template lookup1<merged_count_option_t>(typename merged_count_option_t::Key(count_partsupp_t::Key({part_id})),
                                                                      [&](const merged_count_option_t&) {});
          },
          [this](const Integer part_id) {
             mergedBasicGroup.template lookup1<merged_sum_option_t>(typename merged_sum_option_t::Key(sum_supplycost_t::Key({part_id})),
                                                                    [&](const merged_sum_option_t&) {});
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
          [this](const Integer part_id) { view.lookup1(view_t::Key({part_id}), [&](const view_t&) {}); },
          [this](const Integer part_id) { view.lookup1(view_t::Key({part_id}), [&](const view_t&) {}); });
   }

   // queries
   static void inspectIncrementProduced(const std::string& msg, long& produced)
   {
      if (produced % 1000 == 0) {
         std::cout << "\r" << msg << (double)produced / 1000 << "k------------------------------------";
      }
      produced++;
   }

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
      scanner->template seekForPrev<merged_count_option_t>(typename merged_count_option_t::Key(count_partsupp_t::Key({0})));
      Integer count = 0;
      while (true) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         std::visit(overloaded{[](const merged_partsupp_option_t&) {
                                  // do nothing
                               },
                               [&](const merged_count_option_t& count_rec) { count = count_rec.payload.count; },
                               [&](const merged_sum_option_t& sum) { [[maybe_unused]] auto avg_supplycost = sum.payload.sum_supplycost / count; }},
                    v);
         inspectIncrementProduced("Enumerating merged: ", produced);
      }
      std::cout << "\rEnumerating merged: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "query-merged");
   }

   void pointQueryByView()
   {
      logger.reset();
      std::cout << "BasicGroup::pointQueryByView()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      Integer part_id = workload.getPartID();

      view.scan(view_t::Key{part_id}, [&](const view_t::Key&, const view_t&) { return false; }, [&]() {});
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "point-query-view");
   }

   void pointQueryByMerged()
   {
      logger.reset();
      std::cout << "BasicGroup::pointQueryByMerged()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      Integer part_id = workload.getPartID();

      auto scanner = mergedBasicGroup.getScanner();
      scanner->template seek<merged_count_option_t>(typename merged_count_option_t::Key(count_partsupp_t::Key({part_id})));
      auto update_part_id = [&part_id](const std::pair<merged_k_variant_t, merged_v_variant_t>& kv) {
         auto& [k, v] = kv;
         std::visit(overloaded{[&](const typename merged_partsupp_option_t::Key& actual_key) { part_id = actual_key.jk.partkey; },
                               [&](const typename merged_count_option_t::Key& actual_key) { part_id = actual_key.jk.partkey; },
                               [&](const typename merged_sum_option_t::Key& actual_key) { part_id = actual_key.jk.partkey; }},
                    k);
      };
      update_part_id(scanner->current().value());
      auto start_part_id = part_id;
      while (start_part_id == part_id) {
         update_part_id(scanner->next().value());
      }
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "point-query-merged");
   }

   // maintenance
   void maintainTemplate(std::function<std::pair<Integer, Integer>()> get_part_supplier_id,
                         std::function<void(const partsupp_t::Key&, const partsupp_t&)> partsupp_insert_func,
                         std::function<void(const Integer, leanstore::UpdateSameSizeInPlaceDescriptor&)> count_increment_func,
                         std::function<void(const Integer, const Numeric, leanstore::UpdateSameSizeInPlaceDescriptor&)> sum_update_func,
                         std::string name)
   {
      std::cout << "BasicGroup::maintainTemplate for " << name << std::endl;
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      // add one supplier (preexisting) for an existing part
      auto [part_id, supplier_id] = get_part_supplier_id();

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

   void maintainMerged()
   {
      // Refer only to merged for partsupp. Partsupp is discarded.
      UpdateDescriptorGenerator1(countsupp_update_descriptor, merged_count_option_t, payload.count);

      UpdateDescriptorGenerator1(sum_supplycost_update_descriptor, merged_sum_option_t, payload.sum_supplycost);

      maintainTemplate(
          [this]() {
             auto supposed_part_id = workload.getPartID();
             auto supposed_supplier_id = workload.getSupplierID();
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
                                         } else {
                                            // suppose the next id pairs are the following
                                            if (actual_key.pk.ps_suppkey == workload.last_supplier_id) {
                                               supposed_part_id = actual_key.pk.ps_partkey + 1;
                                               supposed_supplier_id = 1;
                                            } else {
                                               supposed_part_id = actual_key.pk.ps_partkey;
                                               supposed_supplier_id = actual_key.pk.ps_suppkey + 1;
                                            }
                                         }
                                      },
                                      [&](const typename merged_count_option_t::Key&) { UNREACHABLE(); },
                                      [&](const typename merged_sum_option_t::Key&) {
                                         UNREACHABLE();
                                      }},  // merged_partsupp_option_t is the only one that can be found
                           k);
             }
             return std::pair<Integer, Integer>(supposed_part_id, supposed_supplier_id);
          },
          [this](const partsupp_t::Key& k, const partsupp_t& v) {
             mergedBasicGroup.insert(typename merged_partsupp_option_t::Key(k), merged_partsupp_option_t(v));
          },
          [&countsupp_update_descriptor, this](const Integer part_id, leanstore::UpdateSameSizeInPlaceDescriptor&) {
             mergedBasicGroup.template update1<merged_count_option_t>(
                 typename merged_count_option_t::Key(count_partsupp_t::Key({part_id})), [](merged_count_option_t& rec) { rec.payload.count++; },
                 countsupp_update_descriptor);
          },
          [&sum_supplycost_update_descriptor, this](const Integer part_id, const Numeric supplycost, leanstore::UpdateSameSizeInPlaceDescriptor&) {
             mergedBasicGroup.template update1<merged_sum_option_t>(
                 typename merged_sum_option_t::Key(sum_supplycost_t::Key({part_id})),
                 [supplycost](merged_sum_option_t& rec) { rec.payload.sum_supplycost += supplycost; }, sum_supplycost_update_descriptor);
          },
          "merged");
   }

   void maintainView()
   {
      // partsupp is kept as a base table
      maintainTemplate(
          [this]() {
             auto supposed_part_id = workload.getPartID();
             auto supposed_supplier_id = workload.getSupplierID();
             partsupp.scan(
                 partsupp_t::Key({supposed_part_id, supposed_supplier_id}),
                 [&](const partsupp_t::Key& k, const partsupp_t&) {
                    if (k.ps_partkey != supposed_part_id || k.ps_suppkey != supposed_supplier_id) {  // supposed ids are new
                       return false;
                    } else {  // suppose the next id pairs are the following
                       if (k.ps_suppkey == workload.last_supplier_id) {
                          supposed_part_id = k.ps_partkey + 1;
                          supposed_supplier_id = 1;
                       } else {
                          supposed_part_id = k.ps_partkey;
                          supposed_supplier_id = k.ps_suppkey + 1;
                       }
                       return true;
                    }
                 },
                 []() {});
             return std::pair<Integer, Integer>(supposed_part_id, supposed_supplier_id);
          },
          [this](const partsupp_t::Key& k, const partsupp_t& v) { workload.partsupp.insert(k, v); },
          [this](const Integer part_id, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) {
             view.update1(view_t::Key({part_id}), [](view_t& rec) { rec.count_partsupp++; }, update_descriptor);
          },
          [this](const Integer part_id, const Numeric supplycost, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) {
             view.update1(view_t::Key({part_id}), [supplycost](view_t& rec) { rec.sum_supplycost += supplycost; }, update_descriptor);
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
      int num_part_keys = 0;
      while (true) {
         auto kv = partsupp_scanner->next();
         if (kv == std::nullopt) {
            insertAll(curr_partkey, count, supplycost_sum);
            break;
         }
         auto& [k, v] = *kv;
         mergedBasicGroup.insert(typename merged_partsupp_option_t::Key(k), merged_partsupp_option_t(v));
         if (k.ps_partkey == curr_partkey) {
            count++;
            supplycost_sum += v.ps_supplycost;
         } else {
            if (curr_partkey != 0) {
               insertAll(curr_partkey, count, supplycost_sum);
               std::cout << "\rLoading views and indexes for " << num_part_keys++ << " part keys------------------------------------";
            }

            curr_partkey = k.ps_partkey;
            count = 1;
            supplycost_sum = v.ps_supplycost;
         }
      }
   }

   void insertAll(Integer curr_partkey, Integer count, Numeric supplycost_sum)
   {
      view.insert(view_t::Key({curr_partkey}), view_t({count, supplycost_sum}));

      auto count_pk = count_partsupp_t::Key({curr_partkey});
      auto count_pv = count_partsupp_t({count});
      auto sum_pk = sum_supplycost_t::Key({curr_partkey});
      auto sum_pv = sum_supplycost_t({supplycost_sum});

      mergedBasicGroup.insert(typename merged_count_option_t::Key(count_pk), merged_count_option_t(count_pv));
      mergedBasicGroup.insert(typename merged_sum_option_t::Key(sum_pk), merged_sum_option_t(sum_pv));
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