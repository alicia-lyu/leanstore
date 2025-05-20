#pragma once

#include <chrono>
#include <limits>
#include <optional>
#include <variant>
#include "../../shared/Adapter.hpp"
#include "../tpch_workload.hpp"
#include "Exceptions.hpp"
#include "views.hpp"

// SELECT o.orderkey, o.orderdate, COUNT(*)
// FROM Orders o, Lineitem l
// WHERE o.orderkey = l.orderkey
// GROUP BY o.orderkey, o.orderdate;

namespace basic_join_group
{
template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
class BasicJoinGroup
{
   using TPCH = TPCHWorkload<AdapterType>;
   TPCH& workload;
   using MergedTree = MergedAdapterType<merged_view_t, merged_orders_t, merged_lineitem_t>;
   MergedTree& merged;
   AdapterType<view_t>& view;
   AdapterType<orders_t>& orders;
   AdapterType<lineitem_t>& lineitem;

   Logger& logger;

  public:
   BasicJoinGroup(TPCH& workload, MergedTree& m, AdapterType<view_t>& v)
       : workload(workload), merged(m), view(v), orders(workload.orders), lineitem(workload.lineitem), logger(workload.logger)
   {
   }

   // -------------------------------------------------------------
   // ---------------------- POINT LOOKUPS ------------------------
   // point lookups: one per view, one per all base tables, sharing orderkey if present

   void point_lookups_template(std::function<void(Integer)> lookup_tables_with_orderkey)
   {
      auto orderkey = workload.getOrderID();
      lookup_tables_with_orderkey(orderkey);

      // base tables
      auto partkey = workload.getPartID();
      auto supplierkey = workload.getSupplierID();
      auto customerkey = workload.getCustomerID();
      auto nationkey = workload.getNationID();
      auto regionkey = workload.getRegionID();

      workload.part.scan(part_t::Key{partkey}, [](const part_t::Key&, const part_t&) { return false; }, []() {});
      workload.supplier.scan(supplier_t::Key{supplierkey}, [](const supplier_t::Key&, const supplier_t&) { return false; }, []() {});
      workload.partsupp.scan(partsupp_t::Key{partkey, supplierkey}, [](const partsupp_t::Key&, const partsupp_t&) { return false; }, []() {});
      workload.customer.scan(customerh_t::Key{customerkey}, [](const customerh_t::Key&, const customerh_t&) { return false; }, []() {});
      workload.nation.scan(nation_t::Key{nationkey}, [](const nation_t::Key&, const nation_t&) { return false; }, []() {});
      workload.region.scan(region_t::Key{regionkey}, [](const region_t::Key&, const region_t&) { return false; }, []() {});
   }

   void point_lookups_for_view()
   {
      point_lookups_template([this](Integer orderkey) {
         orders.scan(orders_t::Key{orderkey}, [&](const orders_t::Key&, const orders_t&) { return false; }, [&]() {});
         lineitem.scan(lineitem_t::Key{orderkey, 1}, [&](const lineitem_t::Key&, const lineitem_t&) { return false; }, [&]() {});
         view.scan(view_t::Key{orderkey, Timestamp{0}}, [&](const view_t::Key&, const view_t&) { return false; }, [&]() {});
      });
   }

   void point_lookups_for_merged()
   {
      point_lookups_template([this](Integer orderkey) {
         auto scanner = merged.getScanner();
         scanner->seekJK(sort_key_t{orderkey, 0});
         Integer curr_orderkey = orderkey;
         while (curr_orderkey == orderkey) {
            auto kv = scanner->next();
            if (kv == std::nullopt)
               break;
            auto& [k, v] = *kv;

            std::visit(overloaded{[&](const merged_orders_t::Key& actual_key) { curr_orderkey = actual_key.jk.orderkey; },
                                  [&](const merged_lineitem_t::Key& actual_key) { curr_orderkey = actual_key.jk.orderkey; },
                                  [&](const merged_view_t::Key& actual_key) { curr_orderkey = actual_key.jk.orderkey; }},
                       k);
         }
      });
   }

   // -------------------------------------------------------------
   // ---------------------- QUERIES -----------------------------

   void query_by_view()  // scan through the view
   {
      logger.reset();
      std::cout << "BasicJoinGroup::query_by_view()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      view.scan(
          {},
          [&](const view_t::Key&, const view_t&) {
             TPCH::inspect_produced("Enumerating materialized view: ", produced);
             return true;
          },
          [&]() {});
      std::cout << "\rEnumerating materialized view: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, "query", "view", get_view_size());
   }

   void query_by_merged()  // jump through the merged for the view
   {
      logger.reset();
      std::cout << "BasicJoinGroup::query_by_merged()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      auto scanner = merged.getScanner();
      Integer orderkey = 1;
      while (true) {
         auto ret = scanner->template seekTyped<merged_view_t>(
             typename merged_view_t::Key(orderkey, Timestamp{1}));  // timestamp 1 to skip lineitems, each orderkey has a unique timestamp, expect to
         // land on merged_orders_t, whatever the timestamp is, scan only 1 extra record
         if (ret == false)
            break;
         auto kv = scanner->current();
         if (kv == std::nullopt)
            break;
         TPCH::inspect_produced("Enumerating merged: ", produced);
         orderkey++;
      }
      std::cout << "\rEnumerating merged: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, "query", "merged", get_merged_size());
   }

   // -----------------------------------------------------------------------------------
   // ---------------------- QUERIES w EXTERNAL SELECT CONDITION ------------------------
   // WHERE o.orderstatus = 'O' -- not shipped yet

   void query_by_view_external_select()
   {
      logger.reset();
      std::cout << "BasicJoinGroup::query_by_view_external_select()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      auto orders_scanner = orders.getScanner();
      while (true) {
         auto kv = orders_scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         if (v.o_orderstatus == "O") {
            view.lookup1(view_t::Key{k.o_orderkey, v.o_orderdate}, [&](const view_t&) {});
            workload.inspect_produced("Jumping through view: ", produced);
         }
      }
      std::cout << "\rJumping through view: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, "query-external-select", "view", get_view_size());
   }

   void query_by_merged_external_select()
   {
      logger.reset();
      std::cout << "BasicJoinGroup::query_by_merged_external_select()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] long produced = 0;
      auto scanner = merged.getScanner();
      bool selected = false;
      while (true) {
         auto kv = scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         std::visit(overloaded{[&](const merged_orders_t& actual_v) {
                                  if (actual_v.payload.o_orderstatus == "O") {
                                     selected = true;
                                  }
                               },
                               [&](const merged_lineitem_t&) {},
                               [&](const merged_view_t&) {
                                  if (selected) {
                                     // do something with the selected aggregates
                                     workload.inspect_produced("Jumping through merged: ", produced);
                                  }
                                  selected = false;
                               }},
                    v);
         std::visit(overloaded{[&](const merged_orders_t::Key&) {},
                               [&](const merged_lineitem_t::Key& actual_k) {
                                  scanner->template seekForPrev<merged_orders_t>(
                                      typename merged_orders_t::Key(actual_k.jk.orderkey, Timestamp{1}));  // skip lineitems
                               },
                               [&](const merged_view_t::Key&) {}},
                    k);
      }
      std::cout << "\rJumping through merged: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, "query-external-select", "merged", get_merged_size());
   }

   // ---------------------------------------------------------------
   // ---------------------- POINT QUERIES -----------------------------
   // WHERE orderkey = ?

   void point_query_by_view()
   {
      view.scan(view_t::Key{workload.getOrderID(), Timestamp{0}}, [&](const view_t::Key&, const view_t&) { return false; }, [&]() {});
   }

   void point_query_by_merged()
   {
      auto scanner = merged.getScanner();
      scanner->template seekTyped<merged_view_t>(typename merged_view_t::Key(workload.getOrderID(), Timestamp{0}));
      [[maybe_unused]] auto kv = scanner->current();
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTENANCE --------------------------
   // Insert a lineitem from a random order, update o_totalprice, update the view

   void maintain_view()
   {
      // get the last lineitem number for this orderkey for lineitem count
      int lineitem_cnt = 0;
      auto rand_orderkey = workload.getOrderID();
      lineitem.scanDesc(
          lineitem_t::Key{rand_orderkey, std::numeric_limits<Integer>::max()},
          [&](const lineitem_t::Key& k, const lineitem_t&) {
             lineitem_cnt = k.l_linenumber;
             return false;
          },
          [&]() {});
      // insert new lineitem
      auto lv = lineitem_t::generateRandomRecord([this]() { return workload.getPartID(); }, [this]() { return workload.getSupplierID(); });
      lineitem.insert(lineitem_t::Key{rand_orderkey, lineitem_cnt + 1}, lv);
      // get the correct orderdate for this orderkey
      view_t::Key vk{rand_orderkey, Timestamp{0}};
      view.scan(
          view_t::Key{rand_orderkey, Timestamp{0}},
          [&](const view_t::Key& k, const view_t&) {
             vk = k;
             return false;
          },
          []() {});
      // update view
      UpdateDescriptorGenerator1(view_update_descriptor, view_t, count_lineitem);
      view.update1(vk, [](view_t& rec) { rec.count_lineitem++; }, view_update_descriptor);
      // update orders
      UpdateDescriptorGenerator1(orders_update_descriptor, orders_t, o_totalprice);
      orders.update1(orders_t::Key{rand_orderkey}, [&lv](orders_t& rec) { rec.o_totalprice += lv.l_extendedprice; }, orders_update_descriptor);
   }

   void maintain_merged()
   {
      auto scanner = merged.getScanner();
      auto rand_orderkey = workload.getOrderID();
      auto max_int = std::numeric_limits<Integer>::max();
      // in this complex object instance, first come the last lineitem because their date is 0
      scanner->template seekForPrev<merged_lineitem_t>(
          typename merged_lineitem_t::Key(sort_key_t{rand_orderkey, 0}, lineitem_t::Key{max_int, max_int}));
      auto l_kv = scanner->current();
      assert(l_kv != std::nullopt);
      auto& [lk, lv] = *l_kv;
      // get lineitem count
      int lineitem_cnt = 0;
      std::visit(
          overloaded{[&](const merged_orders_t::Key&) { UNREACHABLE(); },
                     [&](const merged_lineitem_t::Key& lk) { lineitem_cnt = lk.pk.l_linenumber; }, [&](const merged_view_t::Key&) { UNREACHABLE(); }},
          lk);
      // get actual keys with correct orderdate
      auto o_kv = scanner->next();
      assert(o_kv != std::nullopt);
      auto& [ok, ov] = *o_kv;
      merged_orders_t::Key actual_ok;
      std::visit(overloaded{[&](const merged_orders_t::Key& actual_key) { actual_ok = actual_key; },
                            [&](const merged_lineitem_t::Key&) { UNREACHABLE(); }, [&](const merged_view_t::Key&) { UNREACHABLE(); }},
                 ok);

      auto v_kv = scanner->next();
      auto& [vk, vv] = *v_kv;
      merged_view_t::Key actual_vk;
      std::visit(overloaded{[&](const merged_orders_t::Key&) { UNREACHABLE(); }, [&](const merged_lineitem_t::Key&) { UNREACHABLE(); },
                            [&](const merged_view_t::Key& actual_key) { actual_vk = actual_key; }},
                 vk);
      scanner->reset();  // avoid contention
      // insert new lineitem
      auto new_lv = lineitem_t::generateRandomRecord([this]() { return workload.getPartID(); }, [this]() { return workload.getSupplierID(); });
      merged.insert(merged_lineitem_t::Key(sort_key_t{rand_orderkey, Timestamp{0}}, lineitem_t::Key{rand_orderkey, lineitem_cnt + 1}),
                    merged_lineitem_t(new_lv));
      // update view
      UpdateDescriptorGenerator1(view_update_descriptor, merged_view_t, payload.count_lineitem);
      merged.template update1<merged_view_t>(actual_vk, [](merged_view_t& rec) { rec.payload.count_lineitem++; }, view_update_descriptor);

      // update orders
      UpdateDescriptorGenerator1(orders_update_descriptor, merged_orders_t, payload.o_totalprice);
      merged.template update1<merged_orders_t>(
          actual_ok, [&new_lv](merged_orders_t& rec) { rec.payload.o_totalprice += new_lv.l_extendedprice; }, orders_update_descriptor);
   }

   // -------------------------------------------------------------
   // ------------------------- LOAD -----------------------------

   void load()
   {
      workload.load();
      // group lineitem by orderkey into a view with order_date from orders
      auto lineitem_scanner = workload.lineitem.getScanner();
      auto orders_scanner = workload.orders.getScanner();
      Integer curr_orderkey = 0;
      Timestamp curr_orderdate = Timestamp{0};
      Integer count = 0;
      long produced = 0;
      while (true) {
         auto kv = lineitem_scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [lk, lv] = *kv;
         merged.insert(typename merged_lineitem_t::Key(sort_key_t{lk.l_orderkey, Timestamp{0}}, lk), merged_lineitem_t(lv));
         if (curr_orderkey != lk.l_orderkey) {
            if (curr_orderkey != 0) {
               view_t::Key k_new{curr_orderkey, curr_orderdate};
               view_t v_new{count};
               view.insert(k_new, v_new);
               merged.insert(typename merged_view_t::Key(k_new), merged_view_t(v_new));
            }
            auto [ok, ov] = orders_scanner->next().value();
            merged.insert(typename merged_orders_t::Key(sort_key_t{ok.o_orderkey, ov.o_orderdate}, ok), merged_orders_t(ov));
            curr_orderkey = ok.o_orderkey;
            curr_orderdate = ov.o_orderdate;
            count = 0;
         }
         count++;
         TPCH::inspect_produced("Loading all options: ", produced);
      }
      log_sizes();
   }

   double get_view_size() { return view.size() + orders.size() + lineitem.size(); }

   double get_merged_size() { return merged.size(); }

   void log_sizes()
   {
      workload.log_sizes();
      std::map<std::string, double> sizes = {{"view", get_view_size()}, {"merged", get_merged_size()}};
      logger.log_sizes(sizes);
   }
};
}  // namespace basic_join_group