#pragma once

// SELECT o.orderkey, o.orderdate, COUNT(*)
// FROM Orders o, Lineitem l
// WHERE o.orderkey = l.orderkey
// GROUP BY o.orderkey, o.orderdate;

#include <chrono>
#include <optional>
#include <variant>
#include "../tpch_workload.hpp"
#include "views.hpp"

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

   Integer rand_orderkey;

  public:
   BasicJoinGroup(TPCH& workload, MergedTree& m, AdapterType<view_t>& v)
       : workload(workload), merged(m), view(v), orders(workload.orders), lineitem(workload.lineitem), logger(workload.logger), rand_orderkey(0)
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
      logger.log(t, ColumnName::ELAPSED, "query-view");
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
      logger.log(t, ColumnName::ELAPSED, "query-merged");
   }

   // ---------------------------------------------------------------
   // ---------------------- POINT QUERIES -----------------------------
   // search aggregate for a specific orderkey

   void refresh_rand_keys() { rand_orderkey = workload.getOrderID(); }

   void point_query_by_view()
   {
      view.scan(view_t::Key{rand_orderkey, Timestamp{0}}, [&](const view_t::Key&, const view_t&) { return false; }, [&]() {});
   }

   void point_query_by_merged()
   {
      auto scanner = merged.getScanner();
      scanner->template seekTyped<merged_view_t>(typename merged_view_t::Key(rand_orderkey, Timestamp{0}));
      [[maybe_unused]] auto kv = scanner->current();
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTENANCE --------------------------
   // a new order with several lineitems, update the view

   void maintain_template(std::function<void(const orders_t::Key&, const orders_t&)> insert_orders,
                          std::function<void(const lineitem_t::Key&, const lineitem_t&)> insert_lineitem,
                          std::function<void(const view_t::Key&, const view_t&)> insert_view)
   {
      auto new_orderkey = workload.last_order_id + 1;
      auto order_v = orders_t::generateRandomRecord([this]() { return workload.getCustomerID(); });

      insert_orders(orders_t::Key{new_orderkey}, order_v);

      auto lineitem_cnt = workload.load_lineitems_1order(insert_lineitem, new_orderkey);

      insert_view(view_t::Key{new_orderkey, Timestamp{order_v.o_orderdate}}, view_t{lineitem_cnt});

      workload.last_order_id = new_orderkey;
   }

   void maintain_view()
   {
      maintain_template([this](const orders_t::Key& k, const orders_t& v) { orders.insert(k, v); },
                        [this](const lineitem_t::Key& k, const lineitem_t& v) { lineitem.insert(k, v); },
                        [this](const view_t::Key& k, const view_t& v) { view.insert(k, v); });
   }

   void maintain_merged()
   {
      maintain_template(
          [this](const orders_t::Key& k, const orders_t& v) {
             merged.insert(merged_orders_t::Key(sort_key_t{k.o_orderkey, v.o_orderdate}, k), merged_orders_t(v));
          },
          [this](const lineitem_t::Key& k, const lineitem_t& v) {
             merged.insert(merged_lineitem_t::Key(sort_key_t{k.l_orderkey, Timestamp{0}}, k), merged_lineitem_t(v));
          },
          [this](const view_t::Key& k, const view_t& v) { merged.insert(merged_view_t::Key(k), merged_view_t(v)); });
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
      Integer count = 0;
      long produced = 0;
      while (true) {
         auto kv = lineitem_scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [lk, lv] = *kv;
         merged.insert(typename merged_lineitem_t::Key(sort_key_t{lk.l_orderkey, Timestamp{0}}, lk), merged_lineitem_t(lv));
         if (curr_orderkey != lk.l_orderkey) {
            auto [ok, ov] = orders_scanner->next().value();
            merged.insert(typename merged_orders_t::Key(sort_key_t{curr_orderkey, ov.o_orderdate}, ok), merged_orders_t(ov));
            if (curr_orderkey != 0) {
               view_t::Key k_new{curr_orderkey, ov.o_orderdate};
               view_t v_new{count};
               view.insert(k_new, v_new);
               merged.insert(typename merged_view_t::Key(k_new), merged_view_t(v_new));
            }
            curr_orderkey = ok.o_orderkey;
            count = 0;
         }
         count++;
         TPCH::inspect_produced("Loading all options: ", produced);
      }
      std::map<std::string, double> sizes = {{"view", view.size() + orders.size() + lineitem.size()}, {"merged", merged.size()}};
      logger.log_sizes(sizes);
   }
};
}  // namespace basic_join_group