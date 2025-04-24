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

      workload.part.scan(part_t::Key{partkey}, [&](const part_t&) { return false; }, [&]() {});
      workload.supplier.scan(supplier_t::Key{supplierkey}, [&](const supplier_t&) { return false; }, [&]() {});
      workload.partsupp.scan(partsupp_t::Key{partkey, supplierkey}, [&](const partsupp_t&) { return false; }, [&]() {});
      workload.customer.scan(customerh_t::Key{customerkey}, [&](const customerh_t&) { return false; }, [&]() {});
      workload.nation.scan(nation_t::Key{nationkey}, [&](const nation_t&) { return false; }, [&]() {});
      workload.region.scan(region_t::Key{regionkey}, [&](const region_t&) { return false; }, [&]() {});
   }

   void point_lookups_for_view()
   {
      point_lookups_template([this](Integer orderkey) {
         orders.scan(orders_t::Key{orderkey}, [&](const orders_t&) { return false; }, [&]() {});
         lineitem.scan(lineitem_t::Key{orderkey}, [&](const lineitem_t&) { return false; }, [&]() {});
         view.scan(view_t::Key{orderkey}, [&](const view_t&) { return false; }, [&]() {});
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

            std::visit(overloaded{[&](const merged_orders_t::Key&) { curr_orderkey = k.jk.orderkey; },
                                  [&](const merged_lineitem_t::Key&) { curr_orderkey = k.jk.orderkey; },
                                  [&](const merged_view_t::Key&) { curr_orderkey = k.jk.orderkey; }},
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
         scanner->template seekTyped<merged_view_t>(
             typename merged_view_t::Key(orderkey, Timestamp{1}));  // timestamp 1 to skip lineitems, each orderkey has a unique timestamp, expect to
                                                                    // land on merged_orders_t, whatever the timestamp is, scan only 1 extra record
         auto kv = scanner->current();
         if (kv == std::nullopt)
            break;
         TPCH::inspect_produced("Enumerating merged: ", produced);
      }
      std::cout << "\rEnumerating merged: " << (double)produced / 1000 << "k------------------------------------" << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, ColumnName::ELAPSED, "query-merged");
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
      while (true) {
         auto kv = lineitem_scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [lk, lv] = *kv;
         merged.insert(typename merged_lineitem_t::Key(lk), merged_lineitem_t(lv));
         auto& [ok, ov] = *kv;
         if (curr_orderkey != ok.l_orderkey) {
            auto& [k, v] = orders_scanner->next().value();
            merged.insert(typename merged_orders_t::Key(ok), merged_orders_t(ov));
            if (curr_orderkey != 0) {
               view_t::Key k_new(curr_orderkey, v.o_orderdate);
               view_t v_new{count};
               view.insert(k_new, v_new);
               merged.insert(typename merged_view_t::Key(k_new), merged_view_t(v_new));
            }
            curr_orderkey = k.l_orderkey;
            count = 0;
         }
         count++;
      }
      std::map<std::string, double> sizes = {{"view", view.size() + orders.size() + lineitem.size()}, {"merged", merged.size()}};
   }
};
}  // namespace basic_join_group