#pragma once
#include <gflags/gflags.h>
#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <vector>

#include "tables.hpp"

#include "logger.hpp"

DECLARE_int32(tpch_scale_factor);

template <class... Ts>
struct overloaded : Ts... {
   using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

template <template <typename> class AdapterType>
struct TPCHWorkload {
   Logger& logger;
   AdapterType<part_t>& part;
   AdapterType<supplier_t>& supplier;
   AdapterType<partsupp_t>& partsupp;
   AdapterType<customerh_t>& customer;
   AdapterType<orders_t>& orders;
   AdapterType<lineitem_t>& lineitem;
   AdapterType<nation_t>& nation;
   AdapterType<region_t>& region;

   TPCHWorkload(AdapterType<part_t>& p,
                AdapterType<supplier_t>& s,
                AdapterType<partsupp_t>& ps,
                AdapterType<customerh_t>& c,
                AdapterType<orders_t>& o,
                AdapterType<lineitem_t>& l,
                AdapterType<nation_t>& n,
                AdapterType<region_t>& r,
                Logger& logger)
       : logger(logger),
         part(p),
         supplier(s),
         partsupp(ps),
         customer(c),
         orders(o),
         lineitem(l),
         nation(n),
         region(r),
         last_part_id(0),
         last_supplier_id(0),
         last_customer_id(0),
         last_order_id(0)
   {
   }

   void load()
   {
      loadPart();
      loadSupplier();
      loadPartsuppLineitem();
      loadCustomer();
      loadOrders();
      loadNation();
      loadRegion();
      log_sizes();
   }

   static constexpr Integer PART_SCALE = 200;
   static constexpr Integer SUPPLIER_SCALE = 10;
   static constexpr Integer CUSTOMER_SCALE = 150;
   static constexpr Integer ORDERS_SCALE = 1500;
   static constexpr Integer LINEITEM_SCALE = 6000;
   static constexpr Integer PARTSUPP_SCALE = 800;
   static constexpr Integer NATION_COUNT = 25;
   static constexpr Integer REGION_COUNT = 5;

   Integer last_part_id;
   Integer last_supplier_id;
   Integer last_customer_id;
   Integer last_order_id;

   void recover_last_ids()
   {
      part.scanDesc(
          part_t::Key{std::numeric_limits<Integer>::max()},
          [&](const part_t::Key& k, const part_t&) {
             last_part_id = k.p_partkey;
             return false;
          },
          []() {});
      supplier.scanDesc(
          supplier_t::Key{std::numeric_limits<Integer>::max()},
          [&](const supplier_t::Key& k, const supplier_t&) {
             last_supplier_id = k.s_suppkey;
             return false;
          },
          []() {});
      customer.scanDesc(
          customerh_t::Key{std::numeric_limits<Integer>::max()},
          [&](const customerh_t::Key& k, const customerh_t&) {
             last_customer_id = k.c_custkey;
             return false;
          },
          []() {});
      orders.scanDesc(
          orders_t::Key{std::numeric_limits<Integer>::max()},
          [&](const orders_t::Key& k, const orders_t&) {
             last_order_id = k.o_orderkey;
             return false;
          },
          []() {});
      std::cout << "Recovered last_part_id: " << last_part_id << ", last_supplier_id: " << last_supplier_id
                << ", last_customer_id: " << last_customer_id << ", last_order_id: " << last_order_id << std::endl;
   }

   inline Integer getPartID() { return urand(1, last_part_id); }

   inline Integer getSupplierID() { return urand(1, last_supplier_id); }

   inline Integer getCustomerID() { return urand(1, last_customer_id); }

   inline Integer getOrderID() { return urand(1, last_order_id); }

   inline Integer getNationID() { return urand(1, NATION_COUNT); }

   inline Integer getRegionID() { return urand(1, REGION_COUNT); }

   // ------------------------------------LOAD-------------------------------------------------

   void prepare() { logger.prepare(); }

   void printProgress(std::string msg, Integer i, Integer start, Integer end)
   {
      auto scale = end - start + 1;
      if (scale < 100)
         return;
      if (i % 1000 == start % 1000 || i == end) {
         double progress = (double)(i - start + 1) / scale * 100;
         std::cout << "\rLoading " << scale << " " << msg << ": " << progress << "%------------------------------------";
      }
      if (i == end && scale > 100) {
         std::cout << std::endl;
      }
   }

   void loadPart(std::function<void(const part_t::Key&, const part_t&)> insert_func, Integer start, Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         // std::cout << "partkey: " << i << std::endl;
         insert_func(part_t::Key{i}, part_t::generateRandomRecord());
         printProgress("part", i, start, end);
      }
      last_part_id = end;
   }

   void loadPart(Integer start = 1, Integer end = PART_SCALE * FLAGS_tpch_scale_factor)
   {
      loadPart([this](const part_t::Key& k, const part_t& v) { this->part.insert(k, v); }, start, end);
   }

   void loadSupplier(std::function<void(const supplier_t::Key&, const supplier_t&)> insert_func, Integer start, Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         insert_func(supplier_t::Key{i}, supplier_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("supplier", i, start, end);
      }
      last_supplier_id = end;
   }

   void loadSupplier(Integer start = 1, Integer end = SUPPLIER_SCALE * FLAGS_tpch_scale_factor)
   {
      loadSupplier([this](const supplier_t::Key& k, const supplier_t& v) { this->supplier.insert(k, v); }, start, end);
   }

   void loadPartsuppLineitem(std::function<void(const partsupp_t::Key&, const partsupp_t&)> ps_insert_func,
                             std::function<void(const lineitem_t::Key&, const lineitem_t&)> l_insert_func,
                             Integer part_start,
                             Integer part_end,
                             Integer order_start,
                             Integer order_end)
   {
      // generate order keys from order_start to order_end
      if (order_end > order_start)
         std::cout << "Generating and shuffling order keys..." << std::endl;
      std::vector<Integer> order_keys(order_end - order_start + 1);
      std::iota(order_keys.begin(), order_keys.end(), order_start);
      std::random_shuffle(order_keys.begin(), order_keys.end());

      // Load partsupp and lineitem
      const Integer partsupp_size = (PARTSUPP_SCALE / PART_SCALE) * (part_end - part_start + 1);
      const Integer lineitem_size = (LINEITEM_SCALE / ORDERS_SCALE) * (order_end - order_start + 1);
      auto current_order_key = order_keys.begin();
      auto lineitem_cnt_in_order = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2 - 1);
      int lineitem_number = 1;
      // (part, supplier) and (order, lineitem) pairs are not perfectly independent,
      // but (part, supplier) and (order) are.
      for (Integer i = part_start; i <= part_end; i++) {
         printProgress("parts of partsupp and lineitem", i, part_start, part_end);
         // Randomly select suppliers for this part
         size_t supplier_cnt = urand(1, PARTSUPP_SCALE / PART_SCALE * 2 - 1);
         std::set<Integer> suppliers = {};
         while (suppliers.size() < supplier_cnt) {
            Integer supplier_id = urand(1, last_supplier_id);
            suppliers.insert(supplier_id);
         }
         for (auto& s : suppliers) {
            // load 1 partsupp
            ps_insert_func(partsupp_t::Key{i, s}, partsupp_t::generateRandomRecord());
            // load lineitems
            Integer lineitem_cnt_ps = urand(0, lineitem_size / partsupp_size * 2);
            // ps pairs between partsupp and lineitem has no referential integrity but mostly matched
            for (Integer l = 0; l < lineitem_cnt_ps; l++) {
               auto rec = lineitem_t::generateRandomRecord([i]() { return i; }, [s]() { return s; });
               l_insert_func(lineitem_t::Key{*current_order_key, lineitem_number}, rec);
               lineitem_number++;
               if (lineitem_number > lineitem_cnt_in_order) {
                  lineitem_number = 1;
                  current_order_key++;
                  lineitem_cnt_in_order = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2 - 1);
                  if (current_order_key == order_keys.end()) {
                     current_order_key = order_keys.begin();
                  }
               }
            }
         }
      }
      // load remaining lineitems
      if (lineitem_number > 1 && current_order_key < order_keys.end()) {
         lineitem_number = 1;
         current_order_key++;
         lineitem_cnt_in_order = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2 - 1);
      }
      auto rem_order_cnt = order_keys.end() - current_order_key;
      if (rem_order_cnt > 0)
         std::cout << rem_order_cnt << " orders left to fill out lineitems" << std::endl;
      auto orders_rem_start = current_order_key;
      for (; current_order_key < order_keys.end(); current_order_key++) {
         printProgress("orders of lineitems", current_order_key - orders_rem_start, 0, order_keys.end() - orders_rem_start);
         loadLineitem(l_insert_func, *current_order_key, *current_order_key);
      }
   }

   void loadPartsuppLineitem(Integer part_start = 1,
                             Integer part_end = PART_SCALE * FLAGS_tpch_scale_factor,
                             Integer order_start = 1,
                             Integer order_end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      loadPartsuppLineitem([this](const partsupp_t::Key& k, const partsupp_t& v) { this->partsupp.insert(k, v); },
                           [this](const lineitem_t::Key& k, const lineitem_t& v) { this->lineitem.insert(k, v); }, part_start, part_end, order_start,
                           order_end);
   }

   void loadPartsupp(std::function<void(const partsupp_t::Key&, const partsupp_t&)> insert_func,
                     Integer part_start = 1,
                     Integer part_end = PART_SCALE * FLAGS_tpch_scale_factor)
   {
      loadPartsuppLineitem(insert_func, [](const lineitem_t::Key&, const lineitem_t&) {}, part_start, part_end, last_order_id, last_order_id - 1);
   }

   void loadLineitem(std::function<void(const lineitem_t::Key&, const lineitem_t&)> insert_func, Integer order_start, Integer order_end)
   {
      for (Integer i = order_start; i <= order_end; i++) {
         load_lineitems_1order(insert_func, i);
      }
   }

   int load_lineitems_1order(std::function<void(const lineitem_t::Key&, const lineitem_t&)> insert_func, Integer orderkey)
   {
      Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2 - 1);
      for (Integer j = 1; j <= lineitem_cnt; j++) {
         // look up partsupp
         auto p = urand(1, last_part_id);
         auto s = urand(1, last_supplier_id);
         auto start_key = partsupp_t::Key{p, s};
         partsupp.scan(
             start_key,
             [&](const partsupp_t::Key& k, const partsupp_t&) {
                p = k.ps_partkey;
                s = k.ps_suppkey;
                // LATER: each ps pair does not have uniform chance of being selected
                return false;
             },
             []() {});
         insert_func(lineitem_t::Key{orderkey, j}, lineitem_t::generateRandomRecord([p]() { return p; }, [s]() { return s; }));
      }
      return lineitem_cnt;
   }

   void loadLineitem(Integer order_start = 1, Integer order_end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      loadLineitem([&](const lineitem_t::Key& k, const lineitem_t& v) { this->lineitem.insert(k, v); }, order_start, order_end);
   }

   void loadCustomer(std::function<void(const customerh_t::Key&, const customerh_t&)> insert_func, Integer start, Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         insert_func(customerh_t::Key{i}, customerh_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("customer", i, start, end);
      }
      last_customer_id = end;
   }

   void loadCustomer(Integer start = 1, Integer end = CUSTOMER_SCALE * FLAGS_tpch_scale_factor)
   {
      loadCustomer([this](const customerh_t::Key& k, const customerh_t& v) { this->customer.insert(k, v); }, start, end);
   }

   void loadOrders(std::function<void(const orders_t::Key&, const orders_t&)> insert_func, Integer start, Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         insert_func(orders_t::Key{i}, orders_t::generateRandomRecord([this]() { return this->getCustomerID(); }));
         printProgress("orders", i, start, end);
      }
      last_order_id = end;
   }

   void loadOrders(Integer start = 1, Integer end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      loadOrders([this](const orders_t::Key& k, const orders_t& v) { this->orders.insert(k, v); }, start, end);
   }

   void loadNation(std::function<void(const nation_t::Key&, const nation_t&)> insert_func)
   {
      for (Integer i = 1; i <= NATION_COUNT; i++) {
         insert_func(nation_t::Key{i}, nation_t::generateRandomRecord([this]() { return this->getRegionID(); }));
         printProgress("nation", i, 1, NATION_COUNT);
      }
   }

   void loadNation()
   {
      loadNation([this](const nation_t::Key& k, const nation_t& v) { this->nation.insert(k, v); });
   }

   void loadRegion(std::function<void(const region_t::Key&, const region_t&)> insert_func)
   {
      for (Integer i = 1; i <= REGION_COUNT; i++) {
         insert_func(region_t::Key{i}, region_t::generateRandomRecord());
         printProgress("region", i, 1, REGION_COUNT);
      }
   }

   void loadRegion()
   {
      loadRegion([this](const region_t::Key& k, const region_t& v) { this->region.insert(k, v); });
   }

   void log_sizes()
   {
      std::map<std::string, double> sizes = {{"part", part.size()},         {"supplier", supplier.size()}, {"partsupp", partsupp.size()},
                                             {"customer", customer.size()}, {"orders", orders.size()},     {"lineitem", lineitem.size()},
                                             {"nation", nation.size()},     {"region", region.size()}};
      logger.log_sizes(sizes);
   }

   static void inspect_produced(const std::string& msg, long& produced)
   {
      if (produced % 100 == 0) {
         std::cout << "\r" << msg << (double)produced / 1000 << "k------------------------------------";
      }
      produced++;
   }
};
