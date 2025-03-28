#pragma once
#include <gflags/gflags.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <set>
#include <vector>

#include "Tables.hpp"

#include "Logger.hpp"
#include "leanstore/Config.hpp"

DECLARE_double(tpch_scale_factor);

template <template <typename> class AdapterType, class MergedAdapterType>
class BasicJoin;

template <template <typename> class AdapterType, class MergedAdapterType>
class TPCHWorkload
{
   friend class BasicJoin<AdapterType, MergedAdapterType>;

  private:
   Logger& logger;
   AdapterType<part_t>& part;
   AdapterType<supplier_t>& supplier;
   AdapterType<partsupp_t>& partsupp;
   AdapterType<customerh_t>& customer;
   AdapterType<orders_t>& orders;
   AdapterType<lineitem_t>& lineitem;
   AdapterType<nation_t>& nation;
   AdapterType<region_t>& region;

  public:
   TPCHWorkload(AdapterType<part_t>& p,
                AdapterType<supplier_t>& s,
                AdapterType<partsupp_t>& ps,
                AdapterType<customerh_t>& c,
                AdapterType<orders_t>& o,
                AdapterType<lineitem_t>& l,
                AdapterType<nation_t>& n,
                AdapterType<region_t>& r,
                Logger& logger)
       : part(p),
         supplier(s),
         partsupp(ps),
         customer(c),
         orders(o),
         lineitem(l),
         nation(n),
         region(r),
         logger(logger),
         last_part_id(0),
         last_supplier_id(0),
         last_customer_id(0),
         last_order_id(0)
   {
   }

  private:
   static constexpr Integer PART_SCALE = 200000;
   static constexpr Integer SUPPLIER_SCALE = 10000;
   static constexpr Integer CUSTOMER_SCALE = 150000;
   static constexpr Integer ORDERS_SCALE = 1500000;
   static constexpr Integer LINEITEM_SCALE = 6000000;
   static constexpr Integer PARTSUPP_SCALE = 800000;
   static constexpr Integer NATION_COUNT = 25;
   static constexpr Integer REGION_COUNT = 5;

   Integer last_part_id;
   Integer last_supplier_id;
   Integer last_customer_id;
   Integer last_order_id;

   inline Integer getPartID() { return urand(1, PART_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getSupplierID() { return urand(1, SUPPLIER_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getCustomerID() { return urand(1, CUSTOMER_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getOrderID() { return urand(1, ORDERS_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getNationID() { return urand(1, NATION_COUNT); }

   inline Integer getRegionID() { return urand(1, REGION_COUNT); }

  public:
   void basicGroup();

   void basicJoinGroup();

   // ------------------------------------LOAD-------------------------------------------------

   void prepare() { logger.prepare(); }

   void printProgress(std::string msg, Integer i, Integer start, Integer end)
   {
      if (i % 1000 == start % 1000 || i == end) {
         auto scale = end - start + 1;
         double progress = (double)(i - start + 1) / scale * 100;
         std::cout << "\rLoading " << scale << " " << msg << ": " << progress << "%------------------------------------";
      }
      if (i == end) {
         std::cout << std::endl;
      }
   }

   void loadPart(std::function<void(const part_t::Key&, const part_t&)> insert_func, Integer start, Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         // std::cout << "partkey: " << i << std::endl;
         insert_func(part_t::Key({i}), part_t::generateRandomRecord());
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
         insert_func(supplier_t::Key({i}), supplier_t::generateRandomRecord([this]() { return this->getNationID(); }));
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
      // Generate and shuffle lineitem keys
      std::vector<lineitem_t::Key> lineitem_keys = {};
      for (Integer i = order_start; i <= order_end; i++) {
         Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2 - 1);
         for (Integer j = 1; j <= lineitem_cnt; j++) {
            lineitem_keys.push_back(lineitem_t::Key({i, j}));
         }
         printProgress("orders of lineitem keys", i, order_start, order_end);
      }
      std::random_shuffle(lineitem_keys.begin(), lineitem_keys.end());
      // Load partsupp and lineitem
      long l_global_cnt = 0;
      const Integer partsupp_size = (PARTSUPP_SCALE / PART_SCALE) * (part_end - part_start + 1);
      for (Integer i = part_start; i <= part_end; i++) {
         // Randomly select suppliers for this part
         Integer supplier_cnt = urand(1, PARTSUPP_SCALE / PART_SCALE * 2 - 1);
         std::set<Integer> suppliers = {};
         while (suppliers.size() < supplier_cnt) {
            Integer supplier_id = urand(1, last_supplier_id);
            suppliers.insert(supplier_id);
         }
         for (auto& s : suppliers) {
            // load 1 partsupp
            ps_insert_func(partsupp_t::Key({i, s}), partsupp_t::generateRandomRecord());
            // load lineitems
            Integer lineitem_cnt = urand(0, lineitem_keys.size() / partsupp_size * 2);
            // No reference integrity but mostly matched
            for (Integer l = 0; l < lineitem_cnt; l++) {
               auto rec = lineitem_t::generateRandomRecord([i]() { return i; }, [s]() { return s; });
               if (l_global_cnt >= lineitem_keys.size()) {
                  std::cout << "Warning: lineitem table is not fully populated" << std::endl;
                  break;
               } else {
                  l_insert_func(lineitem_keys[l_global_cnt], rec);
                  l_global_cnt++;
               }
            }
         }
         printProgress("parts of partsupp and lineitem", i, part_start, part_end);
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
      loadPartsuppLineitem(insert_func, [this](const lineitem_t::Key&, const lineitem_t&) {}, part_start, part_end, last_order_id, last_order_id - 1);
   }

   void loadLineitem(std::function<void(const lineitem_t::Key&, const lineitem_t&)> insert_func, Integer order_start, Integer order_end)
   {
      for (Integer i = order_start; i <= order_end; i++) {
         Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2 - 1);
         for (Integer j = 1; j <= lineitem_cnt; j++) {
            // look up partsupp
            auto p = urand(1, last_part_id);
            auto s = urand(1, last_supplier_id);
            auto start_key = partsupp_t::Key({p, s});
            partsupp.scan(
                start_key,
                [&](const partsupp_t::Key& k, const partsupp_t&) {
                   p = k.ps_partkey;
                   s = k.ps_suppkey;
                   // LATER: each ps pair does not have uniform chance of being selected
                   return false;
                },
                []() {});
            insert_func(lineitem_t::Key({i, j}), lineitem_t::generateRandomRecord([p]() { return p; }, [s]() { return s; }));
         }
         printProgress("orders of lineitem", i, order_start, order_end);
      }
   }

   void loadLineitem(Integer order_start, Integer order_end)
   {
      loadLineitem([&](const lineitem_t::Key& k, const lineitem_t& v) { this->lineitem.insert(k, v); }, order_start, order_end);
   }

   void loadCustomer(std::function<void(const customerh_t::Key&, const customerh_t&)> insert_func, Integer start, Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         insert_func(customerh_t::Key({i}), customerh_t::generateRandomRecord([this]() { return this->getNationID(); }));
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
         insert_func(orders_t::Key({i}), orders_t::generateRandomRecord([this]() { return this->getCustomerID(); }));
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
         insert_func(nation_t::Key({i}), nation_t::generateRandomRecord([this]() { return this->getRegionID(); }));
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
         insert_func(region_t::Key({i}), region_t::generateRandomRecord());
         printProgress("region", i, 1, REGION_COUNT);
      }
   }

   void loadRegion()
   {
      loadRegion([this](const region_t::Key& k, const region_t& v) { this->region.insert(k, v); });
   }

   // ------------------------------------LOAD VIEWS-------------------------------------------------

   void loadBasicGroup();

   void loadBasicJoinGroup();

   // Log size
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
         *o << "part," << part.size() << std::endl;
         *o << "supplier," << supplier.size() << std::endl;
         *o << "partsupp," << partsupp.size() << std::endl;
         *o << "customer," << customer.size() << std::endl;
         *o << "orders," << orders.size() << std::endl;
         *o << "lineitem," << lineitem.size() << std::endl;
         *o << "nation," << nation.size() << std::endl;
         *o << "region," << region.size() << std::endl;
      }
      size_csv.close();
   }
};
