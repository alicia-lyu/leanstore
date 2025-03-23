#pragma once
#include <gflags/gflags.h>
#include <algorithm>
#include <fstream>
#include <functional>
#include <vector>
#include <queue>

#include "Tables.hpp"

#include "Units.hpp"

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
       : 
         part(p),
         supplier(s),
         partsupp(ps),
         customer(c),
         orders(o),
         lineitem(l),
         nation(n),
         region(r),
         logger(logger)
   {  }

  private:
   static constexpr Integer PART_SCALE = 200000;
   static constexpr Integer SUPPLIER_SCALE = 10000;
   static constexpr Integer CUSTOMER_SCALE = 150000;
   static constexpr Integer ORDERS_SCALE = 1500000;
   static constexpr Integer LINEITEM_SCALE = 6000000;
   static constexpr Integer PARTSUPP_SCALE = 800000;
   static constexpr Integer NATION_COUNT = 25;
   static constexpr Integer REGION_COUNT = 5;

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

   void prepare()
   {
      logger.prepare();
   }

   void printProgress(std::string msg, Integer i, Integer scale)
   {
      if (i % 1000 == 1 || i == scale) {
         double progress = (double)i / scale * 100;
         std::cout << "\rLoading " << msg << ": " << progress << "%------------------------------------";
      }
      if (i == scale) {
         std::cout << std::endl;
      }
   }

   void loadPart()
   {
      for (Integer i = 1; i <= PART_SCALE * FLAGS_tpch_scale_factor; i++) {
         // std::cout << "partkey: " << i << std::endl;
         part.insert(part_t::Key({i}), part_t::generateRandomRecord());
         printProgress("part", i, PART_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadSupplier()
   {
      for (Integer i = 1; i <= SUPPLIER_SCALE * FLAGS_tpch_scale_factor; i++) {
         supplier.insert(supplier_t::Key({i}), supplier_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("supplier", i, SUPPLIER_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadPartsuppLineitem()
   {
      // Generate and shuffle lineitem keys
      std::vector<lineitem_t::Key> lineitems = {};
      for (Integer i = 1; i <= ORDERS_SCALE * FLAGS_tpch_scale_factor; i++) {
         Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2);
         for (Integer j = 1; j <= lineitem_cnt; j++) {
            lineitems.push_back(lineitem_t::Key({i, j}));
         }
      }
      std::random_shuffle(lineitems.begin(), lineitems.end());
      // Load partsupp and lineitem
      long l_global_cnt = 0;
      for (Integer i = 1; i <= PART_SCALE * FLAGS_tpch_scale_factor; i++) {
         // Randomly select suppliers for this part
         Integer supplier_cnt = urand(1, PARTSUPP_SCALE / PART_SCALE * 2);
         std::vector<Integer> suppliers = {};
         while (true) {
            Integer supplier_id = urand(1, SUPPLIER_SCALE * FLAGS_tpch_scale_factor);
            if (std::find(suppliers.begin(), suppliers.end(), supplier_id) == suppliers.end()) {
               suppliers.push_back(supplier_id);
            }
            if (suppliers.size() == supplier_cnt) {
               break;
            }
         }
         for (auto& s : suppliers) {
            // load 1 partsupp
            partsupp.insert(partsupp_t::Key({i, s}), partsupp_t::generateRandomRecord());
            // load lineitems
            Integer lineitem_cnt = urand(0, LINEITEM_SCALE / PARTSUPP_SCALE * 2); // No reference integrity but mostly matched
            for (Integer l = 0; l < lineitem_cnt; l++) {
               auto rec = lineitem_t::generateRandomRecord([i]() { return i; }, [s]() { return s; });
               if (l_global_cnt >= lineitems.size()) {
                  std::cout << "Warning: lineitem table is not fully populated" << std::endl;
                  break;
               } else {
                  lineitem.insert(lineitems[l_global_cnt], rec);
                  l_global_cnt++;
               }
            }
         }
         printProgress("partsupp and lineitem", i, PART_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadCustomer()
   {
      for (Integer i = 1; i <= CUSTOMER_SCALE * FLAGS_tpch_scale_factor; i++) {
         customer.insert(customerh_t::Key({i}), customerh_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("customer", i, CUSTOMER_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadOrders()
   {
      for (Integer i = 1; i <= ORDERS_SCALE * FLAGS_tpch_scale_factor; i++) {
         orders.insert(orders_t::Key({i}), orders_t::generateRandomRecord([this]() { return this->getCustomerID(); }));
         printProgress("orders", i, ORDERS_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadNation()
   {
      for (Integer i = 1; i <= NATION_COUNT; i++) {
         nation.insert(nation_t::Key({i}), nation_t::generateRandomRecord([this]() { return this->getRegionID(); }));
         printProgress("nation", i, NATION_COUNT);
      }
   }

   void loadRegion()
   {
      for (Integer i = 1; i <= REGION_COUNT; i++) {
         region.insert(region_t::Key({i}), region_t::generateRandomRecord());
         printProgress("region", i, REGION_COUNT);
      }
   }

   // ------------------------------------LOAD VIEWS-------------------------------------------------

   template <typename JK>
   struct HeapEntry {
      JK jk;
      std::vector<std::byte> k;
      std::vector<std::byte> v;
      u8 source;

      bool operator>(const HeapEntry& other) const { return jk > other.jk; }
   };

   template <typename JK>
   static void heapMerge(std::vector<std::function<HeapEntry<JK>()>> sources, std::vector<std::function<void(HeapEntry<JK>&)>> consumes)
   {  
      std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<HeapEntry<JK>>> heap;
      for (auto& s: sources) {
         HeapEntry<JK> entry = s();
         if (entry.jk == JK::max()) {
            std::cout << "Warning: source is empty" << std::endl;
            continue;
         }
         heap.push(entry);
      }
      while (!heap.empty()) {
         HeapEntry<JK> entry = heap.top();
         heap.pop();
         consumes[entry.source](entry);
         HeapEntry<JK> next = sources[entry.source]();
         if (next.jk != JK::max()) {
            heap.push(next);
         }
      }
   }

   void loadBasicGroup();

   void loadBasicJoinGroup();

   // Log size
   void logSize()
   {
      std::cout << "Logging size" << std::endl;
      std::ofstream size_csv;
      size_csv.open(FLAGS_csv_path + "/size.csv", std::ios::app);
      if (size_csv.tellp() == 0) {
         size_csv << "table,size (MiB)" << std::endl;
      }
      std::cout << "table,size" << std::endl;
      std::vector<std::ostream*> out = {&std::cout, &size_csv};
      for (std::ostream* o: out) {
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
