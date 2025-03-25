#pragma once
#include <gflags/gflags.h>
#include <algorithm>
#include <fstream>
#include <functional>
#include <limits>
#include <optional>
#include <set>
#include <vector>
#include <queue>
#include <future>

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
         logger(logger),
         last_part_id(0),
         last_supplier_id(0),
         last_customer_id(0),
         last_order_id(0)
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

   void prepare()
   {
      logger.prepare();
   }

   void printProgress(std::string msg, Integer i, Integer scale)
   {
      if (i % 1000 == 1 || i == scale) {
         double progress = (double)i / scale * 100;
         std::cout << "\rLoading " << scale << " " << msg << ": " << progress << "%------------------------------------";
      }
      if (i == scale) {
         std::cout << std::endl;
      }
   }

   void loadPart(Integer start = 1, Integer end = PART_SCALE * FLAGS_tpch_scale_factor)
   {
      for (Integer i = start; i <= end; i++) {
         // std::cout << "partkey: " << i << std::endl;
         part.insert(part_t::Key({i}), part_t::generateRandomRecord());
         printProgress("part", start, end);
      }
      last_part_id = end;
   }

   void loadSupplier(Integer start = 1, Integer end = SUPPLIER_SCALE * FLAGS_tpch_scale_factor)
   {
      for (Integer i = start; i <= end; i++) {
         supplier.insert(supplier_t::Key({i}), supplier_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("supplier", i, end);
      }
      last_supplier_id = end;
   }

   void loadPartsuppLineitem(Integer part_start = 1, Integer part_end = PART_SCALE * FLAGS_tpch_scale_factor, Integer order_start = 1, Integer order_end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      // Generate and shuffle lineitem keys
      std::vector<lineitem_t::Key> lineitem_keys = {};
      for (Integer i = order_start; i <= order_end; i++) {
         Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2);
         for (Integer j = 1; j <= lineitem_cnt; j++) {
            lineitem_keys.push_back(lineitem_t::Key({i, j}));
         }
      }
      std::random_shuffle(lineitem_keys.begin(), lineitem_keys.end());
      // Load partsupp and lineitem
      long l_global_cnt = 0;
      const Integer partsupp_size = (PARTSUPP_SCALE / PART_SCALE) * (part_end - part_start);
      std::cout << "Lineitem size: " << lineitem_keys.size() << ", partsupp size: " << partsupp_size << std::endl;
      for (Integer i = part_start; i <= part_end; i++) {
         // Randomly select suppliers for this part
         Integer supplier_cnt = urand(1, PARTSUPP_SCALE / PART_SCALE * 2);
         std::set<Integer> suppliers = {};
         while (suppliers.size() < supplier_cnt) {
            Integer supplier_id = urand(1, last_supplier_id);
            suppliers.insert(supplier_id);
         }
         for (auto& s : suppliers) {
            // load 1 partsupp
            partsupp.insert(partsupp_t::Key({i, s}), partsupp_t::generateRandomRecord());
            // load lineitems
            Integer lineitem_cnt = urand(0, lineitem_keys.size() / partsupp_size * 2);
            // No reference integrity but mostly matched
            for (Integer l = 0; l < lineitem_cnt; l++) {
               auto rec = lineitem_t::generateRandomRecord([i]() { return i; }, [s]() { return s; });
               if (l_global_cnt >= lineitem_keys.size()) {
                  std::cout << "Warning: lineitem table is not fully populated" << std::endl;
                  break;
               } else {
                  lineitem.insert(lineitem_keys[l_global_cnt], rec);
                  l_global_cnt++;
               }
            }
         }
         printProgress("parts of partsupp and lineitem", i, part_end);
      }
   }

   void loadPartsupp(Integer part_start = 1, Integer part_end = PART_SCALE * FLAGS_tpch_scale_factor)
   {
      loadPartsuppLineitem(part_start, part_end, last_order_id, last_order_id);
   }

   void loadLineitem(Integer order_start, Integer order_end)
   {
      for (Integer i = order_start; i <= order_end; i++) {
         Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2);
         std::cout << "Loading " << lineitem_cnt << " lineitem for order " << i << std::endl;
         for (Integer j = 1; j <= lineitem_cnt; j++) {
            // look up partsupp
            auto p = urand(1, last_part_id);
            auto s = urand(1, last_supplier_id);
            auto start_key = partsupp_t::Key({p, s});
            partsupp.scan(start_key, [&](const partsupp_t::Key& k, const partsupp_t&) {
               p = k.ps_partkey;
               s = k.ps_suppkey;
               // LATER: each ps pair does not have uniform chance of being selected
               return false;
            }, []() {});
            lineitem.insert(
               lineitem_t::Key({i, j}),
               lineitem_t::generateRandomRecord([p]() { return p; }, [s]() { return s; }));
         }
         printProgress("orders of lineitem", i, order_end);
      }
   }

   void loadCustomer(Integer start = 1, Integer end = CUSTOMER_SCALE * FLAGS_tpch_scale_factor)
   {
      for (Integer i = start; i <= end; i++) {
         customer.insert(customerh_t::Key({i}), customerh_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("customer", i, end);
      }
      last_customer_id = end;
   }

   void loadOrders(Integer start = 1, Integer end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      for (Integer i = start; i <= end; i++) {
         orders.insert(orders_t::Key({i}), orders_t::generateRandomRecord([this]() { return this->getCustomerID(); }));
         printProgress("orders", i, end);
      }
      last_order_id = end;
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

      HeapEntry() : jk(JK::max()), k(), v(), source(std::numeric_limits<u8>::max()) {}

      HeapEntry(JK jk, std::vector<std::byte> k, std::vector<std::byte> v, u8 source) : jk(jk), k(k), v(v), source(source) {}

      bool operator>(const HeapEntry& other) const { return jk > other.jk; }
   };

   template <typename JK>
   static void heapMerge(std::vector<std::function<HeapEntry<JK>()>> sources, std::vector<std::function<void(HeapEntry<JK>&)>> consumes)
   {  
      std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<HeapEntry<JK>>> heap;
      for (auto& s: sources) {
         auto entry = s();
         if (entry.jk == JK::max()) {
            std::cout << "Warning: source is empty" << std::endl;
            continue;
         }
         heap.push(entry);
      }
      while (!heap.empty()) {
         HeapEntry<JK> entry = heap.top();
         heap.pop();
         consumes.at(entry.source)(entry);
         HeapEntry<JK> next = sources.at(entry.source)();
         if (next.jk != JK::max()) {
            heap.push(next);
         }
      }
   }

   template <typename JK, typename RecordType>
   static std::function<HeapEntry<JK>()> getHeapSource(AdapterType<RecordType>& adapter, u8 source, std::function<JK(const typename RecordType::Key&, const RecordType&)> getJK)
   {
      return [source, &adapter, getJK]() {
         auto kv = adapter.next();
         if (kv == std::nullopt) {
            return HeapEntry<JK>();
         }
         auto& [k, v] = *kv;
         JK jk = getJK(k, v);
         return HeapEntry<JK>(jk, RecordType::toBytes(k), RecordType::toBytes(v), source);
      };
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
