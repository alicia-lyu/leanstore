#pragma once
#include <gflags/gflags.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <optional>
#include <vector>
#include <queue>

#include "Tables.hpp"

#include "Units.hpp"
#include "Views.hpp"

#include "Join.hpp"
#include "Logger.hpp"
#include "leanstore/Config.hpp"


DEFINE_double(tpch_scale_factor, 1, "TPC-H scale factor");

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
   // TODO: Views
   AdapterType<joinedPPsL_t>& joinedPPsL;
   AdapterType<joinedPPs_t>& joinedPPs;
   AdapterType<merged_lineitem_t>& sortedLineitem;
   MergedAdapterType& mergedPPsL;

  public:
   TPCHWorkload(AdapterType<part_t>& p,
                AdapterType<supplier_t>& s,
                AdapterType<partsupp_t>& ps,
                AdapterType<customerh_t>& c,
                AdapterType<orders_t>& o,
                AdapterType<lineitem_t>& l,
                AdapterType<nation_t>& n,
                AdapterType<region_t>& r,
                MergedAdapterType& mbj,
                AdapterType<joinedPPsL_t>& jppsl,
                AdapterType<joinedPPs_t>& jpps,
                AdapterType<merged_lineitem_t>& sl,
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
         joinedPPsL(jppsl),
         joinedPPs(jpps),
         mergedPPsL(mbj),
         sortedLineitem(sl),
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
   // TXs: Measure end-to-end time
   void basicJoin()
   {
      // Enumrate materialized view
      logger.reset();
      [[maybe_unused]] long produced = 0;
      auto inspect_produced = [&](const std::string& msg) {
         if (produced % 100 == 0) {
            std::cout << "\r" << msg << (double) produced / 1000 << "k------------------------------------";
         }
         produced++;
      };
      auto mtdv_start = std::chrono::high_resolution_clock::now();
      joinedPPsL.scan(
          {},
          [&](const auto&, const auto&) {
             inspect_produced("Enumerating materialized view: ");
             return true;
          },
          [&]() {});
      std::cout << std::endl;
      auto mtdv_end = std::chrono::high_resolution_clock::now();
      auto mtdv_t = std::chrono::duration_cast<std::chrono::microseconds>(mtdv_end - mtdv_start).count();
      logger.log(mtdv_t, "mtdv");

      // Scan merged index + join on the fly
      logger.reset();
      auto merged_start = std::chrono::high_resolution_clock::now();

      mergedPPsL.template scanJoin<PPsL_JK, joinedPPsL_t, merged_part_t, merged_partsupp_t, merged_lineitem_t>();
      
      auto merged_end = std::chrono::high_resolution_clock::now();
      auto merged_t = std::chrono::duration_cast<std::chrono::microseconds>(merged_end - merged_start).count();
      logger.log(merged_t, "merged");
   }

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

   void loadSortedLineitem()
   {
      // sort lineitem
      this->lineitem.resetIterator();
      while (true) {
         auto kv = this->lineitem.next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         PPsL_JK jk{v.l_partkey, v.l_suppkey};
         merged_lineitem_t::Key k_new({jk, k});
         merged_lineitem_t v_new(v);
         this->sortedLineitem.insert(k_new, v_new);
      }
   }

   void loadBasicJoin()
   {
      std::cout << "Loading basic join" << std::endl;
      // first join
      {
         std::cout << "Joining part and partsupp" << std::endl;
         this->part.resetIterator();
         this->partsupp.resetIterator();
         Join<PPsL_JK, joinedPPs_t, part_t::Key, part_t, partsupp_t::Key, partsupp_t> join1(
             [](part_t::Key& k, part_t&) { return PPsL_JK{k.p_partkey, 0}; },
             [](partsupp_t::Key& k, partsupp_t&) { return PPsL_JK{k.ps_partkey, k.ps_suppkey}; },
             [](u8* in, u16) {
                part_t::Key k;
                part_t::unfoldKey(in, k);
                return k;
             },
             [](u8* in, u16) {
                partsupp_t::Key k;
                partsupp_t::unfoldKey(in, k);
                return k;
             },
             [this]() { return this->part.next(); }, [this]() { return this->partsupp.next(); });
         while (true) {
            auto kv = join1.next();
            if (kv == std::nullopt) {
               break;
            }
            auto& [k, v] = *kv;
            joinedPPs.insert(k, v);
         }
      }
      
      // second join
      {
         std::cout << "Joining joinedPPs and lineitem" << std::endl;
         assert(this->sortedLineitem.estimatePages() > 0);
         this->joinedPPs.resetIterator();
         this->sortedLineitem.resetIterator();
         Join<PPsL_JK, joinedPPsL_t, joinedPPs_t::Key, joinedPPs_t, merged_lineitem_t::Key, merged_lineitem_t> join2(
            [](joinedPPs_t::Key& k, joinedPPs_t&) { return k.jk; }, [](merged_lineitem_t::Key& k, merged_lineitem_t&) { return k.jk; },
            [](u8* in, u16) {
               joinedPPs_t::Key k;
               joinedPPs_t::unfoldKey(in, k);
               return k;
            },
            [](u8* in, u16) {
               merged_lineitem_t::Key k;
               merged_lineitem_t::unfoldKey(in, k);
               return k;
            },
            [this]() { return this->joinedPPs.next(); }, [this]() { return this->sortedLineitem.next(); });
         while (true) {
            auto kv = join2.next();
            if (kv == std::nullopt) {
               break;
            }
            auto& [k, v] = *kv;
            joinedPPsL.insert(k, v);
         }
      }
   };

   template <typename JK>
   struct HeapEntry {
      JK jk;
      std::vector<std::byte> k;
      std::vector<std::byte> v;
      u8 source;

      bool operator>(const HeapEntry& other) const { return jk > other.jk; }
   };

   template <typename JK>
   void heapMerge(std::vector<std::function<HeapEntry<JK>()>> sources, std::vector<std::function<void(HeapEntry<JK>&)>> consumes)
   {  
      std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<HeapEntry<JK>>> heap;
      for (auto& s: sources) {
         HeapEntry<JK> entry = s();
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

   void loadMergedBasicJoin()
   {
      auto part_src = [this]() {
         auto kv = part.next();
         if (kv == std::nullopt) {
            return HeapEntry<PPsL_JK>{PPsL_JK::max(), {}, {}, 0};
         }
         auto& [k, v] = *kv;
         std::vector<std::byte> k_bytes(sizeof(k));
         std::memcpy(k_bytes.data(), &k, sizeof(k));

         std::vector<std::byte> v_bytes(sizeof(v));
         std::memcpy(v_bytes.data(), &v, sizeof(v));
         return HeapEntry<PPsL_JK>{PPsL_JK{k.p_partkey, 0}, std::move(k_bytes), std::move(v_bytes), 0};
      };

      auto part_consume = [this](HeapEntry<PPsL_JK>& entry) {
         merged_part_t::Key k_new({entry.jk, bytes_to_struct<part_t::Key>(entry.k)});
         merged_part_t v_new(bytes_to_struct<part_t>(entry.v));
         this->mergedPPsL.insert(k_new, v_new);
      };

      auto partsupp_src = [this]() {
         auto kv = partsupp.next();
         if (kv == std::nullopt) {
            return HeapEntry<PPsL_JK>{PPsL_JK::max(), {}, {}, 1};
         }
         auto& [k, v] = *kv;
         std::vector<std::byte> k_bytes(sizeof(k));
         std::memcpy(k_bytes.data(), &k, sizeof(k));

         std::vector<std::byte> v_bytes(sizeof(v));
         std::memcpy(v_bytes.data(), &v, sizeof(v));
         return HeapEntry<PPsL_JK>{PPsL_JK{k.ps_partkey, k.ps_suppkey}, std::move(k_bytes), std::move(v_bytes), 1};
      };

      auto partsupp_consume = [this](HeapEntry<PPsL_JK>& entry) {
         merged_partsupp_t::Key k_new({entry.jk, bytes_to_struct<partsupp_t::Key>(entry.k)});
         merged_partsupp_t v_new(bytes_to_struct<partsupp_t>(entry.v));
         this->mergedPPsL.insert(k_new, v_new);
      };

      auto lineitem_src = [this]() {
         auto kv = sortedLineitem.next();
         if (kv == std::nullopt) {
            return HeapEntry<PPsL_JK>{PPsL_JK::max(), {}, {}, 2};
         }
         auto& [k, v] = *kv;
         std::vector<std::byte> k_bytes(sizeof(k));
         std::memcpy(k_bytes.data(), &k, sizeof(k));
         std::vector<std::byte> v_bytes(sizeof(v));
         std::memcpy(v_bytes.data(), &v, sizeof(v));
         return HeapEntry<PPsL_JK>{PPsL_JK{k.jk.l_partkey, k.jk.l_partsuppkey}, std::move(k_bytes), std::move(v_bytes), 2};
      };

      auto lineitem_consume = [this](HeapEntry<PPsL_JK>& entry) {
         merged_lineitem_t::Key k_new = bytes_to_struct<merged_lineitem_t::Key>(entry.k);
         merged_lineitem_t v_new = bytes_to_struct<merged_lineitem_t>(entry.v);
         this->mergedPPsL.insert(k_new, v_new);
      };
      
      heapMerge<PPsL_JK>({part_src, partsupp_src, lineitem_src}, {part_consume, partsupp_consume, lineitem_consume});
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
         *o << "joinedPPsL," << joinedPPsL.size() << std::endl;
         *o << "joinedPPs," << joinedPPs.size() << std::endl;
         *o << "sortedLineitem," << sortedLineitem.size() << std::endl;
         *o << "mergedPPsL," << mergedPPsL.size() << std::endl;
      }
      size_csv.close();
   }
};
