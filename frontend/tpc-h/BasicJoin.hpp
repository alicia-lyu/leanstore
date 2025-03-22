#include "Join.hpp"
#include "Logger.hpp"
#include "TPCHWorkload.hpp"
#include "Tables.hpp"
#include "Views.hpp"
#include <chrono>

template <template <typename> class AdapterType, class MergedAdapterType>
class BasicJoin
{
   using TPCH = TPCHWorkload<AdapterType, MergedAdapterType>;
   TPCH& workload;
   AdapterType<joinedPPsL_t>& joinedPPsL;
   AdapterType<joinedPPs_t>& joinedPPs;
   AdapterType<merged_lineitem_t>& sortedLineitem;
   MergedAdapterType& mergedPPsL;
   Logger& logger;
   AdapterType<part_t>& part;
   AdapterType<supplier_t>& supplier;
   AdapterType<partsupp_t>& partsupp;
   AdapterType<customerh_t>& customer;
   AdapterType<orders_t>& orders;
   AdapterType<lineitem_t>& lineitem;
   AdapterType<nation_t>& nation;
   AdapterType<region_t>& region;
   template <typename JK>
   using HeapEntry = typename TPCH::template HeapEntry<JK>;

  public:
   BasicJoin(TPCH& workload,
             MergedAdapterType& mbj,
             AdapterType<joinedPPsL_t>& jppsl,
             AdapterType<joinedPPs_t>& jpps,
             AdapterType<merged_lineitem_t>& sl)
       : mergedPPsL(mbj),
         joinedPPsL(jppsl),
         joinedPPs(jpps),
         sortedLineitem(sl),
         workload(workload),
         logger(workload.logger),
         part(workload.part),
         supplier(workload.supplier),
         partsupp(workload.partsupp),
         customer(workload.customer),
         orders(workload.orders),
         lineitem(workload.lineitem),
         nation(workload.nation),
         region(workload.region)
   {
   }

   // TXs: Measure end-to-end time
   void basicJoin()
   {
      // Enumrate materialized view
      logger.reset();
      [[maybe_unused]] long produced = 0;
      auto inspect_produced = [&](const std::string& msg) {
         if (produced % 100 == 0) {
            std::cout << "\r" << msg << (double)produced / 1000 << "k------------------------------------";
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

   void loadBaseTables()
    {
        workload.loadPart();
        workload.loadSupplier();
        workload.loadCustomer();
        workload.loadOrders();
        workload.loadPartsuppLineitem();
        workload.loadNation();
        workload.loadRegion();
        workload.logSize();
    }

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

      TPCH::template heapMerge<PPsL_JK>({part_src, partsupp_src, lineitem_src}, {part_consume, partsupp_consume, lineitem_consume});
   }

   void logSize()
   {
      std::cout << "Logging size" << std::endl;
      std::ofstream size_csv;
      size_csv.open(FLAGS_csv_path + "basic_join/size.csv", std::ios::app);
      if (size_csv.tellp() == 0) {
         size_csv << "table,size (MiB)" << std::endl;
      }
      std::cout << "table,size" << std::endl;
      std::vector<std::ostream*> out = {&std::cout, &size_csv};
      for (std::ostream* o : out) {
         *o << "joinedPPsL," << joinedPPsL.size() << std::endl;
         *o << "joinedPPs," << joinedPPs.size() << std::endl;
         *o << "sortedLineitem," << sortedLineitem.size() << std::endl;
         *o << "mergedPPsL," << mergedPPsL.size() << std::endl;
      }
      size_csv.close();
   }
};