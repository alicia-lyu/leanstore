#include <chrono>
#include <optional>
#include <vector>
#include "Merge.hpp"
#include "Join.hpp"
#include "Logger.hpp"
#include "TPCHWorkload.hpp"
#include "Tables.hpp"
#include "Views.hpp"

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

   void queryByView()
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
   }

   void query()
   {
      // TXs: Measure end-to-end time
      queryByMerged();
      queryByIndex();
      queryByView();
   }

   // TXs: Measure end-to-end time
   void queryByMerged()
   {
      // Scan merged index + join on the fly
      logger.reset();
      auto merged_start = std::chrono::high_resolution_clock::now();

      mergedPPsL.template scanJoin<PPsL_JK, joinedPPsL_t, merged_part_t, merged_partsupp_t, merged_lineitem_t>();

      auto merged_end = std::chrono::high_resolution_clock::now();
      auto merged_t = std::chrono::duration_cast<std::chrono::microseconds>(merged_end - merged_start).count();
      logger.log(merged_t, "merged");
   }

   void queryByIndex()
   {
      logger.reset();
      auto index_start = std::chrono::high_resolution_clock::now();

      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();

      using Merge = MultiWayMerge<PPsL_JK, joinedPPsL_t, part_t, partsupp_t, merged_lineitem_t>;

      Merge multiway_merge(part, partsupp, sortedLineitem);
      multiway_merge.run();

      auto index_end = std::chrono::high_resolution_clock::now();
      auto index_t = std::chrono::duration_cast<std::chrono::microseconds>(index_end - index_start).count();
      logger.log(index_t, "index");

      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
   }

   void maintainMerged()
   {
      // sortedLineitem, part, partsupp are seen as discarded and do not contribute to database size
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { this->orders.insert(k, v); },
                       [this](const lineitem_t::Key& k, const lineitem_t& v) {
                          merged_lineitem_t::Key k_new(JKBuilder<PPsL_JK>::create(k, v), k);
                          merged_lineitem_t v_new(v);
                          mergedPPsL.insert(k_new, v_new);
                       },
                       [this](const part_t::Key& k, const part_t& v) {
                          merged_part_t::Key k_new(JKBuilder<PPsL_JK>::create(k, v), k);
                          merged_part_t v_new(v);
                          mergedPPsL.insert(k_new, v_new);
                       },
                       [this](const partsupp_t::Key& k, const partsupp_t& v) {
                          merged_partsupp_t::Key k_new(JKBuilder<PPsL_JK>::create(k, v), k);
                          merged_partsupp_t v_new(v);
                          mergedPPsL.insert(k_new, v_new);
                       },
                       "merged");
   }

   void maintainView()
   {
      // sortedLineitem and all base tables cannot be replaced by this view
      std::vector<std::tuple<part_t::Key, part_t>> new_parts;
      std::vector<std::tuple<partsupp_t::Key, partsupp_t>> new_partsupps;
      std::vector<std::tuple<lineitem_t::Key, lineitem_t>> new_lineitems;
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { this->orders.insert(k, v); },
                       [&, this](const lineitem_t::Key& k, const lineitem_t& v) {
                          this->lineitem.insert(k, v);
                          new_lineitems.push_back({k, v});
                       },
                       [&, this](const part_t::Key& k, const part_t& v) {
                          this->part.insert(k, v);
                          new_parts.push_back({k, v});
                       },
                       [&, this](const partsupp_t::Key& k, const partsupp_t& v) {
                          this->partsupp.insert(k, v);
                          new_partsupps.push_back({k, v});
                       },
                       "view");
      // Step 1 Join deltas
      // Step 2 delta join
   }

   void joinDeltas(std::vector<std::tuple<part_t::Key, part_t>>& new_parts,
                   std::vector<std::tuple<partsupp_t::Key, partsupp_t>>& new_partsupps,
                   std::vector<std::tuple<lineitem_t::Key, lineitem_t>>& new_lineitems)
   {
      


   }

   void maintainBase()
   {
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { this->orders.insert(k, v); },
                       [this](const lineitem_t::Key& k, const lineitem_t& v) { this->lineitem.insert(k, v); },
                       [this](const part_t::Key& k, const part_t& v) { this->part.insert(k, v); },
                       [this](const partsupp_t::Key& k, const partsupp_t& v) { this->partsupp.insert(k, v); }, "base");
   }

   void maintainTemplate(std::function<void(const orders_t::Key&, const orders_t&)> order_insert_func,
                         std::function<void(const lineitem_t::Key&, const lineitem_t&)> lineitem_insert_func,
                         std::function<void(const part_t::Key&, const part_t&)> part_insert_func,
                         std::function<void(const partsupp_t::Key&, const partsupp_t&)> partsupp_insert_func,
                         std::string name)
   {
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      // 100 new orders
      auto order_start = workload.last_order_id + 1;
      auto order_end = workload.last_order_id + 100;
      workload.loadOrders(order_insert_func, order_start, order_end);
      workload.loadLineitem(lineitem_insert_func, order_start, order_end);
      // 1 new part & several partsupps
      auto part_start = workload.last_part_id + 1;
      auto part_end = workload.last_part_id + 1;
      workload.loadPart(part_insert_func, part_start, part_end);
      workload.loadPartsupp(partsupp_insert_func, part_start, part_end);
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "maintain-" + name);
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
         merged_lineitem_t::Key k_new(jk, k);
         merged_lineitem_t v_new(v);
         this->sortedLineitem.insert(k_new, v_new);
      }
      this->lineitem.resetIterator();
   }

   void loadBasicJoin()
   {
      std::cout << "Loading basic join" << std::endl;
      // first join
      {
         std::cout << "Joining part and partsupp" << std::endl;
         this->part.resetIterator();
         this->partsupp.resetIterator();
         Join<PPsL_JK, joinedPPs_t, part_t, partsupp_t> join1(
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
         this->part.resetIterator();
         this->partsupp.resetIterator();
      }

      // second join
      {
         std::cout << "Joining joinedPPs and lineitem" << std::endl;
         assert(this->sortedLineitem.estimatePages() > 0);
         this->joinedPPs.resetIterator();
         this->sortedLineitem.resetIterator();
         Join<PPsL_JK, joinedPPsL_t, joinedPPs_t, merged_lineitem_t> join2(
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
         this->joinedPPs.resetIterator();
         this->sortedLineitem.resetIterator();
      }
   };

   void loadMergedBasicJoin()
   {
      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
      
      using Merge = MultiWayMerge<PPsL_JK, joinedPPsL_t, merged_part_t, merged_partsupp_t, merged_lineitem_t>;

      Merge multiway_merge(mergedPPsL, part, partsupp, sortedLineitem);
      multiway_merge.run();

      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
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