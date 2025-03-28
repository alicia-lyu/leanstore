#include <chrono>
#include <optional>
#include <vector>
#include "Logger.hpp"
#include "Merge.hpp"
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
      auto start = std::chrono::high_resolution_clock::now();
      joinedPPsL.scan(
          {},
          [&](const auto&, const auto&) {
             inspect_produced("Enumerating materialized view: ");
             return true;
          },
          [&]() {});
      std::cout << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "query-view");
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
      logger.log(merged_t, "query-merged");
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
      logger.log(index_t, "query-base");

      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
   }

   void maintain()
   {
      maintainBase();
      maintainMerged();
      maintainView();
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
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();

      // sortedLineitem and all base tables cannot be replaced by this view
      std::vector<std::tuple<part_t::Key, part_t>> new_part;
      std::vector<std::tuple<partsupp_t::Key, partsupp_t>> new_partupp;
      std::vector<std::tuple<merged_lineitem_t::Key, merged_lineitem_t>> new_lineitems;
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { this->orders.insert(k, v); },
                       [&, this](const lineitem_t::Key& k, const lineitem_t& v) {
                          this->lineitem.insert(k, v);
                          merged_lineitem_t::Key k_new(JKBuilder<PPsL_JK>::create(k, v), k);
                          merged_lineitem_t v_new(v);
                          this->sortedLineitem.insert(k_new, v_new);
                          new_lineitems.push_back({k_new, v_new});
                       },
                       [&, this](const part_t::Key& k, const part_t& v) {
                          this->part.insert(k, v);
                          new_part.push_back({k, v});
                       },
                       [&, this](const partsupp_t::Key& k, const partsupp_t& v) {
                          this->partsupp.insert(k, v);
                          new_partupp.push_back({k, v});
                       },
                       "view");
      // sort deltas
      auto compare = [](const auto& a, const auto& b) {
         return JKBuilder<PPsL_JK>::create(std::get<0>(a), std::get<1>(a)) < JKBuilder<PPsL_JK>::create(std::get<0>(b), std::get<1>(b));
      };
      std::sort(new_part.begin(), new_part.end(), compare);
      std::sort(new_partupp.begin(), new_partupp.end(), compare);
      std::sort(new_lineitems.begin(), new_lineitems.end(), compare);
      using Merge = MultiWayMerge<PPsL_JK, joinedPPsL_t, part_t, partsupp_t, merged_lineitem_t>;
      auto part_it = new_part.begin();
      auto partsupp_it = new_partupp.begin();
      auto lineitems_it = new_lineitems.begin();
      PPsL_JK next_jk = JKBuilder<PPsL_JK>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      auto part_delta_src = [&]() {
         if (part_it == new_part.end()) {
            return typename Merge::HeapEntry();
         }
         auto& [k, v] = *part_it;
         part_it++;
         auto jk = JKBuilder<PPsL_JK>::create(k, v);
         next_jk = jk;
         return typename Merge::HeapEntry(jk, part_t::toBytes(k), part_t::toBytes(v), 0);
      };
      auto partsupp_delta_src = [&]() {
         if (partsupp_it == new_partupp.end()) {
            return typename Merge::HeapEntry();
         }
         auto& [k, v] = *partsupp_it;
         partsupp_it++;
         auto jk = JKBuilder<PPsL_JK>::create(k, v);
         next_jk = jk;
         return typename Merge::HeapEntry(jk, partsupp_t::toBytes(k), partsupp_t::toBytes(v), 1);
      };
      auto lineitem_delta_src = [&]() {
         if (lineitems_it == new_lineitems.end()) {
            return typename Merge::HeapEntry();
         }
         auto& [k, v] = *lineitems_it;
         lineitems_it++;
         auto jk = JKBuilder<PPsL_JK>::create(k, v);
         return typename Merge::HeapEntry(jk, merged_lineitem_t::toBytes(k), merged_lineitem_t::toBytes(v), 2);
      };

      PPsL_JK last_accessed_jk = next_jk;
      auto part_src = [&next_jk, &last_accessed_jk, this]() {
         while (true) {
            auto kv = part.next();
            if (kv == std::nullopt) {
               return typename Merge::HeapEntry();
            }
            auto& [k, v] = *kv;
            auto jk = JKBuilder<PPsL_JK>::create(k, v);
            if (last_accessed_jk.match(jk) != 0  // Scanned to a new jk
                && next_jk.match(jk) > 0)        // this new jk is not in the deltas, deltas have advanced to a larger jk
            {
               part.seek(part_t::Key({next_jk.l_partkey}));
               last_accessed_jk = next_jk;
               continue;
            }
            last_accessed_jk = jk;
            return typename Merge::HeapEntry(jk, merged_lineitem_t::toBytes(k), merged_lineitem_t::toBytes(v),
                                             0);  // not guaranteed to match the deltas but such waste is limited
         }
      };

      auto partsupp_src = [&next_jk, &last_accessed_jk, this]() {
         while (true) {
            auto kv = partsupp.next();
            if (kv == std::nullopt) {
               return typename Merge::HeapEntry();
            }
            auto& [k, v] = *kv;
            auto jk = JKBuilder<PPsL_JK>::create(k, v);
            if (last_accessed_jk.match(jk) != 0  // Scanned to a new jk
                && next_jk.match(jk) > 0)        // this new jk is not in the deltas, deltas have advanced to a larger jk
            {
               partsupp.seek(partsupp_t::Key({next_jk.l_partkey, next_jk.l_partsuppkey}));
               last_accessed_jk = next_jk;
               continue;
            }
            last_accessed_jk = jk;
            return typename Merge::HeapEntry(jk, merged_lineitem_t::toBytes(k), merged_lineitem_t::toBytes(v),
                                             1);  // not guaranteed to match the deltas but such waste is limited
         }
      };

      auto lineitem_src = [&next_jk, &last_accessed_jk, this]() {
         while (true) {
            auto kv = sortedLineitem.next();
            if (kv == std::nullopt) {
               return typename Merge::HeapEntry();
            }
            auto& [k, v] = *kv;
            auto jk = JKBuilder<PPsL_JK>::create(k, v);
            if (last_accessed_jk.match(jk) != 0  // Scanned to a new jk
                && next_jk.match(jk) > 0)        // this new jk is not in the deltas, deltas have advanced to a larger jk
            {
               sortedLineitem.seek(merged_lineitem_t::Key(next_jk, lineitem_t::Key({0, 0})));
               last_accessed_jk = next_jk;
               continue;
            }
            last_accessed_jk = jk;
            return typename Merge::HeapEntry(jk, merged_lineitem_t::toBytes(k), merged_lineitem_t::toBytes(v),
                                             2);  // not guaranteed to match the deltas but such waste is limited
         }
      };

      // Step 1 Join deltas
      std::cout << "Size: " << new_part.size() << " parts, " << new_partupp.size() << " partsupps, " << new_lineitems.size() << " lineitems" << std::endl;
      std::vector<std::function<typename Merge::HeapEntry()>> sources1 = {part_delta_src, partsupp_delta_src, lineitem_delta_src};
      Merge delta_join1(sources1);
      delta_join1.run();
      // Step 2 join 2 deltas and search for matches in a base table
      part_it = new_part.begin();
      partsupp_it = new_partupp.begin();
      sortedLineitem.resetIterator();
      next_jk = JKBuilder<PPsL_JK>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      std::vector<std::function<typename Merge::HeapEntry()>> sources2_1 = {part_delta_src, partsupp_delta_src, lineitem_src};
      Merge delta_join2_1(sources2_1);
      delta_join2_1.run();

      // TODO: change order of record types
      part_it = new_part.begin();
      lineitems_it = new_lineitems.begin();
      partsupp.resetIterator();
      next_jk = JKBuilder<PPsL_JK>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      std::vector<std::function<typename Merge::HeapEntry()>> sources2_2 = {part_delta_src, lineitem_delta_src, partsupp_src};
      Merge delta_join2_2(sources2_2);
      delta_join2_2.run();

      partsupp_it = new_partupp.begin();
      lineitems_it = new_lineitems.begin();
      part.resetIterator();
      next_jk = JKBuilder<PPsL_JK>::create(std::get<0>(*partsupp_it), std::get<1>(*partsupp_it));
      std::vector<std::function<typename Merge::HeapEntry()>> sources2_3 = {partsupp_delta_src, lineitem_delta_src, part_src};
      Merge delta_join2_3(sources2_3);
      delta_join2_3.run();

      // step 3 For 1 delta, search for matches in 2 base tables
      part_it = new_part.begin();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
      next_jk = JKBuilder<PPsL_JK>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      std::vector<std::function<typename Merge::HeapEntry()>> sources3_1 = {part_delta_src, partsupp_src, lineitem_src};
      Merge delta_join3_1(sources3_1);
      delta_join3_1.run();

      partsupp_it = new_partupp.begin();
      sortedLineitem.resetIterator();
      part.resetIterator();
      next_jk = JKBuilder<PPsL_JK>::create(std::get<0>(*partsupp_it), std::get<1>(*partsupp_it));
      std::vector<std::function<typename Merge::HeapEntry()>> sources3_2 = {partsupp_delta_src, lineitem_src, part_src};
      Merge delta_join3_2(sources3_2);
      delta_join3_2.run();

      lineitems_it = new_lineitems.begin();
      part.resetIterator();
      partsupp.resetIterator();
      next_jk = JKBuilder<PPsL_JK>::create(std::get<0>(*lineitems_it), std::get<1>(*lineitems_it));
      std::vector<std::function<typename Merge::HeapEntry()>> sources3_3 = {lineitem_delta_src, partsupp_src, part_src};
      Merge delta_join3_3(sources3_3);
      delta_join3_3.run();

      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();

      auto end = std::chrono::high_resolution_clock::now();

      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

      logger.log(t, "view");
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
      // 1 new part & several partsupp
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
      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();

      using Merge = MultiWayMerge<PPsL_JK, joinedPPsL_t, part_t, partsupp_t, merged_lineitem_t>;
      Merge multiway_merge(joinedPPsL, part, partsupp, sortedLineitem);
      multiway_merge.run();
      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
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
      std::filesystem::create_directories(FLAGS_csv_path);
      size_csv.open(FLAGS_csv_path + "/size.csv", std::ios::app);
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