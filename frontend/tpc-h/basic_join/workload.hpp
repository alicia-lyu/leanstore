#include <chrono>
#include <optional>
#include <variant>
#include <vector>
#include "../binary_join.hpp"
#include "../logger.hpp"
#include "../merge.hpp"
#include "../tables.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

// SELECT *
// FROM Lineitem l, PartSupp ps, Part p
// WHERE l.partkey = ps.partkey AND l.partkey = p.partkey AND l.suppkey = ps.suppkey;

namespace basic_join
{
template <template <typename> class AdapterType, template <typename...> class MergedAdapterType>
class BasicJoin
{
   using TPCH = TPCHWorkload<AdapterType>;
   using merged_t = MergedAdapterType<merged_part_t, merged_partsupp_t, merged_lineitem_t>;
   TPCH& workload;
   AdapterType<joinedPPsL_t>& joinedPPsL;
   AdapterType<sorted_lineitem_t>& sortedLineitem;
   merged_t& mergedPPsL;
   Logger& logger;
   AdapterType<part_t>& part;
   AdapterType<partsupp_t>& partsupp;

  public:
   BasicJoin(TPCH& workload, merged_t& mbj, AdapterType<joinedPPsL_t>& jppsl, AdapterType<sorted_lineitem_t>& sl)
       : workload(workload),
         mergedPPsL(mbj),
         joinedPPsL(jppsl),
         sortedLineitem(sl),
         logger(workload.logger),
         part(workload.part),
         partsupp(workload.partsupp)
   {
   }

   std::tuple<Integer, Integer> pointLookupsForBase()
   {
      Integer part_rnd = workload.getPartID();
      Integer supplier_rnd = workload.getSupplierID();
      Integer part_id, supplier_id = 0;
      partsupp.scan(
          partsupp_t::Key{part_rnd, supplier_rnd},
          [&](const partsupp_t::Key& k, const partsupp_t&) {
             part_id = k.ps_partkey;
             supplier_id = k.ps_suppkey;
             return false;
          },
          []() {});
      if (part_id == 0 || supplier_id == 0) {
         partsupp.scanDesc(
             partsupp_t::Key{part_rnd, supplier_rnd},
             [&](const partsupp_t::Key& k, const partsupp_t&) {
                part_id = k.ps_partkey;
                supplier_id = k.ps_suppkey;
                return false;
             },
             []() {});
      }
      assert(part_id != 0 && supplier_id != 0);

      join_key_t jk{part_id, supplier_id};
      sortedLineitem.tryLookup(jk, [&](const sorted_lineitem_t&) {
         // std::cout << "found sorted lineitem" << std::endl;
      });
      part.lookup1(part_t::Key{part_id}, [&](const part_t&) {});

      pointLookupsOfRest(part_id, supplier_id);

      return std::make_tuple(part_id, supplier_id);
   }

   void pointLookupsOfRest(Integer, Integer supplier_id)
   {
      Integer customer_id = workload.getCustomerID();
      Integer order_id = workload.getOrderID();
      Integer nation_id = workload.getNationID();
      Integer region_id = workload.getRegionID();
      workload.supplier.lookup1(supplier_t::Key{supplier_id}, [&](const supplier_t&) {});
      workload.customer.lookup1(customerh_t::Key{customer_id}, [&](const customerh_t&) {});
      workload.orders.lookup1(orders_t::Key{order_id}, [&](const orders_t&) {});
      workload.lineitem.lookup1(lineitem_t::Key{order_id, 1}, [&](const lineitem_t&) {});
      workload.nation.lookup1(nation_t::Key{nation_id}, [&](const nation_t&) {});
      workload.region.lookup1(region_t::Key{region_id}, [&](const region_t&) {});
   }

   void pointLookupsForMerged()
   {
      Integer part_rnd = workload.getPartID();
      Integer supplier_rnd = workload.getSupplierID();
      auto merged_scanner = mergedPPsL.getScanner();
      merged_scanner->seekJK(join_key_t{part_rnd, supplier_rnd});
      Integer part_id = 0;
      Integer supplier_id = 0;
      while (part_id == 0 || supplier_id == 0) {
         auto [k, v] = merged_scanner->current().value();
         std::visit(
             [&](auto& actual_key) {  // if actual_key is merged_part_t, supplier_id = 0
                join_key_t jk = actual_key.jk;
                part_id = jk.l_partkey;
                supplier_id = jk.suppkey;
             },
             k);
         merged_scanner->next();
      }

      pointLookupsOfRest(part_id, supplier_id);
   }

   void pointLookupsForView()
   {
      auto [part_id, supplier_id] = pointLookupsForBase();
      joinedPPsL.tryLookup(join_key_t{part_id, supplier_id}, [&](const auto&) {});
   }

   void queryByView()
   {
      // Enumrate materialized view
      logger.reset();
      std::cout << "BasicJoin::queryByView()" << std::endl;
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

   // TXs: Measure end-to-end time
   void queryByMerged()
   {
      // Scan merged index + join on the fly
      logger.reset();
      std::cout << "BasicJoin::queryByMerged()" << std::endl;
      auto merged_start = std::chrono::high_resolution_clock::now();

      auto merged_scanner = mergedPPsL.getScanner();

      merged_scanner->template scanJoin<join_key_t, joinedPPsL_t>();

      auto merged_end = std::chrono::high_resolution_clock::now();
      auto merged_t = std::chrono::duration_cast<std::chrono::microseconds>(merged_end - merged_start).count();
      logger.log(merged_t, "query-merged");
   }

   void queryByBase()
   {
      logger.reset();
      std::cout << "BasicJoin::queryByBase()" << std::endl;
      auto index_start = std::chrono::high_resolution_clock::now();

      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();

      BinaryJoin<join_key_t, joinedPPs_t, part_t, partsupp_t> binary_join1([&]() { return part_scanner->next(); },
                                                                           [&]() { return partsupp_scanner->next(); });

      BinaryJoin<join_key_t, joinedPPsL_t, joinedPPs_t, sorted_lineitem_t> binary_join2([&]() { return binary_join1.next(); },
                                                                                        [&]() { return lineitem_scanner->next(); });

      binary_join2.run();

      auto index_end = std::chrono::high_resolution_clock::now();
      auto index_t = std::chrono::duration_cast<std::chrono::microseconds>(index_end - index_start).count();
      logger.log(index_t, "query-base");
   }

   void pointQueryByView()
   {
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      std::cout << "BasicJoin::pointQueryByView()" << std::endl;
      auto part_id = workload.getPartID();
      auto supplier_id = workload.getSupplierID();
      int count = 0;
      joinedPPsL.scan(
          joinedPPsL_t::Key{part_id, supplier_id},
          [&](const joinedPPsL_t::Key& k, const joinedPPsL_t&) {
             if (count == 0) {
                part_id = k.jk.l_partkey;
                supplier_id = k.jk.suppkey;
             } else if (k.jk.l_partkey != part_id || k.jk.suppkey != supplier_id) {
                return false;
             }
             count++;
             return true;
          },
          [&]() {});
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "point-query-view");
   }

   void pointQueryByBase()
   {
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      std::cout << "BasicJoin::pointQueryByBase()" << std::endl;
      auto part_id = workload.getPartID();
      auto supplier_id = workload.getSupplierID();
      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();
      part_scanner->seek(part_t::Key{part_id});
      partsupp_scanner->seek(partsupp_t::Key{part_id, supplier_id});
      lineitem_scanner->seek(sorted_lineitem_t::Key{part_id, supplier_id});

      BinaryJoin<join_key_t, joinedPPs_t, part_t, partsupp_t> binary_join1([&]() { return part_scanner->next(); },
                                                                           [&]() { return partsupp_scanner->next(); });
      BinaryJoin<join_key_t, joinedPPsL_t, joinedPPs_t, sorted_lineitem_t> binary_join2([&]() { return binary_join1.next(); },
                                                                                        [&]() { return lineitem_scanner->next(); });

      binary_join2.next_jk();

      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "point-query-base");
   }

   void pointQueryByMerged()
   {
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      std::cout << "BasicJoin::pointQueryByMerged()" << std::endl;

      auto part_id = workload.getPartID();
      auto supplier_id = workload.getSupplierID();
      auto merged_scanner = mergedPPsL.getScanner();
      merged_scanner->seekJK(join_key_t{part_id, supplier_id});
      PremergedJoin<join_key_t, joinedPPsL_t, part_t, partsupp_t, sorted_lineitem_t> merge(merged_scanner);
      merge.next_jk();

      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "point-query-merged");
   }

   void maintainMerged()
   {
      std::cout << "BasicJoin::maintainMerged()" << std::endl;
      // sortedLineitem, part, partsupp are seen as discarded and do not contribute to database size
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { workload.orders.insert(k, v); },
                       [this](const lineitem_t::Key& k, const lineitem_t& v) {
                          workload.lineitem.insert(k, v);
                          merged_lineitem_t::Key k_new{SKBuilder<join_key_t>::create(k, v), k};
                          merged_lineitem_t v_new(v);
                          mergedPPsL.insert(k_new, v_new);
                       },
                       [this](const part_t::Key& k, const part_t& v) {
                          merged_part_t::Key k_new{SKBuilder<join_key_t>::create(k, v), k};
                          merged_part_t v_new{v};
                          mergedPPsL.insert(k_new, v_new);
                       },
                       [this](const partsupp_t::Key& k, const partsupp_t& v) {
                          merged_partsupp_t::Key k_new{SKBuilder<join_key_t>::create(k, v), k};
                          merged_partsupp_t v_new(v);
                          mergedPPsL.insert(k_new, v_new);
                       },
                       "merged");
   }

   void maintainView()
   {
      logger.reset();
      std::cout << "BasicJoin::maintainView()" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      // sortedLineitem and all base tables cannot be replaced by this view
      std::vector<std::tuple<part_t::Key, part_t>> new_part;
      std::vector<std::tuple<partsupp_t::Key, partsupp_t>> new_partupp;
      std::vector<std::tuple<sorted_lineitem_t::Key, sorted_lineitem_t>> new_lineitems;
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { workload.orders.insert(k, v); },
                       [&, this](const lineitem_t::Key& k, const lineitem_t& v) {
                          workload.lineitem.insert(k, v);
                          sorted_lineitem_t::Key k_new{k, v};
                          sorted_lineitem_t v_new(v);
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
                       "view-stage1");
      // sort deltas
      auto compare = [](const auto& a, const auto& b) {
         return SKBuilder<join_key_t>::create(std::get<0>(a), std::get<1>(a)) < SKBuilder<join_key_t>::create(std::get<0>(b), std::get<1>(b));
      };
      std::sort(new_part.begin(), new_part.end(), compare);
      std::sort(new_partupp.begin(), new_partupp.end(), compare);
      std::sort(new_lineitems.begin(), new_lineitems.end(), compare);
      using Merge = MergeJoin<join_key_t, joinedPPsL_t, part_t, partsupp_t, sorted_lineitem_t>;
      auto part_it = new_part.begin();
      auto partsupp_it = new_partupp.begin();
      auto lineitems_it = new_lineitems.begin();
      join_key_t next_jk = SKBuilder<join_key_t>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      auto part_delta_src = [&]() {
         if (part_it == new_part.end()) {
            return HeapEntry<join_key_t>();
         }
         auto& [k, v] = *part_it;
         part_it++;
         auto jk = SKBuilder<join_key_t>::create(k, v);
         next_jk = jk;
         return HeapEntry<join_key_t>(jk, part_t::toBytes(k), part_t::toBytes(v), 0);
      };
      auto partsupp_delta_src = [&]() {
         if (partsupp_it == new_partupp.end()) {
            return HeapEntry<join_key_t>();
         }
         auto& [k, v] = *partsupp_it;
         partsupp_it++;
         auto jk = SKBuilder<join_key_t>::create(k, v);
         next_jk = jk;
         return HeapEntry<join_key_t>(jk, partsupp_t::toBytes(k), partsupp_t::toBytes(v), 1);
      };
      auto lineitem_delta_src = [&]() {
         if (lineitems_it == new_lineitems.end()) {
            return HeapEntry<join_key_t>();
         }
         auto& [k, v] = *lineitems_it;
         lineitems_it++;
         auto jk = SKBuilder<join_key_t>::create(k, v);
         return HeapEntry<join_key_t>(jk, sorted_lineitem_t::toBytes(k), sorted_lineitem_t::toBytes(v), 2);
      };

      join_key_t last_accessed_jk = next_jk;
      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();

      auto part_src = [&next_jk, &last_accessed_jk, &part_scanner]() {
         while (true) {
            auto kv = part_scanner->next();
            if (kv == std::nullopt) {
               return HeapEntry<join_key_t>();
            }
            auto& [k, v] = *kv;
            auto jk = SKBuilder<join_key_t>::create(k, v);
            if (last_accessed_jk.match(jk) != 0  // Scanned to a new jk
                && next_jk.match(jk) > 0)        // this new jk is not in the deltas, deltas have advanced to a larger jk
            {
               part_scanner->seek(part_t::Key{next_jk.l_partkey});
               last_accessed_jk = next_jk;
               continue;
            }
            last_accessed_jk = jk;
            return HeapEntry<join_key_t>(jk, part_t::toBytes(k), part_t::toBytes(v),
                                         0);  // not guaranteed to match the deltas but such waste is limited
         }
      };

      auto partsupp_src = [&next_jk, &last_accessed_jk, &partsupp_scanner]() {
         while (true) {
            auto kv = partsupp_scanner->next();
            if (kv == std::nullopt) {
               return HeapEntry<join_key_t>();
            }
            auto& [k, v] = *kv;
            auto jk = SKBuilder<join_key_t>::create(k, v);
            if (last_accessed_jk.match(jk) != 0  // Scanned to a new jk
                && next_jk.match(jk) > 0)        // this new jk is not in the deltas, deltas have advanced to a larger jk
            {
               partsupp_scanner->seek(partsupp_t::Key{next_jk.l_partkey, next_jk.suppkey});
               last_accessed_jk = next_jk;
               continue;
            }
            last_accessed_jk = jk;
            return HeapEntry<join_key_t>(jk, partsupp_t::toBytes(k), partsupp_t::toBytes(v),
                                         1);  // not guaranteed to match the deltas but such waste is limited
         }
      };

      auto lineitem_src = [&next_jk, &last_accessed_jk, &lineitem_scanner]() {
         while (true) {
            auto kv = lineitem_scanner->next();
            if (kv == std::nullopt) {
               return HeapEntry<join_key_t>();
            }
            auto& [k, v] = *kv;
            auto jk = SKBuilder<join_key_t>::create(k, v);
            if (last_accessed_jk.match(jk) != 0  // Scanned to a new jk
                && next_jk.match(jk) > 0)        // this new jk is not in the deltas, deltas have advanced to a larger jk
            {
               lineitem_scanner->seek(sorted_lineitem_t::Key{next_jk, lineitem_t::Key{0, 0}});
               last_accessed_jk = next_jk;
               continue;
            }
            last_accessed_jk = jk;
            return HeapEntry<join_key_t>(jk, sorted_lineitem_t::toBytes(k), sorted_lineitem_t::toBytes(v),
                                         2);  // not guaranteed to match the deltas but such waste is limited
         }
      };

      // Step 1 Join deltas
      std::cout << "Size: " << new_part.size() << " parts, " << new_partupp.size() << " partsupps, " << new_lineitems.size() << " lineitems"
                << std::endl;
      std::vector<std::function<HeapEntry<join_key_t>()>> sources1 = {part_delta_src, partsupp_delta_src, lineitem_delta_src};
      Merge delta_join1(sources1);
      delta_join1.run();
      // Step 2 join 2 deltas and search for matches in a base table
      part_it = new_part.begin();
      partsupp_it = new_partupp.begin();
      lineitem_scanner->reset();
      next_jk = SKBuilder<join_key_t>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      std::vector<std::function<HeapEntry<join_key_t>()>> sources2_1 = {part_delta_src, partsupp_delta_src, lineitem_src};
      Merge delta_join2_1(sources2_1);
      delta_join2_1.run();

      // TODO: change order of record types
      part_it = new_part.begin();
      lineitems_it = new_lineitems.begin();
      partsupp_scanner->reset();
      next_jk = SKBuilder<join_key_t>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      std::vector<std::function<HeapEntry<join_key_t>()>> sources2_2 = {part_delta_src, lineitem_delta_src, partsupp_src};
      Merge delta_join2_2(sources2_2);
      delta_join2_2.run();

      partsupp_it = new_partupp.begin();
      lineitems_it = new_lineitems.begin();
      part_scanner->reset();
      next_jk = SKBuilder<join_key_t>::create(std::get<0>(*partsupp_it), std::get<1>(*partsupp_it));
      std::vector<std::function<HeapEntry<join_key_t>()>> sources2_3 = {partsupp_delta_src, lineitem_delta_src, part_src};
      Merge delta_join2_3(sources2_3);
      delta_join2_3.run();

      // step 3 For 1 delta, search for matches in 2 base tables
      part_it = new_part.begin();
      partsupp_scanner->reset();
      lineitem_scanner->reset();
      next_jk = SKBuilder<join_key_t>::create(std::get<0>(*part_it), std::get<1>(*part_it));
      std::vector<std::function<HeapEntry<join_key_t>()>> sources3_1 = {part_delta_src, partsupp_src, lineitem_src};
      Merge delta_join3_1(sources3_1);
      delta_join3_1.run();

      partsupp_it = new_partupp.begin();
      lineitem_scanner->reset();
      part_scanner->reset();
      next_jk = SKBuilder<join_key_t>::create(std::get<0>(*partsupp_it), std::get<1>(*partsupp_it));
      std::vector<std::function<HeapEntry<join_key_t>()>> sources3_2 = {partsupp_delta_src, lineitem_src, part_src};
      Merge delta_join3_2(sources3_2);
      delta_join3_2.run();

      lineitems_it = new_lineitems.begin();
      part_scanner->reset();
      partsupp_scanner->reset();
      next_jk = SKBuilder<join_key_t>::create(std::get<0>(*lineitems_it), std::get<1>(*lineitems_it));
      std::vector<std::function<HeapEntry<join_key_t>()>> sources3_3 = {lineitem_delta_src, partsupp_src, part_src};
      Merge delta_join3_3(sources3_3);
      delta_join3_3.run();

      auto end = std::chrono::high_resolution_clock::now();

      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

      logger.log(t, "maintain-view");
   }

   void maintainBase()
   {
      std::cout << "BasicJoin::maintainBase()" << std::endl;
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { workload.orders.insert(k, v); },
                       [this](const lineitem_t::Key& k, const lineitem_t& v) { workload.lineitem.insert(k, v); },
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
      // 1 new part & several partsupp & one order/lineitem for this part
      auto part_id = workload.last_part_id + 1;
      workload.loadPart(part_insert_func, part_id, part_id);
      workload.loadPartsupp(partsupp_insert_func, part_id, part_id);
      auto o_id = workload.last_order_id + 1;
      Integer s_id;
      partsupp.scan(
          partsupp_t::Key{part_id, 1},
          [&](const partsupp_t::Key& k, const partsupp_t&) {
             assert(k.ps_partkey == part_id);
             s_id = k.ps_suppkey;
             return false;
          },
          []() {});
      lineitem_insert_func(lineitem_t::Key{o_id, 1}, lineitem_t::generateRandomRecord([part_id]() { return part_id; }, [s_id]() { return s_id; }));
      workload.loadOrders(order_insert_func, o_id, o_id);
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "maintain-" + name);
   }

   void loadBaseTables() { workload.load(); }

   void loadSortedLineitem()
   {
      // sort lineitem
      auto lineitem_scanner = workload.lineitem.getScanner();
      while (true) {
         auto kv = lineitem_scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         join_key_t jk{v.l_partkey, v.l_suppkey};
         sorted_lineitem_t::Key k_new(jk, k);
         sorted_lineitem_t v_new(v);
         this->sortedLineitem.insert(k_new, v_new);
         if (k.l_linenumber == 1)
            workload.printProgress("sortedLineitem", k.l_orderkey, 1, workload.last_order_id);
      }
   }

   void loadBasicJoin()
   {
      using Merge = MergeJoin<join_key_t, joinedPPsL_t, part_t, partsupp_t, sorted_lineitem_t>;
      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();
      Merge multiway_merge(joinedPPsL, *part_scanner.get(), *partsupp_scanner.get(), *lineitem_scanner.get());
      multiway_merge.run();
   };

   void loadMergedBasicJoin()
   {
      using Merge = Merge<join_key_t, merged_part_t, merged_partsupp_t, merged_lineitem_t>;
      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();
      Merge multiway_merge(mergedPPsL, *part_scanner.get(), *partsupp_scanner.get(), *lineitem_scanner.get());
      multiway_merge.run();
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
         *o << "view," << joinedPPsL.size() << std::endl;
         *o << "sortedLineitem," << sortedLineitem.size() << std::endl;
         *o << "merged," << mergedPPsL.size() << std::endl;
         *o << "base," << part.size() + partsupp.size() + sortedLineitem.size() << std::endl;
      }
      size_csv.close();
   }
};
}  // namespace basic_join