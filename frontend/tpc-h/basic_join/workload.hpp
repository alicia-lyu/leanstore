#pragma once
#include <chrono>
#include <variant>
#include "../logger.hpp"
#include "../merge.hpp"
#include "../tables.hpp"
#include "../tpch_workload.hpp"
#include "view_maintainer.hpp"
#include "views.hpp"

// SELECT *
// FROM Lineitem l, PartSupp ps, Part p
// WHERE l.partkey = ps.partkey AND l.partkey = p.partkey AND l.suppkey = ps.suppkey;

namespace basic_join
{
template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename> class ScannerType>
class BasicJoin
{
   using TPCH = TPCHWorkload<AdapterType>;
   using merged_t = MergedAdapterType<merged_part_t, merged_partsupp_t, merged_lineitem_t>;
   TPCH& workload;
   merged_t& mergedPPsL;
   AdapterType<joinedPPsL_t>& joinedPPsL;
   AdapterType<sorted_lineitem_t>& sortedLineitem;

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

   // -------------------------------------------------------------
   // ---------------------- POINT LOOKUPS ------------------------
   // Warm-up/background TXs

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
      workload.nation.tryLookup(nation_t::Key{nation_id}, [&](const nation_t&) {});
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

   // -------------------------------------------------------------
   // ---------------------- QUERIES -----------------------------
   // Enumerate all rows in the joined result

   void queryByView()
   {
      // Enumrate materialized view
      logger.reset();
      std::cout << "BasicJoin::queryByView()" << std::endl;
      [[maybe_unused]] long produced = 0;
      auto start = std::chrono::high_resolution_clock::now();
      joinedPPsL.scan(
          {},
          [&](const auto&, const auto&) {
             TPCH::inspect_produced("Enumerating materialized view: ", produced);
             return true;
          },
          [&]() {});
      std::cout << std::endl;
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      logger.log(t, "query", "view", get_view_size());
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
      auto merged_t = std::chrono::duration_cast<std::chrono::milliseconds>(merged_end - merged_start).count();
      logger.log(merged_t, "query", "merged", get_merged_size());
   }

   void queryByBase()
   {
      logger.reset();
      std::cout << "BasicJoin::queryByBase()" << std::endl;
      auto index_start = std::chrono::high_resolution_clock::now();

      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();

      BinaryMergeJoin<join_key_t, joinedPPs_t, part_t, partsupp_t> binary_join1([&]() { return part_scanner->next(); },
                                                                                [&]() { return partsupp_scanner->next(); });

      BinaryMergeJoin<join_key_t, joinedPPsL_t, joinedPPs_t, sorted_lineitem_t> binary_join2([&]() { return binary_join1.next(); },
                                                                                             [&]() { return lineitem_scanner->next(); });

      binary_join2.run();

      auto index_end = std::chrono::high_resolution_clock::now();
      auto index_t = std::chrono::duration_cast<std::chrono::milliseconds>(index_end - index_start).count();
      logger.log(index_t, "query", "base", get_base_size());
   }

   // --------------------------------------------------------------
   // ---------------------- RANGE QUERIES ------------------------
   // Find all joined rows for the same partkey

   void range_query_by_view()
   {
      auto part_id = workload.getPartID();
      int count = 0;
      joinedPPsL.scan(
          joinedPPsL_t::Key(join_key_t{part_id, 0}, part_t::Key{part_id}, partsupp_t::Key{part_id, 0}, sorted_lineitem_t::Key{}),
          [&](const joinedPPsL_t::Key& k, const joinedPPsL_t&) {
             if (count == 0) {
                part_id = k.jk.l_partkey;
             } else if (k.jk.l_partkey != part_id) {
                return false;
             }
             count++;
             return true;
          },
          [&]() {});
   }

   void range_query_by_base()
   {
      auto part_id = workload.getPartID();
      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();
      part_scanner->seek(part_t::Key{part_id});
      partsupp_scanner->seek(partsupp_t::Key{part_id, 0});
      lineitem_scanner->seek(sorted_lineitem_t::Key(join_key_t{part_id, 0}, lineitem_t::Key{}));

      BinaryMergeJoin<join_key_t, joinedPPs_t, part_t, partsupp_t> binary_join1([&]() { return part_scanner->next(); },
                                                                                [&]() { return partsupp_scanner->next(); });
      BinaryMergeJoin<join_key_t, joinedPPsL_t, joinedPPs_t, sorted_lineitem_t> binary_join2([&]() { return binary_join1.next(); },
                                                                                             [&]() { return lineitem_scanner->next(); });

      while (true) {
         auto res = binary_join2.next_jk();
         if (res == -1 || binary_join2.current_jk.l_partkey != part_id) {
            break;
         }
      }
   }

   void range_query_by_merged()
   {
      auto part_id = workload.getPartID();
      auto merged_scanner = mergedPPsL.getScanner();
      merged_scanner->seekJK(join_key_t{part_id, 0});
      PremergedJoin<join_key_t, joinedPPsL_t, merged_part_t, merged_partsupp_t, merged_lineitem_t> merge(*merged_scanner);
      while (true) {
         auto res = merge.next_jk();
         if (res == -1 || merge.current_jk.l_partkey != part_id) {
            break;
         }
      }
   }

   // -------------------------------------------------------------
   // ---------------------- POINT QUERIES ------------------------
   // Find all joined rows for the same join key

   void pointQueryByView()
   {
      auto part_id = workload.getPartID();
      auto supplier_id = workload.getSupplierID();
      int count = 0;
      joinedPPsL.scan(
          joinedPPsL_t::Key(join_key_t{part_id, supplier_id}, part_t::Key{part_id}, partsupp_t::Key{part_id, supplier_id}, sorted_lineitem_t::Key{}),
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
   }

   void pointQueryByBase()
   {
      auto part_id = workload.getPartID();
      auto supplier_id = workload.getSupplierID();
      auto part_scanner = part.getScanner();
      auto partsupp_scanner = partsupp.getScanner();
      auto lineitem_scanner = sortedLineitem.getScanner();
      part_scanner->seek(part_t::Key{part_id});
      partsupp_scanner->seek(partsupp_t::Key{part_id, supplier_id});
      lineitem_scanner->seek(sorted_lineitem_t::Key(join_key_t{part_id, supplier_id}, lineitem_t::Key{}));

      BinaryMergeJoin<join_key_t, joinedPPs_t, part_t, partsupp_t> binary_join1([&]() { return part_scanner->next(); },
                                                                                [&]() { return partsupp_scanner->next(); });
      BinaryMergeJoin<join_key_t, joinedPPsL_t, joinedPPs_t, sorted_lineitem_t> binary_join2([&]() { return binary_join1.next(); },
                                                                                             [&]() { return lineitem_scanner->next(); });

      binary_join2.next_jk();
   }

   void pointQueryByMerged()
   {
      auto part_id = workload.getPartID();
      auto supplier_id = workload.getSupplierID();
      auto merged_scanner = mergedPPsL.getScanner();
      merged_scanner->template seekTyped<merged_part_t>(merged_part_t::Key{part_id});
      [[maybe_unused]] auto part = merged_scanner->current().value();
      merged_scanner->seekJK(join_key_t{part_id, supplier_id});
      PremergedJoin<join_key_t, joinedPPsL_t, merged_part_t, merged_partsupp_t, merged_lineitem_t> merge(*merged_scanner);
      merge.next_jk();
   }

   // -------------------------------------------------------------
   // ---------------------- MAINTENANCE --------------------------
   // // Add 1 new part & several partsupp & one order/lineitem for this part

   void maintainTemplate(std::function<void(const orders_t::Key&, const orders_t&)> order_insert_func,
                         std::function<void(const lineitem_t::Key&, const lineitem_t&)> lineitem_insert_func,
                         std::function<void(const part_t::Key&, const part_t&)> part_insert_func,
                         std::function<void(const partsupp_t::Key&, const partsupp_t&)> partsupp_insert_func)
   {
      auto part_id = workload.last_part_id + 1;
      auto o_id = workload.last_order_id + 1;

      size_t supplier_cnt = urand(1, workload.PARTSUPP_SCALE / workload.PART_SCALE * 2 - 1);
      std::set<Integer> suppliers = {};
      while (suppliers.size() < supplier_cnt) {
         Integer supplier_id = urand(1, workload.last_supplier_id);
         suppliers.insert(supplier_id);
      }
      for (auto& s : suppliers) {
         // load 1 partsupp
         partsupp_insert_func(partsupp_t::Key{part_id, s}, partsupp_t::generateRandomRecord());
      }

      workload.loadPart(part_insert_func, part_id, part_id);

      // load 1 lineitem for this part
      auto s_idx = urand(0, suppliers.size() - 1);
      auto& s = *std::next(suppliers.begin(), s_idx);
      auto rec = lineitem_t::generateRandomRecord([part_id]() { return part_id; }, [s]() { return s; });
      lineitem_insert_func(lineitem_t::Key{o_id, 1}, rec);

      workload.loadOrders(order_insert_func, o_id, o_id);
   }

   void maintainMerged()
   {
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
                       });
   }

   void maintainView()
   {
      ViewMaintainer<AdapterType, MergedAdapterType, ScannerType> vm(
          [this](auto&&... args) { return this->maintainTemplate(std::forward<decltype(args)>(args)...); }, workload, sortedLineitem, joinedPPsL);
      vm.run();
   }

   void maintainBase()
   {
      maintainTemplate([this](const orders_t::Key& k, const orders_t& v) { workload.orders.insert(k, v); },
                       [this](const lineitem_t::Key& k, const lineitem_t& v) {
                          workload.lineitem.insert(k, v);
                          sortedLineitem.insert(sorted_lineitem_t::Key{k, v}, sorted_lineitem_t{v});
                       },
                       [this](const part_t::Key& k, const part_t& v) { this->part.insert(k, v); },
                       [this](const partsupp_t::Key& k, const partsupp_t& v) { this->partsupp.insert(k, v); });
   }

   // -------------------------------------------------------------
   // ---------------------- LOAD ---------------------------------

   void loadBaseTables();
   void loadSortedLineitem();
   void loadBasicJoin();
   void loadMergedBasicJoin();
   void load();
   double get_view_size();
   double get_merged_size();
   double get_base_size();
   void log_sizes();
};
}  // namespace basic_join
#include "load.tpp" // IWYU pragma: keep