#include <chrono>
#include <vector>
#include "../shared/LeanStoreMergedAdapter.hpp"
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

   template <typename JK, typename JoinedRec, typename TupleVecs, typename TupleFuncs, std::size_t... Is>
   void joinAndClear(TupleVecs& cached_records_all,
                                  TupleFuncs& getJKs,
                                  const JK& current_jk,
                                  const HeapEntry<JK>& entry,
                                  long& joined_cnt,
                                  std::index_sequence<Is...>)
   {
      (..., ([&] {
          auto& vec = std::get<Is>(cached_records_all);
          auto getJK = std::get<Is>(getJKs);
          if (entry.jk.match(getJK(current_jk)) != 0) {
             LeanStoreMergedAdapter::joinCurrent<JK, JoinedRec>(cached_records_all, joined_cnt);
             vec.clear();
          }
       }()));
   }

   template <typename JK, typename RecordType, typename JoinedRec, typename... Records>
   auto getHeapConsumeToBeJoined(std::vector<RecordType>& cached_records,
                                 JK& current_jk,
                                 std::tuple<JK (*)(const JK&), JK (*)(const JK&), JK (*)(const JK&)>& getJKs,
                                 long& joined_cnt,
                                 std::tuple<std::vector<Records>...>& cached_records_all)
   {
      return [&cached_records, &current_jk, &joined_cnt, &cached_records_all, getJKs, this](HeapEntry<JK>& entry) {
         joinAndClear<JK, JoinedRec>(cached_records_all, getJKs, current_jk, entry, joined_cnt, std::index_sequence_for<Records...>{});
         current_jk = entry.jk;
         cached_records.push_back(RecordType::template fromBytes<RecordType>(entry.v));
      };
   }

   void queryByIndex()
   {
      logger.reset();
      auto index_start = std::chrono::high_resolution_clock::now();
      std::tuple<std::vector<part_t>, std::vector<partsupp_t>, std::vector<merged_lineitem_t>> cached_records;
      auto& [cached_parts, cached_partsupps, cached_lineitems] = cached_records;
      PPsL_JK current_jk{};

      long joined_cnt = 0;

      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();

      auto part_src = TPCH::template getHeapSource<PPsL_JK, part_t>(
          part, 0, std::function([](const part_t::Key& k, const part_t&) { return PPsL_JK{k.p_partkey, 0}; }));
      auto partsupp_src = TPCH::template getHeapSource<PPsL_JK, partsupp_t>(
          partsupp, 1, std::function([](const partsupp_t::Key& k, const partsupp_t&) { return PPsL_JK{k.ps_partkey, k.ps_suppkey}; }));
      auto lineitem_src = TPCH::template getHeapSource<PPsL_JK, merged_lineitem_t>(
          sortedLineitem, 2, std::function([](const merged_lineitem_t::Key& k, const merged_lineitem_t&) { return k.jk; }));

      auto getJKs = std::make_tuple(merged_part_t::getJK, merged_partsupp_t::getJK, merged_lineitem_t::getJK);
      auto part_consume = getHeapConsumeToBeJoined<PPsL_JK, part_t, joinedPPsL_t, part_t, partsupp_t, merged_lineitem_t>(
          cached_parts, current_jk, getJKs, joined_cnt, cached_records);
      auto partsupp_consume = getHeapConsumeToBeJoined<PPsL_JK, partsupp_t, joinedPPsL_t, part_t, partsupp_t, merged_lineitem_t>(
          cached_partsupps, current_jk, getJKs, joined_cnt, cached_records);
      auto lineitem_consume = getHeapConsumeToBeJoined<PPsL_JK, merged_lineitem_t, joinedPPsL_t, part_t, partsupp_t, merged_lineitem_t>(
          cached_lineitems, current_jk, getJKs, joined_cnt, cached_records);

      TPCH::template heapMerge<PPsL_JK>({part_src, partsupp_src, lineitem_src}, {part_consume, partsupp_consume, lineitem_consume});

      auto index_end = std::chrono::high_resolution_clock::now();
      auto index_t = std::chrono::duration_cast<std::chrono::microseconds>(index_end - index_start).count();
      std::cout << std::endl
                << "Scanned " << part.produced << " parts, " << partsupp.produced << " partsupps, " << sortedLineitem.produced
                << " lineitems, joined " << joined_cnt << " records" << std::endl;
      logger.log(index_t, "index");

      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
   }

   void maintainMerged()
   {

   }

   void maintainView()
   {

   }

   void maintainIndex()
   {
      logger.reset();
      auto start = std::chrono::high_resolution_clock::now();
      // 100 new orders
      auto order_start = workload.last_order_id + 1;
      auto order_end = workload.last_order_id + 100;
      workload.loadOrders(order_start, order_end);
      workload.loadLineitem(order_start, order_end);
      // 1 new part & several partsupps
      auto part_start = workload.last_part_id + 1;
      auto part_end = workload.last_part_id + 1;
      workload.loadPart(part_start, part_end);
      workload.loadPartsupp(part_start, part_end);
      auto end = std::chrono::high_resolution_clock::now();
      auto t = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      logger.log(t, "index-maintain");
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
         this->part.resetIterator();
         this->partsupp.resetIterator();
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
         this->joinedPPs.resetIterator();
         this->sortedLineitem.resetIterator();
      }
   };

   template <typename JK, typename Base, typename Merged>
   static std::function<void(HeapEntry<JK>&)> getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter](HeapEntry<JK>& entry) {
         typename Merged::Key k_new({entry.jk, Base::template fromBytes<typename Base::Key>(entry.k)});
         Merged v_new(Base::template fromBytes<Base>(entry.v));
         mergedAdapter.insert(k_new, v_new);
      };
   }

   template <typename JK, typename RecordType>
   static std::function<void(HeapEntry<JK>&)> getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter](HeapEntry<JK>& entry) {
         typename RecordType::Key k_new(RecordType::template fromBytes<typename RecordType::Key>(entry.k));
         RecordType v_new(RecordType::template fromBytes<RecordType>(entry.v));
         mergedAdapter.insert(k_new, v_new);
      };
   }

   void loadMergedBasicJoin()
   {
      part.resetIterator();
      partsupp.resetIterator();
      sortedLineitem.resetIterator();
      auto part_src = TPCH::template getHeapSource<PPsL_JK, part_t>(
          part, 0, std::function([](const part_t::Key& k, const part_t&) { return PPsL_JK{k.p_partkey, 0}; }));
      auto partsupp_src = TPCH::template getHeapSource<PPsL_JK, partsupp_t>(
          partsupp, 1, std::function([](const partsupp_t::Key& k, const partsupp_t&) { return PPsL_JK{k.ps_partkey, k.ps_suppkey}; }));
      auto lineitem_src = TPCH::template getHeapSource<PPsL_JK, merged_lineitem_t>(
          sortedLineitem, 2, std::function([](const merged_lineitem_t::Key& k, const merged_lineitem_t&) { return k.jk; }));

      auto part_consume = getHeapConsumeToMerged<PPsL_JK, part_t, merged_part_t>(mergedPPsL);
      auto partsupp_consume = getHeapConsumeToMerged<PPsL_JK, partsupp_t, merged_partsupp_t>(mergedPPsL);
      auto lineitem_consume = getHeapConsumeToMerged<PPsL_JK, merged_lineitem_t>(mergedPPsL);

      std::vector<std::function<HeapEntry<PPsL_JK>()>> sources = {part_src, partsupp_src, lineitem_src};
      std::vector<std::function<void(HeapEntry<PPsL_JK>&)>> consumes = {part_consume, partsupp_consume, lineitem_consume};
      TPCH::template heapMerge<PPsL_JK>(sources, consumes);

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