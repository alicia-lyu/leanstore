#pragma once
#include "join_state.hpp"

template <typename JK, typename... Rs>
struct Merge {
   HeapMergeHelper<JK, Rs...> heap_merge;
   bool logging = false;

   template <typename MergedAdapterType, template <typename> class ScannerType, typename... SourceRecords>
   Merge(MergedAdapterType& mergedAdapter, ScannerType<SourceRecords>&... scanners)
       : heap_merge(getHeapConsumesToMerged<MergedAdapterType, SourceRecords...>(mergedAdapter), scanners...)
   {
      heap_merge.init();
   }

   ~Merge()
   {
      if (heap_merge.sifted > 1000)
         std::cout << "~Merge: produced " << (double)heap_merge.sifted / 1000 << "k records------------------------------------" << std::endl;
   }

   void printProgress()
   {
      if (heap_merge.current_jk % 10 == 0 && FLAGS_log_progress) {
         double progress = (double)heap_merge.sifted / 1000;
         std::cout << "\rMerge: " << progress << "k records------------------------------------";
      }
   }

   template <typename MergedAdapterType, typename RecordType, typename SourceRecord>
   auto getHeapConsumeToMerged(MergedAdapterType& mergedAdapter)
   {
      return [&mergedAdapter, this](HeapEntry<JK>& entry) {
         heap_merge.current_entry = entry;
         auto source_k = SourceRecord::template fromBytes<typename SourceRecord::Key>(entry.k);
         auto source_v = SourceRecord::template fromBytes<SourceRecord>(entry.v);
         mergedAdapter.insert(typename RecordType::Key(source_k, source_v), RecordType(source_v));
         printProgress();
      };
   }

   template <typename MergedAdapterType, typename... SourceRecords>
   std::vector<std::function<void(HeapEntry<JK>&)>> getHeapConsumesToMerged(MergedAdapterType& mergedAdapter)
   {
      return {getHeapConsumeToMerged<MergedAdapterType, Rs, SourceRecords>(mergedAdapter)...};
   }

   void run()
   {
      logging = true;
      heap_merge.run();
   }

   void next_jk()
   {
      JK start_jk = heap_merge.current_jk;
      while (heap_merge.current_jk == start_jk && heap_merge.has_next()) {
         heap_merge.next();
      }
   }

   JK current_jk() const { return heap_merge.current_jk; }

   long produced() const { return heap_merge.sifted; }
};