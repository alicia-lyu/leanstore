// RocksDB entry point for Q3I workload (skeleton — main() is a no-op).
//
// When body work lands, this file should mirror q12/executable_rocksdb.cpp:
//   1. Include RocksDB adapter headers and tpch_executable_helper.hpp.
//   2. Declare all 9 base TPC-H adapters (part, supplier, partsupp, customer,
//      orders, lineitem, nation, region, invoice).
//   3. Declare a pipeline_view adapter (q3i_pipeline_view_t).
//   4. Declare the 4-type COLI merged adapter
//      MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>.
//   5. Call rocks_db.open(), construct TPCHWorkload and Q3IWorkload.
//   6. Dispatch on FLAGS_storage_structure via
//      BaseQ3I / ViewQ3I / MergedQ3I / HashQ3I wrappers and
//      TpchExecutableHelper.

int main(int, char**)
{
   // TODO: implement — see comment above and q12/executable_rocksdb.cpp for
   // the exact pattern.
   return 0;
}
