#pragma once

// Backend traits for TPC-H Tier 1 workloads (Q12, Q3, Q9).
//
// Each traits struct exposes four nested alias templates so that per-query
// workload classes can be templated on a single `Backend` parameter instead
// of the four separate adapter/scanner type-template parameters used in the
// geo benchmark.
//
// Usage:
//   template <typename Backend>
//   class Q12Workload {
//     typename Backend::template Adapter<orders_t>& orders;
//     typename Backend::template MergedAdapter<orders_t, lineitem_t>& mi;
//     ...
//   };
//
// `LeanStoreBackend` is compiled only when the LeanStore headers are
// available (i.e., not on macOS ROCKSDB_ONLY builds).

#include "../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedScanner.hpp"
#include "../shared/adapter-scanner/RocksDBScanner.hpp"

#ifndef ROCKSDB_ONLY
#include "../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../shared/adapter-scanner/LeanStoreMergedScanner.hpp"
#include "../shared/adapter-scanner/LeanStoreScanner.hpp"
#endif

#include "views_ol.hpp"  // joined_ol_t — required by MergedScanner alias

namespace tpch
{

// ---------------------------------------------------------------------------
// RocksDB backend

struct RocksDBBackend {
   // Single-type adapter and scanner.
   template <typename T>
   using Adapter = RocksDBAdapter<T>;

   template <typename... Ts>
   using MergedAdapter = RocksDBMergedAdapter<Ts...>;

   template <typename T>
   using Scanner = RocksDBScanner<T>;

   // MergedScanner: JK = sort key, first JR = joined_ol_t (the join-result
   // type required by PremergedJoin / BinaryMergeJoin), then Rs... = the
   // individual record types stored in the merged index.
   template <typename SK, typename... Rs>
   using MergedScanner = RocksDBMergedScanner<SK, joined_ol_t, Rs...>;
};

// ---------------------------------------------------------------------------
// LeanStore backend (Linux / full build only)

#ifndef ROCKSDB_ONLY
struct LeanStoreBackend {
   template <typename T>
   using Adapter = LeanStoreAdapter<T>;

   template <typename... Ts>
   using MergedAdapter = LeanStoreMergedAdapter<Ts...>;

   template <typename T>
   using Scanner = LeanStoreScanner<T>;

   template <typename SK, typename... Rs>
   using MergedScanner = LeanStoreMergedScanner<SK, joined_ol_t, Rs...>;
};
#endif

}  // namespace tpch
