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
   // COLI walker: skip rejected customer groups via physical Seek to the
   // next custkey rather than forward iteration. RocksDB pays a steep
   // tax here — Seek invalidates the iterator's prefetch buffer (1–2
   // SST blocks lookahead), regressing SF=40 disk-bound by ~5×. See
   // q3i/PERFORMANCE.md §H2 and the archived A/B in
   // archive/PERFORMANCE-2026-05-03.md.
   //
   // Order-level skip stays as forward iteration on BOTH backends —
   // small order groups (~4 lineitems) don't pay back a tree descent.
   static constexpr bool USE_PHYSICAL_SEEK_SKIP = false;

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
   // B-tree Seek is an O(log N) tree descent with no prefetch buffer to
   // invalidate (unlike RocksDB). Forward iteration through a rejected
   // custkey group can touch 10–50 records spread across separate leaf
   // pages, so a Seek to the next customer should win. (A3.) Order-level
   // skip remains forward-iteration on both backends — order groups are
   // too small for the descent cost to pay back.
   static constexpr bool USE_PHYSICAL_SEEK_SKIP = true;

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
