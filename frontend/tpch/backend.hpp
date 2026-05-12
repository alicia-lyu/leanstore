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

#include "tpch_family/views_ol.hpp"  // joined_ol_t — required by MergedScanner alias

namespace tpch
{

// ---------------------------------------------------------------------------
// RocksDB backend

struct RocksDBBackend {
   // COLI walker: skip rejected customer groups via physical Seek to
   // the next custkey rather than forward iteration. The original A/B
   // (commit 8d10782b, archive/PERFORMANCE-2026-05-03.md §H2) ran on
   // macOS and reported a ~5× regression at SF=40, attributed to the
   // SST prefetch buffer being invalidated on Seek. The Linux re-A/B
   // (q3i/PERFORMANCE.md §3 A3 RocksDB re-open) refutes that finding:
   // SF=15 1.81→14.46 TX/s (+700%), SF=40 0.90→1.48 TX/s (+64%) at
   // dram=0.1 iso S3 with fused_emit. The macOS regression appears
   // to have been a page-cache artefact. Trait flipped to true on
   // both backends as a result.
   //
   // Order-level skip stays as forward iteration on BOTH backends —
   // small order groups (~4 lineitems) don't pay back a tree descent.
   //
   // Runtime override available via --use_seek_skip (-1=trait,
   // 0=force off, 1=force on) for future regression A/B work.
   static constexpr bool USE_PHYSICAL_SEEK_SKIP = true;

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
   // invalidate. A3 (LeanStore): SF=15 23.06→105.27 TX/s (+356%),
   // SF=40 0.29→33.69 TX/s (+116×). The Linux re-A/B confirmed the
   // same direction wins on RocksDB too — see RocksDBBackend above.
   // Order-level skip stays as forward iteration on both backends.
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
