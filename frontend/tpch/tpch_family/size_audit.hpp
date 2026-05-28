#pragma once

// size_audit — file_size(ssd_path) sanity helper.
//
// Per-Sx images put the entire DB for one storage-structure variant into one
// directory (LSM) or one file (btree), so the on-disk footprint is the
// authoritative DB-size measure. Per-adapter sums returned by Q*Workload::
// get_size() are kept as the primary measurement (paper plots reference
// historical CSVs) but they have two known gaps:
//
//   (a) Co-resident structures the binary doesn't open. Q3 at S3 can't see
//       the aCOL MI that load_*_family co-loads under S3↔S5 image sharing.
//   (b) LSM default-CF over-counting. Non-merged tables collapse to the
//       `default` CF; each adapter on default returns the FULL default-CF
//       size, summed N times.
//
// log_size_audit() prints both numbers and the delta% so post-hoc analysis can
// disentangle. Per-Sx tolerances (see code below) flag implausible gaps loud.

#include <cstdint>
#include <filesystem>
#include <gflags/gflags.h>
#include <iomanip>
#include <iostream>
#include <string>

DECLARE_string(ssd_path);
DECLARE_int32(storage_structure);

namespace tpch
{

// MiB on disk for the current Sx image. btree image is a single file; LSM
// image is a directory of SST files (WAL/LOG/CURRENT/MANIFEST excluded — we
// only count *.sst per RocksDB live-data semantics).
inline double image_size_mib(const std::string& path)
{
   namespace fs = std::filesystem;
   if (!fs::exists(path)) return 0.0;
   std::error_code ec;
   if (fs::is_regular_file(path, ec)) {
      auto sz = fs::file_size(path, ec);
      return ec ? 0.0 : static_cast<double>(sz) / 1024.0 / 1024.0;
   }
   // Directory: recursively sum *.sst.
   double bytes = 0;
   for (auto it = fs::recursive_directory_iterator(path, ec);
        it != fs::recursive_directory_iterator(); ++it) {
      if (ec) break;
      if (!it->is_regular_file(ec)) continue;
      if (it->path().extension() == ".sst") {
         bytes += static_cast<double>(fs::file_size(it->path(), ec));
      }
   }
   return bytes / 1024.0 / 1024.0;
}

// Per-Sx warning threshold for |sum - image| / image. Set per S knowing what
// the binary can/can't see in the shared image. Anything above this is loud.
inline double size_audit_tolerance_pct(int sx)
{
   switch (sx) {
      case 1: return 5.0;   // base + own splits; image is the same
      case 2: return 25.0;  // image holds COL MI + 3 sibling views + preagg; per-binary view
      case 3: return 25.0;  // image holds COL MI + aCOL; Q3/Q5 miss aCOL
      case 4: return 5.0;   // base only
      case 5: return 10.0;  // Q10/Q10I only — should now match closely with COL MI added
      case 6: return 10.0;
      case 7: return 30.0;  // image holds COL MI + 3 sibling views + preagg; per-binary sees own slice
      default: return 50.0;
   }
}

// Print one line per (query, sx) pair per process: structure, per-adapter sum,
// image size, delta%. Loud warning if delta exceeds Sx tolerance. Static gate
// avoids interleaving when get_size() is called inside a std::cout chain.
inline void log_size_audit(const char* query, double per_adapter_mib)
{
   static std::string last_key;
   std::string key = std::string(query) + "/" + std::to_string(FLAGS_storage_structure);
   if (key == last_key) return;
   last_key = key;
   double img_mib = image_size_mib(FLAGS_ssd_path);
   double delta_pct = (img_mib > 0.0)
                          ? (per_adapter_mib - img_mib) / img_mib * 100.0
                          : 0.0;
   double tol = size_audit_tolerance_pct(FLAGS_storage_structure);
   bool loud = (img_mib > 0.0) && (std::abs(delta_pct) > tol);
   // Prefix newline + emit endl so we don't corrupt an ongoing cout chain
   // (summary line concatenates wrapper.get_size() inline).
   std::cout << "\nsize_audit " << query
             << " sx=" << FLAGS_storage_structure
             << " per_adapter=" << std::fixed << std::setprecision(1) << per_adapter_mib
             << " MiB  image_disk=" << img_mib
             << " MiB  delta=" << std::showpos << delta_pct << "%"
             << std::noshowpos
             << (loud ? "  WARN(>" + std::to_string(static_cast<int>(tol)) + "%)" : "")
             << std::endl;
}

}  // namespace tpch
