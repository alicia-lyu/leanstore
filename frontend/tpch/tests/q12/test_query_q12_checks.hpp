#pragma once
// Cross-structure correctness checks for Q12 query_by_* results.
//
// print_rows   — dump a result vector for human inspection.
// xor_parity_q12 — fold a result vector into a 64-bit XOR digest.
// check_parity_q12 — compare all four structures; print [OK]/[FAIL] per line.
// check_shape_q12  — structural assertions independent of input data.
//
// XOR digest spec (OPERATORS.md §6 / tpch/CLAUDE.md §Cross-Structure):
//   For each row, mix fields with rotl(digest, 13) ^ field_bytes, then
//   XOR the per-row accumulator into the result digest. Order-independent
//   so HashJoin emission order doesn't matter before the final sort.

#include <bit>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string_view>
#include <vector>

#include "../../q12/workload.hpp"

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// print_rows: dump a result table for human inspection.

inline void print_rows(const std::string& label,
                       const std::vector<q12_agg_row_t>& rows,
                       std::ostream& os)
{
   os << "[" << label << "]\n";
   for (const auto& r : rows) {
      r.print(os);
   }
}

// ---------------------------------------------------------------------------
// xor_parity_q12: fold a result vector into a 64-bit XOR digest.
//
// Per row: hash l_shipmode (length-prefixed bytes), high_line_count (4-byte LE),
// low_line_count (4-byte LE) into a per-row accumulator using rotl(acc,13)^field.
// XOR each per-row accumulator into the result digest.

inline uint64_t xor_parity_q12(const std::vector<q12_agg_row_t>& rows)
{
   // Mix one 8-byte chunk into acc using rotate-left-13 then XOR.
   auto mix = [](uint64_t acc, uint64_t field) -> uint64_t {
      return std::rotl(acc, 13) ^ field;
   };

   // Read 4 bytes little-endian from an Integer field.
   auto le32 = [](Integer v) -> uint64_t {
      uint32_t u;
      std::memcpy(&u, &v, sizeof(u));
      return static_cast<uint64_t>(u);
   };

   uint64_t digest = 0;
   for (const auto& r : rows) {
      uint64_t row_acc = 0;

      // l_shipmode: length prefix then each byte.
      row_acc = mix(row_acc, static_cast<uint64_t>(r.l_shipmode.length));
      for (int i = 0; i < r.l_shipmode.length; ++i) {
         row_acc = mix(row_acc, static_cast<uint64_t>(
             static_cast<unsigned char>(r.l_shipmode.data[i])));
      }

      // high_line_count and low_line_count (4-byte LE each).
      row_acc = mix(row_acc, le32(r.high_line_count));
      row_acc = mix(row_acc, le32(r.low_line_count));

      digest ^= row_acc;
   }
   return digest;
}

// ---------------------------------------------------------------------------
// check_parity_q12: compare all four structures against base.
// Also runs row-by-row equality so a parity collision can't mask a bug.
// Returns true iff all checks pass.

inline bool check_parity_q12(const std::vector<q12_agg_row_t>& r_base,
                              const std::vector<q12_agg_row_t>& r_view,
                              const std::vector<q12_agg_row_t>& r_merged,
                              const std::vector<q12_agg_row_t>& r_hash,
                              std::ostream& os)
{
   auto tag = [](bool ok) { return ok ? "[OK]  " : "[FAIL]"; };

   const uint64_t p_base   = xor_parity_q12(r_base);
   const uint64_t p_view   = xor_parity_q12(r_view);
   const uint64_t p_merged = xor_parity_q12(r_merged);
   const uint64_t p_hash   = xor_parity_q12(r_hash);

   bool ok_view   = (p_base == p_view);
   bool ok_merged = (p_base == p_merged);
   bool ok_hash   = (p_base == p_hash);

   // Row-by-row equality check so a parity collision can't silently mask a bug.
   auto row_equal = [](const q12_agg_row_t& a, const q12_agg_row_t& b) {
      return std::string_view(a.l_shipmode.data, a.l_shipmode.length)
                 == std::string_view(b.l_shipmode.data, b.l_shipmode.length)
             && a.high_line_count == b.high_line_count
             && a.low_line_count  == b.low_line_count;
   };
   auto vectors_equal = [&](const std::vector<q12_agg_row_t>& a,
                             const std::vector<q12_agg_row_t>& b) {
      if (a.size() != b.size()) return false;
      for (size_t i = 0; i < a.size(); ++i) {
         if (!row_equal(a[i], b[i])) return false;
      }
      return true;
   };

   ok_view   = ok_view   && vectors_equal(r_base, r_view);
   ok_merged = ok_merged && vectors_equal(r_base, r_merged);
   ok_hash   = ok_hash   && vectors_equal(r_base, r_hash);

   os << "\n=== Q12 cross-structure parity ===\n";
   os << tag(true)       << " parity base   = 0x" << std::hex << p_base   << std::dec << "\n";
   os << tag(ok_view)    << " parity view   = 0x" << std::hex << p_view   << std::dec
      << (ok_view   ? " (== base)" : " (!= base)") << "\n";
   os << tag(ok_merged)  << " parity merged = 0x" << std::hex << p_merged << std::dec
      << (ok_merged ? " (== base)" : " (!= base)") << "\n";
   os << tag(ok_hash)    << " parity hash   = 0x" << std::hex << p_hash   << std::dec
      << (ok_hash   ? " (== base)" : " (!= base)") << "\n";

   return ok_view && ok_merged && ok_hash;
}

// ---------------------------------------------------------------------------
// check_shape_q12: structural assertions independent of input data.
// Expects: exactly 2 rows; shipmodes match params after sort; high+low > 0
// (at SF=1 with ~4% filter selectivity thousands of rows should pass).
// Returns true iff all checks pass.

inline bool check_shape_q12(const std::string& label,
                             const std::vector<q12_agg_row_t>& rows,
                             const Params& params,
                             std::ostream& os)
{
   auto tag = [](bool ok) { return ok ? "[OK]  " : "[FAIL]"; };

   bool ok = true;

   const bool two_rows = (rows.size() == 2);
   os << tag(two_rows) << " shape [" << label << "]: row count == 2 (got " << rows.size() << ")\n";
   ok = ok && two_rows;

   if (two_rows) {
      // After the final sort, row 0 and row 1 should be the sorted shipmodes.
      auto sv = [](const Varchar<10>& v) {
         return std::string_view(v.data, v.length);
      };

      // Determine expected sorted order.
      bool sm1_first = sv(params.shipmode1) < sv(params.shipmode2);
      const Varchar<10>& exp0 = sm1_first ? params.shipmode1 : params.shipmode2;
      const Varchar<10>& exp1 = sm1_first ? params.shipmode2 : params.shipmode1;

      const bool sm0_ok = sv(rows[0].l_shipmode) == sv(exp0);
      const bool sm1_ok = sv(rows[1].l_shipmode) == sv(exp1);
      os << tag(sm0_ok && sm1_ok) << " shape [" << label << "]: expected shipmodes\n";
      ok = ok && sm0_ok && sm1_ok;

      const bool nonzero0 = (rows[0].high_line_count + rows[0].low_line_count) > 0;
      const bool nonzero1 = (rows[1].high_line_count + rows[1].low_line_count) > 0;
      os << tag(nonzero0 && nonzero1) << " shape [" << label << "]: high+low > 0 for both rows\n";
      ok = ok && nonzero0 && nonzero1;
   }

   return ok;
}

}  // namespace tpch::q12
