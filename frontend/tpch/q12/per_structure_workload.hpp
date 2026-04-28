#pragma once

// Per-storage-structure dispatch wrappers for Q12.
//
// Plain (non-virtual) structs — no PerStructureWorkload<> base, no virtual
// destructor. The executable's switch(FLAGS_storage_structure) instantiates
// the chosen struct directly and calls query() / get_size() on it.
//
// Design note: virtual dispatch is not load-bearing here because structure
// selection happens at compile time via a switch. Dropping virtual avoids
// vtable overhead and simplifies the type hierarchy vs the geo benchmark.

#include <vector>
#include "views.hpp"
#include "workload.hpp"

namespace tpch::q12
{

// Structure 1: traditional indexes + merge join.
template <typename Backend>
struct BaseQ12 {
   Q12Workload<Backend>& w;
   explicit BaseQ12(Q12Workload<Backend>& w) : w(w) {}
   long query(std::vector<q12_agg_row_t>& out);
   double get_size() const;
};

// Structure 2: intermediate pipeline view.
template <typename Backend>
struct ViewQ12 {
   Q12Workload<Backend>& w;
   explicit ViewQ12(Q12Workload<Backend>& w) : w(w) {}
   long query(std::vector<q12_agg_row_t>& out);
   double get_size() const;
};

// Structure 3: MI[0] only (PremergedJoin at query time).
template <typename Backend>
struct MergedQ12 {
   Q12Workload<Backend>& w;
   explicit MergedQ12(Q12Workload<Backend>& w) : w(w) {}
   long query(std::vector<q12_agg_row_t>& out);
   double get_size() const;
};

// Structure 4: traditional indexes + hash join.
template <typename Backend>
struct HashQ12 {
   Q12Workload<Backend>& w;
   explicit HashQ12(Q12Workload<Backend>& w) : w(w) {}
   long query(std::vector<q12_agg_row_t>& out);
   double get_size() const;
};

}  // namespace tpch::q12
