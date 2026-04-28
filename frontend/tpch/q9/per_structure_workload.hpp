#pragma once

// Per-storage-structure dispatch wrappers for Q9.
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

namespace tpch::q9
{

// Structure 1: traditional indexes + merge join (5 merge joins).
template <typename Backend>
struct BaseQ9 {
   Q9Workload<Backend>& w;
   explicit BaseQ9(Q9Workload<Backend>& w) : w(w) {}
   long query(std::vector<q9_agg_row_t>& out);
   double get_size() const;
};

// Structure 2: intermediate pipeline view (OL join materialized).
template <typename Backend>
struct ViewQ9 {
   Q9Workload<Backend>& w;
   explicit ViewQ9(Q9Workload<Backend>& w) : w(w) {}
   long query(std::vector<q9_agg_row_t>& out);
   double get_size() const;
};

// Structure 3: MI[0] only (PremergedJoin + 4 merge joins / hash lookups).
template <typename Backend>
struct MergedQ9 {
   Q9Workload<Backend>& w;
   explicit MergedQ9(Q9Workload<Backend>& w) : w(w) {}
   long query(std::vector<q9_agg_row_t>& out);
   double get_size() const;
};

// Structure 4: traditional indexes + hash join (5 hash joins).
template <typename Backend>
struct HashQ9 {
   Q9Workload<Backend>& w;
   explicit HashQ9(Q9Workload<Backend>& w) : w(w) {}
   long query(std::vector<q9_agg_row_t>& out);
   double get_size() const;
};

}  // namespace tpch::q9
