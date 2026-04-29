#pragma once

// Shared per-storage-structure dispatch wrappers, parametrized by workload
// and aggregate-row type. All three Tier 1 queries (Q12, Q3, Q9) use the
// same structural shape: a plain struct holding a workload reference,
// forwarding query() and get_size() to the appropriate query_by_* method.
//
// Method bodies are defined inline here — they are identical for every
// query (pure forwarders). Per-query logic lives entirely in the
// Workload::query_by_* and Workload::get_size implementations.
//
// Design note: no virtual dispatch — structure selection happens at compile
// time in each executable's switch(FLAGS_storage_structure). See
// frontend/tpch/CLAUDE.md §Architectural Decisions.

#include <vector>

namespace tpch
{

template <typename Workload, typename AggRow>
struct BaseStructure {
   Workload& w;
   explicit BaseStructure(Workload& w) : w(w) {}
   long query(std::vector<AggRow>& out) { return w.query_by_base(out); }
   double get_size() const { return w.get_size(); }
};

template <typename Workload, typename AggRow>
struct ViewStructure {
   Workload& w;
   explicit ViewStructure(Workload& w) : w(w) {}
   long query(std::vector<AggRow>& out) { return w.query_by_view(out); }
   double get_size() const { return w.get_size(); }
};

template <typename Workload, typename AggRow>
struct MergedStructure {
   Workload& w;
   explicit MergedStructure(Workload& w) : w(w) {}
   long query(std::vector<AggRow>& out) { return w.query_by_merged(out); }
   double get_size() const { return w.get_size(); }
};

template <typename Workload, typename AggRow>
struct HashStructure {
   Workload& w;
   explicit HashStructure(Workload& w) : w(w) {}
   long query(std::vector<AggRow>& out) { return w.query_by_hash(out); }
   double get_size() const { return w.get_size(); }
};

}  // namespace tpch
