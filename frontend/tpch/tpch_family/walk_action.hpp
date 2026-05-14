#pragma once

// Uniform return type for group-walk visitor hooks (col_group_walk,
// coli_group_walk, and their fused-emit variants).
//
// Every on_<record> hook in a group-walk visitor returns WalkAction.
// Hooks that have nothing to skip return Continue unconditionally —
// the uniformity is the point: any hook can short-circuit the walk in
// the same vocabulary.
//
// Per-hook semantics:
//
//   on_customer / on_invoice (no order yet open):
//     Continue   → dispatch the next record normally
//     SkipGroup  → skip the rest of this custkey group (may seek-skip)
//     SkipOrder  → ILLEGAL — walker throws std::logic_error.  No order is
//                  open at this point in the byte-lex scan, so order-skip
//                  is meaningless.  Use SkipGroup instead.
//
//   on_order:
//     Continue   → process this order's lineitems
//     SkipOrder  → forward-iterate past this order's lineitems and
//                  resume with the next order in the same group
//     SkipGroup  → skip the rest of this custkey group
//
//   on_lineitem:
//     Continue   → accumulate this lineitem
//     SkipOrder  → skip the rest of this order's lineitems
//     SkipGroup  → skip the rest of this custkey group
//
// Replaces the legacy wants_skip_group() / wants_skip_order() poll
// hooks (retired 2026-05-09; see PLAYBOOK anti-pattern #19).

#include <cstdint>

namespace tpch
{

enum class WalkAction : uint8_t {
   Continue,
   SkipOrder,
   SkipGroup,
};

}  // namespace tpch
