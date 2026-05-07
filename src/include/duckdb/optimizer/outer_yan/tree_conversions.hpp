//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/tree_conversions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Shared tree-conversion utilities for OuterYan modules.
//!
//! All tree conversions happen here so the three OuterYan passes
//! (Pre / DP / Post) and any future passes don't reimplement them. This
//! enforces a single source of truth for round-trip semantics across
//! LogicalPlan ↔ OJT, and keeps the per-pass orchestration short.
//!
//! There is no separate Operator Tree IR: OuterYan operates directly on
//! `LogicalOperator` for tree-shape work and lifts to `OrderedJoinTree`
//! only for the join-edge / DP layer.

//! LogicalPlan → OrderedJoinTree (focuses on join + base-relation nodes).
//! Takes ownership of `plan`: the returned OJT holds raw pointers into
//! `plan` (`OJTEdge::join_op`, `OJTNode::base_op`) and stows the plan in
//! `OrderedJoinTree::source_plan` so those pointers stay valid for the
//! OJT's lifetime.
unique_ptr<OrderedJoinTree> LogicalPlanToOJT(unique_ptr<LogicalOperator> plan);

//! OrderedJoinTree → LogicalPlan. Realises the OJT structure back into a
//! `LogicalOperator` tree, deep-copying base-relation subtrees out of
//! `OJTNode::base_op` and reusing the join conditions referenced by
//! `OJTEdge::join_op`. `ojt->source_plan` is intentionally not consulted
//! — it is kept on the OJT only as a debug / printing aid for the
//! original plan shape, and may have diverged from the OJT after
//! transformations.
unique_ptr<LogicalOperator> OJTToLogicalPlan(ClientContext &context,
                                             unique_ptr<OrderedJoinTree> ojt);

} // namespace duckdb
