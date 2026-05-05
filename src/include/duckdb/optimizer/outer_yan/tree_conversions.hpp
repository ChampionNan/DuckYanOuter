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
//! The resulting OJT references join nodes inside `plan` via
//! `OJTEdge::join_op`; `plan` must outlive the returned OJT.
unique_ptr<OrderedJoinTree> LogicalPlanToOJT(const LogicalOperator &plan);

//! OrderedJoinTree → LogicalPlan. Realises the OJT structure back into a
//! `LogicalOperator` tree, reusing the join operators referenced by
//! `OJTEdge::join_op`.
unique_ptr<LogicalOperator> OJTToLogicalPlan(unique_ptr<OrderedJoinTree> ojt);

} // namespace duckdb
