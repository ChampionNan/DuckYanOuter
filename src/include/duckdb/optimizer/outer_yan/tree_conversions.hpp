//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/tree_conversions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Shared tree-conversion utilities for OuterYan modules.
//!
//! All tree conversions happen here so the three OuterYan passes
//! (Pre / DP / Post) and any future passes don't reimplement them. This
//! enforces a single source of truth for round-trip semantics across
//! LogicalPlan ↔ OT ↔ OJT, and keeps the per-pass orchestration short.

//! LogicalPlan → OperatorTree.
unique_ptr<OperatorTree> LogicalPlanToOT(unique_ptr<LogicalOperator> plan);

//! OperatorTree → LogicalPlan.
unique_ptr<LogicalOperator> OTToLogicalPlan(unique_ptr<OperatorTree> ot);

//! OperatorTree → OrderedJoinTree (focuses on join + base-relation nodes).
unique_ptr<OrderedJoinTree> OTToOJT(const OperatorTree &ot);

//! OrderedJoinTree → OperatorTree.
unique_ptr<OperatorTree> OJTToOT(const OrderedJoinTree &ojt);

} // namespace duckdb
