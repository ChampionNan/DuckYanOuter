//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_pre.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/applicability.hpp"
#include "duckdb/optimizer/outer_yan/desimplification.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/optimizer/outer_yan/simplification.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! OuterYanPre — first OuterYan pass.
//!
//! `Optimize` lifts the LogicalPlan into `tree.ot` and runs the OT-stage
//! transforms. The same `OuterYanTree` is handed off to OuterYanDP and
//! OuterYanPost.
//!
//! Step order:
//!   1. ApplicabilityCheck   — gate; clears the OuterYan flag on failure
//!                             (invoked by the caller before `Optimize`).
//!   2. LogicalPlanToOT      — lift to OperatorTree; also classifies the root
//!                             LogicalAggregate (if any) into
//!                             `tree.root_aggregation`, replacing the former
//!                             standalone MarkAggregationRoot step.
//!   3. Simplify             — outer→inner where null-rejecting predicates allow.
//!   4. Desimplify           — drive into associative-query state.
//!   5. AllPairsSatisfy      — post-condition assertion: every (P, D) pair
//!                             now sits in an OK cell of the associativity
//!                             table.
class OuterYanPre {
public:
	OuterYanPre() = default;

	//! Top-level entry point: chains the per-step functions below.
	void Optimize(unique_ptr<LogicalOperator> plan, OuterYanTree &tree);

	ApplicabilityResult ApplicabilityCheck(LogicalOperator &plan);
	void Simplify(OuterYanTree &tree);
	void Desimplify(OuterYanTree &tree);

private:
	Simplification simplification;
	Desimplification desimplification;
};

} // namespace duckdb
