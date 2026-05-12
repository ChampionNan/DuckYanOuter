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
//!   4. RecordSemiJoinPairs  — build a transient OJT against the
//!                             post-simplification OT and populate
//!                             `tree.bottom_up_pairs` / `top_down_pairs`
//!                             via `BottomUpPass` and `TopDownPass`. Done
//!                             here so the recorded join kinds reflect the
//!                             user-intended outer/inner semantics, before
//!                             Desimplification's synthetic relabels.
//!   5. Desimplify           — drive into associative-query state.
//!   6. AllPairsSatisfy      — post-condition assertion: every (P, D) pair
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
	//! Phase 1 (post-order over OJT edges). For every non-root child R_i with
	//! parent R_p, if `edge.kind ∈ {RIGHT, INNER}` append an entry to
	//! `tree.bottom_up_pairs` with `build = R_i` and `probe = R_p`.
	//! Pseudocode: R_p := R_p ⋉ R_i.
	void BottomUpPass(OuterYanTree &tree, OJTNode &node);

	//! Phase 2 (pre-order over OJT edges, i.e. reverse post-order). For every
	//! non-root child R_i with parent R_p, if `edge.kind ∈ {LEFT, INNER}`
	//! append an entry to `tree.top_down_pairs` with `build = R_p` and
	//! `probe = R_i`. Pseudocode: R_i := R_i ⋉ R_p.
	void TopDownPass(OuterYanTree &tree, OJTNode &node);

	//! Resolve a `LogicalComparisonJoin`'s conditions into per-key
	//! build/probe `ColumnBinding`s using `tree.table_to_relation`. Each
	//! condition's two sides bind to exactly one of {build_rel, probe_rel}
	//! thanks to the applicability gate (two-relation equi conditions only).
	void ExtractSemiKeys(OuterYanTree &tree, RelationId build_rel,
	                     RelationId probe_rel, const OJTEdge &edge,
	                     OuterYanSemiPair &out);

	Simplification simplification;
	Desimplification desimplification;
};

} // namespace duckdb
