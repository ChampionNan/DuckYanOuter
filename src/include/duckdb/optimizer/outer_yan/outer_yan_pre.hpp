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
class ClientContext;

//! OuterYanPre — first OuterYan pass.
//!
//! `Optimize` lifts the LogicalPlan into `tree.ot` and runs the OT-stage
//! transforms. The same `OuterYanTree` is handed off to OuterYanDP and
//! OuterYanPost.
//!
//! Step order:
//!   1. ApplicabilityCheck   — gate; clears the OuterYan flag on failure.
//!   2. LogicalPlanToOT      — lift to OperatorTree (filters → tree.filter_records).
//!   3. Simplify             — outer→inner where null-rejecting predicates allow.
//!   4. Desimplify           — drive into associative-query state.
//!   5. MarkAggregationRoot  — tag the output relation as forced DP root
//!                             (aggregation queries only).
class OuterYanPre {
public:
	explicit OuterYanPre(ClientContext &context);

	//! Top-level entry point: chains the per-step functions below.
	void Optimize(unique_ptr<LogicalOperator> plan, OuterYanTree &tree);

	ApplicabilityResult ApplicabilityCheck(LogicalOperator &plan);
	void Simplify(OuterYanTree &tree);
	void Desimplify(OuterYanTree &tree);
	void MarkAggregationRoot(OuterYanTree &tree);

private:
	ClientContext &context;
	Simplification simplification;
	Desimplification desimplification;
};

} // namespace duckdb
