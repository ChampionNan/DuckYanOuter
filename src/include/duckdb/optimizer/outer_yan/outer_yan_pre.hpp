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
#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/simplification.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! OuterYanPre — first OuterYan pass.
//!
//! `Optimize` orchestrates the steps below. Tree conversions
//! (LogicalPlan ↔ OT) delegate to the shared utilities in
//! `tree_conversions.hpp`.
//!
//! Step order:
//!   1. ApplicabilityCheck   — gate; clears the OuterYan flag on failure.
//!   2. LogicalPlanToOT      — (shared utility).
//!   3. Simplify             — outer→inner where null-rejecting predicates allow.
//!   4. Desimplify           — drive into associative-query state.
//!   5. MarkAggregationRoot  — tag the output relation as forced DP root
//!                             (aggregation queries only).
//!   6. OTToLogicalPlan      — (shared utility).
//!
//! Either OuterYanDP or the existing JOIN_ORDER consumes the result based
//! on the OuterYan-active flag.
class OuterYanPre {
public:
	explicit OuterYanPre(ClientContext &context);

	//! Top-level entry point: chains the per-step functions below.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	ApplicabilityResult ApplicabilityCheck(const LogicalOperator &plan);
	void Simplify(OperatorTree &ot);
	void Desimplify(OperatorTree &ot);
	void MarkAggregationRoot(OperatorTree &ot);

private:
	ClientContext &context;
	Simplification simplification;
	Desimplification desimplification;
};

} // namespace duckdb
