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
#include "duckdb/optimizer/outer_yan/simplification.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! OuterYanPre — first OuterYan pass.
//!
//! `Optimize` orchestrates the steps below. Operates directly on the
//! LogicalOperator tree — there is no separate Operator Tree IR.
//!
//! Step order:
//!   1. ApplicabilityCheck   — gate; clears the OuterYan flag on failure.
//!   2. Simplify             — outer→inner where null-rejecting predicates allow.
//!   3. Desimplify           — drive into associative-query state.
//!   4. MarkAggregationRoot  — tag the output relation as forced DP root
//!                             (aggregation queries only).
//!
//! Either OuterYanDP or the existing JOIN_ORDER consumes the result based
//! on the OuterYan-active flag.
class OuterYanPre {
public:
	explicit OuterYanPre(ClientContext &context);

	//! Top-level entry point: chains the per-step functions below.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	ApplicabilityResult ApplicabilityCheck(const LogicalOperator &plan);
	void Simplify(LogicalOperator &plan);
	void Desimplify(LogicalOperator &plan);
	void MarkAggregationRoot(LogicalOperator &plan);

private:
	ClientContext &context;
	Simplification simplification;
	Desimplification desimplification;
};

} // namespace duckdb
