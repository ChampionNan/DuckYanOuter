//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_post.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/evaluation_order.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/optimizer/outer_yan/resimplification.hpp"
#include "duckdb/optimizer/outer_yan/semijoin_insertion.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! OuterYanPost — third OuterYan pass (runs after OuterYanDP).
//!
//! `Optimize` consumes the OJT produced by OuterYanDP, runs the OJT-stage
//! transforms, and lowers to LogicalPlan via the shared utilities in
//! `tree_conversions.hpp`.
//!
//! Step order:
//!   1. Resimplify        — outer→original via arrow-side + eval-order.
//!   2. SemijoinInsert    — bottom-up + top-down passes inserting
//!                          SJBuild / SJProbe pairs.
//!   3. OrderFixApply     — verify / enforce evaluation order on OJT.
//!   4. OJTToLogicalPlan  — (shared utility).
//!
//! Skipped entirely when the OuterYan-active flag is false.
class OuterYanPost {
public:
	explicit OuterYanPost(ClientContext &context);

	//! Consumes `tree.ojt`, runs OJT-stage transforms, and lowers via
	//! `OJTToLogicalPlan` to produce the rebuilt LogicalPlan.
	unique_ptr<LogicalOperator> Optimize(OuterYanTree &tree);

	void Resimplify(OuterYanTree &tree);
	void SemijoinInsert(OuterYanTree &tree);
	void OrderFixApply(OuterYanTree &tree, bool is_aggregation_query);

private:
	ClientContext &context;
	Resimplification resimplification;
	SemijoinInsertion semijoin_insertion;
	OrderFix order_fix;
};

} // namespace duckdb
