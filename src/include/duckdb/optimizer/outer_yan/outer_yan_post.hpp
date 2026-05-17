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
//! transforms, lowers to LogicalPlan via OJTToLogicalPlan, and as a final
//! step splices in LogicalSJBuild / LogicalSJProbe operators based on the
//! pre-decided pair lists on `OuterYanTree`.
//!
//! Step order (NEW per plan 3 — SJ insertion now runs on the rebuilt plan,
//! not on the OJT):
//!   1. Resimplify           — outer→original via arrow-side + eval-order.
//!   2. OrderFixApply        — verify / enforce evaluation order on OJT.
//!   3. OJTToLogicalPlan     — (shared utility) rebuild the LogicalPlan.
//!   4. SemijoinInsertOnPlan — wrap LogicalGets with SJBuild + SJProbe.
//!
//! Skipped entirely when the OuterYan-active flag is false.
class OuterYanPost {
public:
	explicit OuterYanPost(ClientContext &context);

	//! Consumes `tree.ojt`, runs OJT-stage transforms, lowers via
	//! `OJTToLogicalPlan`, then splices SJ operators onto the rebuilt
	//! LogicalPlan. Returns the rebuilt + spliced plan.
	unique_ptr<LogicalOperator> Optimize(OuterYanTree &tree);

	void Resimplify(OuterYanTree &tree);
	void OrderFixApply(OuterYanTree &tree, bool is_aggregation_query);
	//! Splice LogicalSJBuild / LogicalSJProbe onto the rebuilt LogicalPlan
	//! using `tree.{bottom_up_pairs, top_down_pairs}`. Plan 3 contract — the
	//! pass runs on the rebuilt plan, not on the OJT.
	unique_ptr<LogicalOperator> SemijoinInsertOnPlan(unique_ptr<LogicalOperator> plan, const OuterYanTree &tree);

private:
	ClientContext &context;
	Resimplification resimplification;
	SemijoinInsertion semijoin_insertion;
	OrderFix order_fix;
};

} // namespace duckdb
