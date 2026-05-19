//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_post.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/agg_pushdown.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/optimizer/outer_yan/resimplification.hpp"
#include "duckdb/optimizer/outer_yan/semijoin_insertion.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! OuterYanPost — third OuterYan pass (runs after OuterYanDP).
//!
//! `Optimize` consumes the OJT produced by OuterYanDP, runs the OJT-stage
//! transforms, records the aggregate-pushdown decision vector, lowers to
//! LogicalPlan via OJTToLogicalPlan, and as a final step splices in
//! LogicalSJBuild / LogicalSJProbe operators based on the pre-decided pair
//! lists on `OuterYanTree`.
//!
//! Step order:
//!   1. Resimplify           — outer→original via arrow-side + eval-order.
//!   2. MarkAggPushdown      — record per-edge `OuterYanAggPushdownDecision`
//!                             on the OJT (Slice 1 of plan 4: relation-level
//!                             PK-FK gate; threshold check deferred to the
//!                             plan-side `AggregatePushdownOuter`).
//!   3. OJTToLogicalPlan     — (shared utility) rebuild the LogicalPlan.
//!   4. SemijoinInsertOnPlan — wrap LogicalGets with SJBuild + SJProbe.
//!
//! The former `OrderFixApply` step is gone: aggregation-driven root anchoring
//! is now enforced upstream by `OuterYanDP` (`FIX_ALL_OUTPUTS` /
//! `FIX_ONE_OUTPUT` regimes), so the OJT handed to this pass already carries
//! the correct evaluation order.
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
	//! Populate `tree.agg_pushdown_decisions` for later consumption by
	//! `AggregatePushdownOuter` on the rebuilt plan.
	void MarkAggPushdown(OuterYanTree &tree);
	//! Splice LogicalSJBuild / LogicalSJProbe onto the rebuilt LogicalPlan
	//! using `tree.{bottom_up_pairs, top_down_pairs}`. Plan 3 contract — the
	//! pass runs on the rebuilt plan, not on the OJT.
	unique_ptr<LogicalOperator> SemijoinInsertOnPlan(unique_ptr<LogicalOperator> plan, const OuterYanTree &tree);

private:
	ClientContext &context;
	Resimplification resimplification;
	SemijoinInsertion semijoin_insertion;
	AggPushdown agg_pushdown;
};

} // namespace duckdb
