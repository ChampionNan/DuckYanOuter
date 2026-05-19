#include "duckdb/optimizer/outer_yan/outer_yan_post.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

namespace duckdb {

OuterYanPost::OuterYanPost(ClientContext &context_p) : context(context_p) {
}

unique_ptr<LogicalOperator> OuterYanPost::Optimize(OuterYanTree &tree) {
	if (!tree.HasOJT()) {
		throw InternalException("OuterYanPost::Optimize: tree has no OJT");
	}

	// Pipeline: Resimplify → MarkAggPushdown → OJTToLogicalPlan →
	// SemijoinInsertOnPlan. Aggregation-driven root anchoring is already
	// enforced by OuterYanDP, so the OJT seen here is order-correct.
	Resimplify(tree);

	// Record the per-edge aggregate-pushdown gate while the OJT is still
	// live. Must run before OJTToLogicalPlan because the marker reads
	// `OJTEdge::has_pk_fk_constraint` / `parent_relation_id` / `order` off
	// the OJT.
	MarkAggPushdown(tree);

	// Lower the OJT to a fresh LogicalPlan. After this call, `tree.ojt` and
	// `tree.source_plan` are released — only `tree.{bottom_up_pairs,
	// top_down_pairs, table_to_relation, agg_pushdown_decisions}` remain
	// valid for SJ insertion and downstream aggregate pushdown.
	auto plan = tree.OJTToLogicalPlan(context);

	// Final pass: splice LogicalSJBuild + LogicalSJProbe wraps based on the
	// pre-decided pairs. Plan 3 contract — runs on the rebuilt LogicalPlan,
	// not on the OJT.
	plan = SemijoinInsertOnPlan(std::move(plan), tree);

	return plan;
}

void OuterYanPost::Resimplify(OuterYanTree &tree) {
	if (!tree.HasOJT()) {
		throw InternalException("OuterYanPost::Resimplify: tree has no OJT");
	}
	resimplification.Apply(tree.OJT());
}

void OuterYanPost::MarkAggPushdown(OuterYanTree &tree) {
	if (!tree.HasOJT()) {
		throw InternalException("OuterYanPost::MarkAggPushdown: tree has no OJT");
	}
	agg_pushdown.Apply(tree);
}

unique_ptr<LogicalOperator> OuterYanPost::SemijoinInsertOnPlan(unique_ptr<LogicalOperator> plan,
                                                               const OuterYanTree &tree) {
	return semijoin_insertion.Apply(std::move(plan), tree);
}

} // namespace duckdb
