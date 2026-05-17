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

	// Pipeline (plan 3): Resimplify → OrderFixApply → OJTToLogicalPlan →
	// SemijoinInsertOnPlan.
	Resimplify(tree);

	// OrderFixApply requires the root-aggregation classification; the caller
	// of OuterYanPost::Optimize that has access to OuterYanTree's
	// `root_aggregation.type` should invoke OrderFixApply explicitly OR we
	// can derive `is_aggregation_query` here from tree.root_aggregation.
	const bool is_aggregation_query = tree.root_aggregation.type != OuterYanAggregationType::NONE;
	OrderFixApply(tree, is_aggregation_query);

	// Lower the OJT to a fresh LogicalPlan. After this call, `tree.ojt` and
	// `tree.source_plan` are released — only `tree.{bottom_up_pairs,
	// top_down_pairs, table_to_relation}` remain valid for SJ insertion.
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

void OuterYanPost::OrderFixApply(OuterYanTree &tree, bool is_aggregation_query) {
	if (!tree.HasOJT()) {
		throw InternalException("OuterYanPost::OrderFixApply: tree has no OJT");
	}
	order_fix.Apply(tree.OJT(), is_aggregation_query);
}

unique_ptr<LogicalOperator> OuterYanPost::SemijoinInsertOnPlan(unique_ptr<LogicalOperator> plan,
                                                               const OuterYanTree &tree) {
	return semijoin_insertion.Apply(std::move(plan), tree);
}

} // namespace duckdb
