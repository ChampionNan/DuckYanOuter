#include "duckdb/optimizer/outer_yan/outer_yan_post.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

namespace duckdb {

OuterYanPost::OuterYanPost(ClientContext &context_p) : context(context_p) {
}

unique_ptr<LogicalOperator> OuterYanPost::Optimize(OuterYanTree &tree) {
	// Pipeline: Resimplify → SemijoinInsert → OrderFixApply → OJTToLogicalPlan.
	throw NotImplementedException("OuterYanPost::Optimize");
}

void OuterYanPost::Resimplify(OuterYanTree &tree) {
	if (!tree.HasOJT()) {
		throw InternalException("OuterYanPost::Resimplify: tree has no OJT");
	}
	resimplification.Apply(tree.OJT());
}

void OuterYanPost::SemijoinInsert(OuterYanTree &tree) {
	if (!tree.HasOJT()) {
		throw InternalException("OuterYanPost::SemijoinInsert: tree has no OJT");
	}
	semijoin_insertion.Apply(tree.OJT());
}

void OuterYanPost::OrderFixApply(OuterYanTree &tree, bool is_aggregation_query) {
	if (!tree.HasOJT()) {
		throw InternalException("OuterYanPost::OrderFixApply: tree has no OJT");
	}
	order_fix.Apply(tree.OJT(), is_aggregation_query);
}

} // namespace duckdb
