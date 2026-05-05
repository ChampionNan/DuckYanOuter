#include "duckdb/optimizer/outer_yan/outer_yan_post.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/tree_conversions.hpp"

namespace duckdb {

OuterYanPost::OuterYanPost(ClientContext &context_p) : context(context_p) {
}

unique_ptr<LogicalOperator> OuterYanPost::Optimize(unique_ptr<LogicalOperator> plan) {
	// Pipeline: LogicalPlanToOJT -> Resimplify -> SemijoinInsert
	// -> OrderFixApply -> OJTToLogicalPlan.
	throw NotImplementedException("OuterYanPost::Optimize");
}

void OuterYanPost::Resimplify(OrderedJoinTree &ojt) {
	resimplification.Apply(ojt);
}

void OuterYanPost::SemijoinInsert(OrderedJoinTree &ojt) {
	semijoin_insertion.Apply(ojt);
}

void OuterYanPost::OrderFixApply(OrderedJoinTree &ojt, bool is_aggregation_query) {
	order_fix.Apply(ojt, is_aggregation_query);
}

} // namespace duckdb
