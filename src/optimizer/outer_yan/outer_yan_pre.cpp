#include "duckdb/optimizer/outer_yan/outer_yan_pre.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OuterYanPre::OuterYanPre(ClientContext &context_p) : context(context_p) {
}

void OuterYanPre::Optimize(unique_ptr<LogicalOperator> plan, OuterYanTree &tree) {
	// Pipeline: LogicalPlanToOT → Simplify → Desimplify → MarkAggregationRoot.
	throw NotImplementedException("OuterYanPre::Optimize");
}

ApplicabilityResult OuterYanPre::ApplicabilityCheck(LogicalOperator &plan) {
	return OuterYanApplicability::Check(plan);
}

void OuterYanPre::Simplify(OuterYanTree &tree) {
	simplification.Apply(tree);
}

void OuterYanPre::Desimplify(OuterYanTree &tree) {
	desimplification.Apply(tree);
}

void OuterYanPre::MarkAggregationRoot(OuterYanTree &tree) {
	throw NotImplementedException("OuterYanPre::MarkAggregationRoot");
}

} // namespace duckdb
