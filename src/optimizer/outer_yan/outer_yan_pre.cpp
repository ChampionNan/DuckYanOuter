#include "duckdb/optimizer/outer_yan/outer_yan_pre.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OuterYanPre::OuterYanPre(ClientContext &context_p) : context(context_p) {
}

unique_ptr<LogicalOperator> OuterYanPre::Optimize(unique_ptr<LogicalOperator> plan) {
	// Pipeline: applicability gate -> Simplify -> Desimplify -> MarkAggregationRoot.
	throw NotImplementedException("OuterYanPre::Optimize");
}

ApplicabilityResult OuterYanPre::ApplicabilityCheck(const LogicalOperator &plan) {
	return OuterYanApplicability::Check(plan);
}

void OuterYanPre::Simplify(LogicalOperator &plan) {
	simplification.Apply(plan);
}

void OuterYanPre::Desimplify(LogicalOperator &plan) {
	desimplification.Apply(plan);
}

void OuterYanPre::MarkAggregationRoot(LogicalOperator &plan) {
	throw NotImplementedException("OuterYanPre::MarkAggregationRoot");
}

} // namespace duckdb
