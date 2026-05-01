#include "duckdb/optimizer/outer_yan/outer_yan_pre.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/tree_conversions.hpp"

namespace duckdb {

OuterYanPre::OuterYanPre(ClientContext &context_p) : context(context_p) {
}

unique_ptr<LogicalOperator> OuterYanPre::Optimize(unique_ptr<LogicalOperator> plan) {
	// Pipeline: applicability gate -> LogicalPlanToOT -> Simplify ->
	// Desimplify -> MarkAggregationRoot -> OTToLogicalPlan.
	throw NotImplementedException("OuterYanPre::Optimize");
}

ApplicabilityResult OuterYanPre::ApplicabilityCheck(const LogicalOperator &plan) {
	return OuterYanApplicability::Check(plan);
}

void OuterYanPre::Simplify(OperatorTree &ot) {
	simplification.Apply(ot);
}

void OuterYanPre::Desimplify(OperatorTree &ot) {
	desimplification.Apply(ot);
}

void OuterYanPre::MarkAggregationRoot(OperatorTree &ot) {
	throw NotImplementedException("OuterYanPre::MarkAggregationRoot");
}

} // namespace duckdb
