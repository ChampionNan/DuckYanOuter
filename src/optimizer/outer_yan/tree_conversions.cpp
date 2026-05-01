#include "duckdb/optimizer/outer_yan/tree_conversions.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

unique_ptr<OperatorTree> LogicalPlanToOT(unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("LogicalPlanToOT");
}

unique_ptr<LogicalOperator> OTToLogicalPlan(unique_ptr<OperatorTree> ot) {
	throw NotImplementedException("OTToLogicalPlan");
}

unique_ptr<OrderedJoinTree> OTToOJT(const OperatorTree &ot) {
	throw NotImplementedException("OTToOJT");
}

unique_ptr<OperatorTree> OJTToOT(const OrderedJoinTree &ojt) {
	throw NotImplementedException("OJTToOT");
}

} // namespace duckdb
