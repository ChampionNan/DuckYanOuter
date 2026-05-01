#include "duckdb/optimizer/outer_yan/applicability.hpp"

namespace duckdb {

ApplicabilityResult OuterYanApplicability::Check(const LogicalOperator &plan) {
	// TODO: implement applicability gate per project plan §"Applicability gate":
	//   - all joins inner / LEFT/RIGHT/FULL outer,
	//   - join predicates null-rejecting and reference exactly two relations
	//     (equi or theta both allowed),
	//   - no semi/anti/mark joins, set ops, correlated subqueries,
	//   - acyclic (GYO ear-removal succeeds).
	ApplicabilityResult result;
	result.applicable = false;
	result.reason = "OuterYan applicability check not yet implemented";
	return result;
}

} // namespace duckdb
