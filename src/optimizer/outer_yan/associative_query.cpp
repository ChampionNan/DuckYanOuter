#include "duckdb/optimizer/outer_yan/associative_query.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

bool AssociativeQuery::Holds(const LogicalOperator &plan) {
	throw NotImplementedException("AssociativeQuery::Holds");
}

bool AssociativeQuery::PairSatisfiesAssociativity(const LogicalOperator &parent, const LogicalOperator &child) {
	throw NotImplementedException("AssociativeQuery::PairSatisfiesAssociativity");
}

} // namespace duckdb
