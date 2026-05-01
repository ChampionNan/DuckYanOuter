#include "duckdb/optimizer/outer_yan/associative_query.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

bool AssociativeQuery::Holds(const OperatorTree &ot) {
	throw NotImplementedException("AssociativeQuery::Holds");
}

bool AssociativeQuery::PairSatisfiesAssociativity(const OTNode &parent, const OTNode &child) {
	throw NotImplementedException("AssociativeQuery::PairSatisfiesAssociativity");
}

} // namespace duckdb
