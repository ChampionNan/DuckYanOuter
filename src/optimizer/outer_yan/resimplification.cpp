#include "duckdb/optimizer/outer_yan/resimplification.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void Resimplification::Apply(OrderedJoinTree &ojt) {
	throw NotImplementedException("Resimplification::Apply");
}

bool Resimplification::TryRevert(OJTNode &parent, OJTEdge &edge) {
	throw NotImplementedException("Resimplification::TryRevert");
}

} // namespace duckdb
