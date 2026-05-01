#include "duckdb/optimizer/outer_yan/semijoin_insertion.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void SemijoinInsertion::Apply(OrderedJoinTree &ojt) {
	// Two passes per Yannakakis-style reduction:
	//   1. bottom-up post-order over edges
	//   2. top-down pre-order over edges (reverse of pass 1)
	throw NotImplementedException("SemijoinInsertion::Apply");
}

void SemijoinInsertion::BottomUpPass(OJTNode &node) {
	throw NotImplementedException("SemijoinInsertion::BottomUpPass");
}

void SemijoinInsertion::TopDownPass(OJTNode &node) {
	throw NotImplementedException("SemijoinInsertion::TopDownPass");
}

void SemijoinInsertion::InsertOnEdge(OJTNode &parent, OJTEdge &edge) {
	throw NotImplementedException("SemijoinInsertion::InsertOnEdge");
}

} // namespace duckdb
