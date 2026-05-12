#include "duckdb/optimizer/outer_yan/semijoin_insertion.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void SemijoinInsertion::Apply(OrderedJoinTree &ojt) {
	// Consume `tree.bottom_up_pairs` then `tree.top_down_pairs` in order;
	// for each pair, emit a LogicalSJBuild + LogicalSJProbe bound via a
	// shared HashFilter.
	throw NotImplementedException("SemijoinInsertion::Apply");
}

void SemijoinInsertion::InsertPair(OrderedJoinTree &ojt, const OuterYanSemiPair &pair) {
	throw NotImplementedException("SemijoinInsertion::InsertPair");
}

} // namespace duckdb
