//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/resimplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"

namespace duckdb {

//! Reverse direction of Desimplification: where the chosen ordering plus
//! arrow-side (preserved/null-supplying) analysis shows an outer join is now
//! redundant, revert it back to its original join type recorded in
//! `OJTEdge::original_kind`. Operates on OJT post-DP because evaluation
//! order is meaningful at this stage.
class Resimplification {
public:
	void Apply(OrderedJoinTree &ojt);

private:
	//! Considers two adjacent join operators in OJT post-order. Inputs are
	//! arrow side of each plus the evaluation order from OJT. When safe,
	//! resets `edge.kind` to `edge.original_kind`.
	bool TryRevert(OJTNode &parent, OJTEdge &edge);
};

} // namespace duckdb
