//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/semijoin_insertion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"

namespace duckdb {

//! Semi-join (SJBuild / SJProbe) insertion pass.
//!
//! The decision-making (which edges trigger which reductions) is done
//! upstream by `OuterYanPre::BottomUpPass` / `TopDownPass`, which populate
//! `OuterYanTree::bottom_up_pairs` and `top_down_pairs` from the
//! post-simplification OJT before Desimplification mutates join kinds. This
//! pass is now purely materialisation: consume those two vectors in order
//! and emit, for every `OuterYanSemiPair`, a `LogicalSJBuild` on the
//! `build` relation plus a matching `LogicalSJProbe` on the `probe`
//! relation. Pairs are bound through a shared `HashFilter` shared_ptr (RPT
//! pattern); the probe additionally records a `related_sj_build`
//! back-pointer.
class SemijoinInsertion {
public:
	void Apply(OrderedJoinTree &ojt);

private:
	//! Materialise one `OuterYanSemiPair` into SJBuild + SJProbe operators
	//! inside the OJT (or its underlying logical plan). Implementation TBD.
	void InsertPair(OrderedJoinTree &ojt, const OuterYanSemiPair &pair);
};

} // namespace duckdb
