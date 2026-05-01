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
//! Two passes over the OJT, per Yannakakis-style semi-join reduction:
//!
//!   1. **Bottom-up pass** — post-order traversal of OJT edges. For each
//!      edge, dispatch on join type and insert SJBuild / SJProbe operators
//!      into the underlying logical plan accordingly.
//!   2. **Top-down pass** — pre-order traversal of OJT edges (reverse of
//!      pass 1). Same per-edge insertion logic, applied in the opposite
//!      direction.
//!
//! After both passes, every join is dangling-free. SJBuild / SJProbe pairs
//! are bound through a shared `HashFilter` shared_ptr (RPT pattern); the
//! probe additionally records a `related_sj_build` back-pointer.
class SemijoinInsertion {
public:
	void Apply(OrderedJoinTree &ojt);

private:
	//! Pass 1: bottom-up (post-order) traversal of edges.
	void BottomUpPass(OJTNode &node);
	//! Pass 2: top-down (pre-order) traversal of edges.
	void TopDownPass(OJTNode &node);

	//! Per-edge insertion, dispatched on join type. Shared between both passes.
	void InsertOnEdge(OJTNode &parent, OJTEdge &edge);
};

} // namespace duckdb
