//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/agg_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"

namespace duckdb {

class OuterYanTree;
struct OJTNode;

//! AggPushdown — OJT-side marker, third step inside `OuterYanPost`.
//! Records one `OuterYanAggPushdownDecision` per OJT edge, sorted by
//! `OJTEdge::order` ascending, into `OuterYanTree::agg_pushdown_decisions`.
//!
//! Slice 1 scope (relation-level only): the marker emits the PK-FK gate
//! and nothing else. Every edge gets
//!
//!     push_into_child = !edge.has_pk_fk_constraint
//!
//! The `GROUP_BY_NUM` threshold cannot be decided here — true column
//! counts are only knowable after column pruning runs. That decision is
//! deferred to `AggregatePushdownOuter` (Slice 2), which speculates the
//! wrap at every candidate edge, runs the prune cycle, then unwraps the
//! aggregates whose surviving `groups.size()` exceeds the threshold.
//!
//! No-op when `tree.root_aggregation.type == OuterYanAggregationType::NONE`.
//!
//! Replaces the reference implementation's PK-FK detection
//! (`AggregationPushdown::CheckPKFK`, `aggregation_pushdown.cpp@38d165ee:
//! L547`). Data lives directly on `OJTEdge::has_pk_fk_constraint`.
class AggPushdown {
public:
	//! Populate `tree.agg_pushdown_decisions`. Idempotent: an existing
	//! vector is cleared before writing.
	void Apply(OuterYanTree &tree);

private:
	//! Recursive DFS over the OJT — appends one decision per outgoing edge
	//! before recursing into the child subtree. Order within `out` is
	//! arbitrary; `Apply` sorts by `edge.order` ascending afterwards.
	void Walk(OJTNode &node, vector<OuterYanAggPushdownDecision> &out);
};

} // namespace duckdb
