//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/resimplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"

namespace duckdb {

//! Reverse direction of Desimplification: where the chosen ordering plus
//! arrow-side analysis shows an outer join is now redundant, revert its
//! kind back to `OJTEdge::original_kind`. Runs on the OJT post-DP.
//!
//! Rule (adjacency formulation). A de-simplified $\Diamond_i$ (i.e.
//! `kind != original_kind`) is reverted iff some directly adjacent OJT edge
//! $\Diamond_k$ satisfies
//!   (i)  `\Diamond_k.order > \Diamond_i.order`, and
//!   (ii) `\Diamond_k`'s arrow targets the node shared with $\Diamond_i$.
//!
//! Orientation: place the shared node `X` on the right of $\Diamond_k$; the
//! arrow lies on the right iff the effective kind is LEFT or OUTER.
//!   - `X == \Diamond_k.child->relation_id`  -> `\Diamond_k.kind in {LEFT, OUTER}`.
//!   - `X == \Diamond_k.parent_relation_id`  -> `\Diamond_k.kind in {RIGHT, OUTER}`.
//!
//! Checking the shared node alone is sufficient: by OJT order monotonicity,
//! $\Diamond_i$'s other endpoint already merged into the same input of
//! $\Diamond_k$ before $\Diamond_k$ evaluates, so both endpoints share the
//! arrow side automatically.
//!
//! `Apply` iterates to fixpoint in ascending-order passes.
class Resimplification {
public:
	void Apply(OrderedJoinTree &ojt);

private:
	//! Post-order walk populating `all_edges`, `node_by_rel`,
	//! `parent_edge_by_rel`.
	void IndexOJT(OJTNode &node);

	//! Reverts `i_edge` if a qualifying adjacent $\Diamond_k$ exists.
	bool TryRevert(OJTEdge &i_edge);

	//! True iff `k_edge`'s arrow lies on the side of its endpoint
	//! `shared_rel`. The boolean `shared_is_child` derived from
	//! `k_edge.child->relation_id` carries the full orientation since
	//! `k_edge.kind` is already in "parent KIND child" form.
	static bool ArrowTargetsSharedNode(const OJTEdge &k_edge, idx_t shared_rel);

	//! Per-Apply caches; cleared on entry.
	vector<OJTEdge *> all_edges;
	unordered_map<idx_t, OJTNode *> node_by_rel;
	unordered_map<idx_t, OJTEdge *> parent_edge_by_rel;
};

} // namespace duckdb
