//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/operator_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

//! `OuterYanJoinKind` and `OuterYanFilterRecord` live in
//! `outer_yan_common.hpp`; they are shared by OperatorTree and
//! OrderedJoinTree.

//! Owned, self-contained metadata for a JOIN OTNode. Survives OT reordering
//! and is moved into `OJTEdge::info` during BuildOJT. After that the original
//! `LogicalComparisonJoin` is never consulted again — conditions live here.
//!
//! Acyclic-CQ invariant: every condition is between the same two base
//! relations, so all `conditions[i].left` bindings reference
//! `cond_left_relation_id` and all `conditions[i].right` bindings reference
//! `cond_right_relation_id`.
struct OTJoin {
	OuterYanJoinKind join_kind = JoinType::INNER;
	//! Recorded before Simplification; Resimplification (on OJT) consults
	//! this to revert outer→outer when the chosen ordering allows it.
	OuterYanJoinKind original_join_kind = JoinType::INNER;
	//! Conditions deep-copied at OT construction from the original
	//! LogicalComparisonJoin. Owned here.
	vector<JoinCondition> conditions;
	//! Relations referenced by `conditions[i].left` / `.right` respectively.
	idx_t cond_left_relation_id = DConstants::INVALID_INDEX;
	idx_t cond_right_relation_id = DConstants::INVALID_INDEX;
	//! HLL-product cost statistic; populated by OuterYanDP::BuildEdgesFromOT.
	double distinct_count = 1.0;
};

//! OTNode — OperatorTree node, plain struct, NOT derived from LogicalOperator.
//!
//! RELATION nodes hold a raw pointer back into the original plan held by
//! OuterYanTree::source_plan; ownership of the underlying LogicalOperator
//! stays with source_plan for the OT's full lifetime.
//!
//! JOIN nodes do NOT reference the original LogicalComparisonJoin. All join
//! metadata (kind, conditions, cond_left/right_relation_id) lives in `info`,
//! deep-copied at BuildOT time. This keeps OT reordering decoupled from the
//! source-plan operator graph.
//!
//! Two kinds:
//!   - JOIN     — info populated; binary children.
//!   - RELATION — origin populated (LogicalGet, possibly under a chain of
//!                single-child LogicalFilter / LogicalProjection); no children.
//!
//! All filters live in OuterYanTree::filter_records, never on the node — the
//! OT skeleton stays composed of joins and base relations only.
struct OTNode {
	enum class Kind : uint8_t { JOIN, RELATION };
	Kind kind;

	// -- RELATION-only --
	//! Raw pointer into OuterYanTree::source_plan. Never owned.
	LogicalOperator *origin = nullptr;
	idx_t relation_id = 0;
	//! Cached at BuildOT time (via CheckPKFK on `origin`). Lets BuildOJT
	//! decide PK/FK side without re-walking the original subtree.
	bool is_pk = false;

	// -- JOIN-only --
	//! Owned. Moved into OJTEdge::info during BuildOJT; null afterwards.
	unique_ptr<OTJoin> info;

	//! Build order of this join in the original plan, identical convention
	//! to OJTEdge::order: 1-based, the deepest (closest-to-leaves) join gets
	//! `order = 1`, and `order` strictly increases up to the root join,
	//! which gets the largest value (== number of joins). Set by
	//! LogicalPlanToOT and read by OTToOJT. RELATION nodes leave it at 0.
	idx_t order = 0;

	//! Binary tree. JOIN: both filled. RELATION: both null.
	unique_ptr<OTNode> children[2];

	//! Relation IDs contained in this subtree. For RELATION, equals
	//! `{relation_id}`. For JOIN, equals the union of `children[0]` and
	//! `children[1]` subsets. Populated by `OperatorTree::Finalize` and
	//! asserted by `IsValid`. Stable under kind-only mutations performed by
	//! Simplification / Desimplification / Resimplification.
	unordered_set<idx_t> subtree_relations;

	//! Per-relation statistics used by OuterYanDP's cost model. Populated for
	//! RELATION nodes by `OuterYanDP::BuildLeafRelations` via
	//! `RelationStatisticsHelper::ExtractGetStats` (walking past single-child
	//! LogicalFilter / LogicalProjection wrappers). Default-constructed (and
	//! `stats_initialized = false`) on JOIN nodes; DP does not consult JOIN-
	//! node stats.
	RelationStats stats;
};

//! OperatorTree — first IR of the OuterYan pipeline. Pure structure: only
//! the root OTNode tree. Shared metadata (source_plan, filter_records,
//! applicability, table_to_relation, indices) lives on `OuterYanTree`.
class OperatorTree {
public:
	OperatorTree() = default;

	unique_ptr<OTNode> root;

	OTNode &Root() {
		return *root;
	}
	const OTNode &Root() const {
		return *root;
	}

	//! Build-side step run by `LogicalPlanToOT` after `relation_id` and the
	//! JOIN-side `info->cond_left_relation_id` / `cond_right_relation_id` are
	//! set. Two responsibilities:
	//!   1. Populate every `OTNode::subtree_relations` (post-order union).
	//!   2. Canonicalise every JOIN so that the join condition's LHS
	//!      relation lives in `children[0]->subtree_relations` and the RHS
	//!      relation lives in `children[1]->subtree_relations`. When the
	//!      copied `info->conditions[0]` is reversed, swaps `cond.left` ↔
	//!      `cond.right` on the owned condition (a semantic no-op for
	//!      INNER/LEFT/RIGHT/FULL equi/theta conditions) and swaps
	//!      `cond_left_relation_id` ↔ `cond_right_relation_id` on `info`.
	//!      Children are never reordered; `join_kind` is never changed.
	void Finalize();

	//! Structural validity check, intended for between-pass assertions.
	//! Verifies:
	//!   1. Tree shape: every node is JOIN (info non-null, binary, both
	//!      children present) or RELATION (origin non-null, no children).
	//!   2. Relation-id uniqueness across all RELATION OTNodes.
	//!   3. `subtree_relations` matches the recomputed union at every node.
	//!   4. For every JOIN node, the info->conditions[0] LHS relation
	//!      equals `info->cond_left_relation_id` and resides in
	//!      `children[0]`'s subset; same for RHS / `cond_right_relation_id`
	//!      / `children[1]`.
	//!   5. Common-relation rule (KEY): for every (parent_join, child_join)
	//!      OTNode pair where child_join is directly under parent_join,
	//!      {parent.info->cond_left_relation_id, parent.info->cond_right_relation_id}
	//!      and child_join's `subtree_relations` share at least one
	//!      relation_id. Empty intersection ⇒ implicit cross product ⇒
	//!      invalid OT.
	//! On failure, writes a diagnostic into *reason (if non-null) and
	//! returns false; caller decides whether to throw.
	bool IsValid(string *reason = nullptr) const;
};

//! For inspecting an OT, convert to OJT via `OTToOJT` and use `PrintOJT` /
//! `PrintOJTAsTree` (defined in `ordered_join_tree.hpp`).

} // namespace duckdb
