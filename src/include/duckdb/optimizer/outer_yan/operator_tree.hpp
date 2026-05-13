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

namespace duckdb {

//! `OuterYanJoinKind` and `OuterYanFilterRecord` live in
//! `outer_yan_common.hpp`; they are shared by OperatorTree and
//! OrderedJoinTree.

//! OTNode — OperatorTree node, plain struct, NOT derived from LogicalOperator.
//!
//! Each node holds a raw pointer back into the original plan held by
//! OuterYanTree::source_plan; ownership of the underlying LogicalOperator
//! stays with source_plan for the OT's full lifetime.
//!
//! Two kinds:
//!   - JOIN     — wraps a LogicalComparisonJoin; binary children.
//!   - RELATION — wraps a base-relation subtree (LogicalGet, possibly under
//!                a chain of single-child LogicalFilter / LogicalProjection).
//!
//! All filters live in OuterYanTree::filter_records, never on the node — the
//! OT skeleton stays composed of joins and base relations only.
struct OTNode {
	enum class Kind : uint8_t { JOIN, RELATION };
	Kind kind;

	//! Raw pointer into OuterYanTree::source_plan. Never owned.
	LogicalOperator *origin = nullptr;

	// -- RELATION-only --
	idx_t relation_id = 0;

	// -- JOIN-only --
	OuterYanJoinKind join_kind = JoinType::INNER;
	//! Recorded before Simplification; Resimplification (on OJT) consults
	//! this to revert outer→outer when the chosen ordering allows it.
	OuterYanJoinKind original_join_kind = JoinType::INNER;
	//! Build order of this join in the original plan, identical convention
	//! to OJTEdge::order: 1-based, the deepest (closest-to-leaves) join gets
	//! `order = 1`, and `order` strictly increases up to the root join,
	//! which gets the largest value (== number of joins). Set by
	//! LogicalPlanToOT and read by OTToOJT.
	idx_t order = 0;
	//! Relations referenced by join.conditions[0] LHS and RHS respectively.
	//! Populated during LogicalPlanToOT by walking the condition expressions
	//! and resolving table_index → relation_id via OuterYanTree::table_to_relation.
	//! Used by OperatorTreeIsValid (parent/child common-relation check) and
	//! by OTToOJT (endpoint resolution without re-walking expressions).
	idx_t left_child_relation_id = DConstants::INVALID_INDEX;
	idx_t right_child_relation_id = DConstants::INVALID_INDEX;

	//! Binary tree. JOIN: both filled. RELATION: both null.
	//! children[0] corresponds to origin->children[0] in the original join.
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

	//! Build-side step run by `LogicalPlanToOT` after `relation_id` /
	//! `left_child_relation_id` / `right_child_relation_id` are set. Two
	//! responsibilities:
	//!   1. Populate every `OTNode::subtree_relations` (post-order union).
	//!   2. Canonicalise every JOIN so that the join condition's LHS
	//!      relation lives in `children[0]->subtree_relations` and the RHS
	//!      relation lives in `children[1]->subtree_relations`. When the
	//!      original `LogicalComparisonJoin::conditions[0]` is reversed,
	//!      swaps `cond.left` ↔ `cond.right` on the underlying join (a
	//!      semantic no-op for INNER/LEFT/RIGHT/FULL equi/theta conditions)
	//!      and swaps `left_child_relation_id` ↔ `right_child_relation_id`
	//!      on the OTNode. Children are never reordered; `join_kind` is
	//!      never changed.
	void Finalize();

	//! Structural validity check, intended for between-pass assertions.
	//! Verifies:
	//!   1. Tree shape: every node is JOIN (binary, both children present)
	//!      or RELATION (no children); origin non-null and of the matching
	//!      type.
	//!   2. Relation-id uniqueness across all RELATION OTNodes.
	//!   3. `subtree_relations` matches the recomputed union at every node.
	//!   4. For every JOIN node, the conditions[0] LHS relation equals
	//!      `left_child_relation_id` and resides in `children[0]`'s subset;
	//!      same for RHS / `right_child_relation_id` / `children[1]`.
	//!   5. Common-relation rule (KEY): for every (parent_join, child_join)
	//!      OTNode pair where child_join is directly under parent_join,
	//!      {parent.left_child_relation_id, parent.right_child_relation_id}
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
