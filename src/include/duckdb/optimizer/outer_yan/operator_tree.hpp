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
#include "duckdb/common/vector.hpp"
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
	//! Recorded before Desimplification; Resimplification (on OJT) consults
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

	//! Structural validity check, intended for between-pass assertions.
	//! Verifies:
	//!   1. Tree shape: every node is JOIN (binary, both children present)
	//!      or RELATION (no children); origin non-null and of the matching
	//!      type.
	//!   2. Relation-id uniqueness across all RELATION OTNodes.
	//!   3. Common-relation rule (KEY): for every (parent_join, child_join)
	//!      OTNode pair where child_join is directly under parent_join,
	//!      {parent.left_child_relation_id, parent.right_child_relation_id}
	//!      and {child.left_child_relation_id, child.right_child_relation_id}
	//!      share at least one relation_id. Empty intersection ⇒ implicit
	//!      cross product ⇒ invalid OT.
	//! On failure, writes a diagnostic into *reason (if non-null) and
	//! returns false; caller decides whether to throw. Coverage of
	//! applicability.table_index_to_relation is checked at the
	//! `OuterYanTree` level (since that map lives there now).
	bool IsValid(string *reason = nullptr) const;
};

//! For inspecting an OT, convert to OJT via `OTToOJT` and use `PrintOJT` /
//! `PrintOJTAsTree` (defined in `ordered_join_tree.hpp`).

} // namespace duckdb
