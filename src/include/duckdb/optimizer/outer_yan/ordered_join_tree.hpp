//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/ordered_join_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

//! `OuterYanJoinKind` (defined in operator_tree.hpp) is the single edge-kind
//! type shared by OperatorTree and OrderedJoinTree. The OJT-edge orientation
//! convention "parent KIND child" still applies: when the OJT places the
//! original RHS as parent, `LEFT_OUTER` and `RIGHT_OUTER` are flipped so the
//! kind continues to label the OJT-parent's role. `INNER` and `FULL_OUTER`
//! are symmetric and unaffected. See `CheckOuterYanJoinFlip` in
//! tree_conversions.cpp for the single source of truth.

struct OJTNode;

//! Edge from an OJTNode to a child subtree.
//!
//! Semijoin insertion is two whole-OJT passes with fixed directions:
//!   1. Bottom-up — every edge reduces child→parent.
//!   2. Top-down  — every edge reduces parent→child.
//! Each pass adds at most one SJ pair per edge, so an edge carries at most
//! two reductions total. Presence is recorded by the `bottom_up` /
//! `top_down` markers below; the actual `LogicalSJBuild` / `LogicalSJProbe`
//! operators are inserted later by the SemijoinInsertion module when
//! lowering to LogicalPlan, driven by these markers.
struct OJTEdge {
	//! Current edge classification (may have been transformed by Desimplification).
	OuterYanJoinKind kind = JoinType::INNER;
	//! Original edge classification, recorded before Desimplification rules were
	//! applied. Resimplification uses this to revert back to the original join
	//! type when the chosen ordering makes the rewrite redundant.
	OuterYanJoinKind original_kind = JoinType::INNER;
	//! Build order of the underlying join in the original logical plan.
	//! 1-based, starting from the bottommost join (`order = 1`) up to the
	//! root join (`order = n`). Derived from a post-order walk of the
	//! original plan during `LogicalPlanToOJT`. 0 means uninitialised.
	idx_t order = 0;
	//! Owned join metadata (kind, original_kind, conditions,
	//! cond_left/right_relation_id, distinct_count). Moved out of the
	//! originating OT JOIN OTNode during BuildOJT. Replaces the prior
	//! `LogicalOperator *join_op`: the OJT is now self-contained and no
	//! longer reaches into the source plan for condition information.
	unique_ptr<OTJoin> info;
	unique_ptr<OJTNode> child;

	//! Bottom-up round marker (child→parent): set iff the round inserted
	//! an SJBuild + SJProbe pair on this edge.
	optional<bool> bottom_up;
	//! Top-down round marker (parent→child): set iff the round inserted
	//! an SJBuild + SJProbe pair on this edge.
	optional<bool> top_down;

	//! Whether the edge carries a PK-FK key constraint, derived from the
	//! underlying logical plan: walk down to the base `LogicalGet` (through
	//! a single-child `LogicalFilter` if present) and check whether its
	//! `TableCatalogEntry` exposes a `UniqueConstraint` covering the join
	//! key bindings on that side. See
	//! `AggregationPushdown::CheckPKFK` in DuckDBYanPlus
	//! (src/optimizer/aggregation_pushdown.cpp ~L547) for the reference
	//! implementation: bindings → `LogicalGet::GetColumnIds()` →
	//! `TableCatalogEntry::GetConstraints()` → `ConstraintType::UNIQUE`.
	bool has_pk_fk_constraint = false;
	//! When `has_pk_fk_constraint` is true: which side holds the PK.
	//! `true`  — child is PK side, parent is FK side.
	//! `false` — parent is PK side, child is FK side.
	//! Undefined when `has_pk_fk_constraint` is false.
	bool child_is_pk = false;
};

//! Node in the Ordered Join Tree.
struct OJTNode {
	idx_t relation_id = 0;
	//! Raw pointer to the base-relation subtree (typically `LogicalGet`,
	//! possibly under a single-child `LogicalFilter` / `LogicalProjection`)
	//! inside the original plan held by `OrderedJoinTree::source_plan`.
	//! The OJT does not own it.
	LogicalOperator *base_op = nullptr;
	//! Single-relation predicates pushed down to this node by OuterYan
	//! passes (independent of any filters already inside `base_op`).
	vector<unique_ptr<Expression>> filters;
	//! Ordered children — order is meaningful for evaluation-order regimes.
	vector<OJTEdge> children;
	RelationStats stats;
};

//! `OuterYanFilterRecord` (defined in operator_tree.hpp) is the single
//! filter-record type shared by OperatorTree and OrderedJoinTree. It is
//! populated during `LogicalPlanToOT`, carried unchanged through OT-stage
//! transforms and `OTToOJT`, and finally consumed during `OJTToLogicalPlan`.

//! Ordered Join Tree — internal IR used inside OuterYanDP and OuterYanPost.
//! Built from a `LogicalOperator` tree by `LogicalPlanToOJT`. The OJT does
//! NOT take ownership of individual join / base-relation operators; instead
//! it holds raw pointers into them and owns the original plan as a whole
//! via `source_plan`. This keeps the original plan structure intact during
//! OJT manipulation passes (Simplification, DP enumeration, etc.). The
//! original plan is consumed during `OJTToLogicalPlan` to materialise the
//! rebuilt plan.
class OrderedJoinTree {
public:
	OrderedJoinTree();
	explicit OrderedJoinTree(unique_ptr<OJTNode> root);

	OJTNode &Root() {
		return *root;
	}
	const OJTNode &Root() const {
		return *root;
	}

	//! Visit every node in post-order. Callback receives node, its parent (or
	//! null for root), and the edge kind from parent to node.
	void PostOrder(std::function<void(OJTNode &, optional_ptr<OJTNode>, OuterYanJoinKind)> callback);

	//! Flat edge-list printer. Lists every relation once and every edge as
	//! `Rparent --KIND/order--> Rchild`. Intended for quick inspection /
	//! golden-file tests. Filter records are not printed here — they live
	//! on `OuterYanTree`, not on the OJT, so callers print them
	//! separately if needed.
	string Print() const;

	//! Indented-tree printer. Same information as `Print` but rendered
	//! parent-child tree style, EXPLAIN-style.
	string PrintAsTree() const;

private:
	unique_ptr<OJTNode> root;
};

} // namespace duckdb
