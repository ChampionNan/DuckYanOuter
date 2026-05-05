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
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

//! Edge classification in the Ordered Join Tree (OJT). Direction marker
//! mapped 1:1 from the original `JoinType` of the underlying logical join.
//! This is just a label of the original join's direction; it does not flip
//! when the OJT orients the edge with reversed parent/child relative to
//! the original plan's left/right.
enum class OJTEdgeKind : uint8_t {
	INNER,
	LEFT_OUTER,
	RIGHT_OUTER,
	FULL_OUTER,
};

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
	OJTEdgeKind kind = OJTEdgeKind::INNER;
	//! Original edge classification, recorded before Desimplification rules were
	//! applied. Resimplification uses this to revert back to the original join
	//! type when the chosen ordering makes the rewrite redundant.
	OJTEdgeKind original_kind = OJTEdgeKind::INNER;
	//! Build order of the underlying join in the original logical plan.
	//! 1-based, starting from the bottommost join (`order = 1`) up to the
	//! root join (`order = n`). Derived from a post-order walk of the
	//! original plan during `LogicalPlanToOJT`. 0 means uninitialised.
	idx_t order = 0;
	//! Raw pointer to the logical join operator (typically a
	//! `LogicalComparisonJoin`) inside the original plan held by
	//! `OrderedJoinTree::source_plan`. The OJT does not own it.
	LogicalOperator *join_op = nullptr;
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
	//! Whether the underlying base-relation subtree carries any filters
	//! (a `LogicalFilter` above the `LogicalGet`, or a `LogicalGet` with
	//! pushed-down `table_filters`). Derived once at OJT construction.
	bool has_filters = false;
	//! Ordered children — order is meaningful for evaluation-order regimes.
	vector<OJTEdge> children;
	RelationStats stats;
};

//! Filter record collected during `LogicalPlanToOJT` and consumed during
//! `OJTToLogicalPlan`. Mimics DuckDB's `FilterInfo` (see
//! `duckdb/optimizer/join_order/query_graph_manager.hpp`) but indexes
//! relations by OJT `relation_id` rather than by `JoinRelationSet`, since
//! OuterYanDP runs DP directly on the OJT.
class OJTFilterRecord {
public:
	OJTFilterRecord(unique_ptr<Expression> filter_p, unordered_set<idx_t> referenced_relations_p,
	                idx_t filter_index_p, JoinType join_type_p = JoinType::INNER)
	    : filter(std::move(filter_p)), referenced_relations(std::move(referenced_relations_p)),
	      filter_index(filter_index_p), join_type(join_type_p) {
	}

	//! Filter expression, moved out of the original `LogicalFilter` during
	//! OJT construction. Consumed (moved out again) when applied during lowering.
	unique_ptr<Expression> filter;
	//! All OJT relation_ids referenced by `filter`.
	unordered_set<idx_t> referenced_relations;
	//! Position in `OrderedJoinTree::filter_records`.
	idx_t filter_index;
	//! Join type if this record came from a join condition (else INNER for
	//! residual single- or multi-relation predicates).
	JoinType join_type;
	//! When this record came from a binary equi-join condition: the two
	//! relations on each side and their bindings. Both unset for residual
	//! filters.
	optional<idx_t> left_relation;
	optional<idx_t> right_relation;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
	//! True if this record came from a residual `LogicalFilter` above the
	//! join tree (rather than from a join's own conditions).
	bool from_residual_predicate = false;
};

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

	//! The original logical plan this OJT was lifted from. Held to keep the
	//! raw pointers in `OJTEdge::join_op` and `OJTNode::base_op` valid.
	//! Consumed by `OJTToLogicalPlan` to produce the rebuilt plan.
	unique_ptr<LogicalOperator> source_plan;

	//! Multi-relation filter records collected during `LogicalPlanToOJT`
	//! and consumed during `OJTToLogicalPlan`. Single-relation predicates
	//! that bind to exactly one OJT relation may be pushed onto that
	//! `OJTNode::filters`; everything else stays here and is applied at
	//! the lowest covering subtree during lowering.
	vector<unique_ptr<OJTFilterRecord>> filter_records;

	//! Visit every node in post-order. Callback receives node, its parent (or
	//! null for root), and the edge kind from parent to node.
	void PostOrder(std::function<void(OJTNode &, optional_ptr<OJTNode>, OJTEdgeKind)> callback);

private:
	unique_ptr<OJTNode> root;
};

} // namespace duckdb
