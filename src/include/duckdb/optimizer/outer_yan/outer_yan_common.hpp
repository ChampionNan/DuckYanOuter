//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! Edge / join classification, shared by OperatorTree and OrderedJoinTree.
//! Aliased to DuckDB's `JoinType` since the two are semantically identical
//! within OuterYan's accepted scope (INNER / LEFT / RIGHT / OUTER); the
//! applicability gate rejects all other JoinType values, so OT and OJT
//! never observe SEMI / ANTI / MARK / SINGLE / etc.
//!
//! In the OJT the convention is "parent KIND child": when the OJT places
//! the original RHS as parent, `LEFT` and `RIGHT` are flipped via
//! `FlipOuterYanJoinKind` so the kind continues to label the OJT-parent's
//! role. `INNER` and `OUTER` are symmetric and unaffected. See
//! `CheckOuterYanJoinFlip` below for the orientation rule.
using OuterYanJoinKind = JoinType;

//! Flip an OuterYanJoinKind across parent/child. INNER and FULL OUTER are
//! symmetric; LEFT and RIGHT swap.
inline OuterYanJoinKind FlipOuterYanJoinKind(OuterYanJoinKind kind) {
	switch (kind) {
	case JoinType::INNER:
		return JoinType::INNER;
	case JoinType::LEFT:
		return JoinType::RIGHT;
	case JoinType::RIGHT:
		return JoinType::LEFT;
	case JoinType::OUTER:
		return JoinType::OUTER;
	default:
		throw InternalException("FlipOuterYanJoinKind: unsupported JoinType");
	}
}

//! Map a join's current kind to one oriented from OJT-parent to OJT-child.
//! `parent_is_left` must be true iff the OJT-parent corresponds to the
//! original join's `children[0]` (LHS); if false, the kind is flipped so
//! that LEFT / RIGHT continue to label the OJT-parent's role rather than
//! the original LHS's role. Single source of truth for the OJT's
//! edge-direction rule: edge kind always reads as "parent KIND child".
inline OuterYanJoinKind CheckOuterYanJoinFlip(OuterYanJoinKind kind,
                                                          bool parent_is_left) {
	return parent_is_left ? kind : FlipOuterYanJoinKind(kind);
}

//! Filter record shared by OperatorTree and OrderedJoinTree. Populated in
//! `LogicalPlanToOT`, carried through OT-stage transforms unchanged, then
//! handed to OJT via `OTToOJT`, and finally consumed during
//! `OJTToLogicalPlan` where each filter is emitted into a fresh
//! `LogicalFilter` at the lowest covering subtree.
class OuterYanFilterRecord {
public:
	OuterYanFilterRecord(unique_ptr<Expression> filter_p,
	                     unordered_set<idx_t> referenced_relations_p,
	                     idx_t filter_index_p,
	                     JoinType join_type_p = JoinType::INNER)
	    : filter(std::move(filter_p)),
	      referenced_relations(std::move(referenced_relations_p)),
	      filter_index(filter_index_p),
	      join_type(join_type_p) {
	}

	unique_ptr<Expression> filter;
	unordered_set<idx_t> referenced_relations;
	idx_t filter_index;
	JoinType join_type;
	optional<idx_t> left_relation;
	optional<idx_t> right_relation;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
	bool from_residual_predicate = false;
};

} // namespace duckdb
