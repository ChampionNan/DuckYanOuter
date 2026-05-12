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
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class LogicalAggregate;

//! OT/OJT base-relation identifier; centralised here so both the
//! applicability gate and OperatorTree/OrderedJoinTree share the same alias.
using RelationId = idx_t;

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

bool Fail(string *reason, const string &msg) {
	if (reason) {
		*reason = msg;
	}
	return false;
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

//! Classification of a root LogicalAggregate sitting above the OuterYan join
//! skeleton. Mirrors the categories used by `DuckDBYanPlus::DetectQueryType`:
//! purely a function-name dispatch over the aggregate's bound function
//! (`count`, `count_star`, `min`, `max`, `sum`); no `IsDistributive` /
//! `IsHolistic` helper is involved.
enum class OuterYanAggregationType : uint8_t {
	NONE,        //!< query has no root aggregation
	COUNT_STAR,  //!< `count_star()` with empty GROUP BY
	MINMAX,      //!< only `min` / `max` aggregates
	SUM,         //!< `sum`, or `count` / `count_star` with non-empty GROUP BY
	OTHER        //!< unsupported / mixed aggregate shape
};

//! Per-aggregate column descriptor recorded during OT build. For
//! `count_star`, `binding` carries `INVALID_INDEX` table/column indices and
//! `relation` stays empty.
struct OuterYanAggregateColumn {
	string function_name;
	ColumnBinding binding;
	optional<RelationId> relation;
};

//! Root-aggregation marker held on `OuterYanTree`. Populated during
//! `OuterYanTree::BuildOT` while peeling the projection/aggregate root chain
//! above the join skeleton.
struct OuterYanRootAggregation {
	OuterYanAggregationType type = OuterYanAggregationType::NONE;
	//! Raw pointer into `OuterYanTree::source_plan`; same lifetime contract
	//! as `OTNode::origin`.
	LogicalAggregate *agg_op = nullptr;
	bool has_group_by = false;
	vector<OuterYanAggregateColumn> columns;
};

//! One equi-join key inside a semi-join pair. `build_binding` is the column
//! that feeds the hash table; `probe_binding` is the column that is
//! filtered. The bindings reference the original LogicalGet `table_index`
//! values inside `OuterYanTree::source_plan` and stay valid for the
//! wrapper's lifetime (until `OJTToLogicalPlan` rebuilds the plan).
struct OuterYanSemiKey {
	ColumnBinding build_binding;
	ColumnBinding probe_binding;
};

//! A semi-join reduction decision recorded by `OuterYanPre` from the
//! post-simplification, pre-desimplification OJT and later materialised by
//! `SemijoinInsertion` into a `LogicalSJBuild` on `build` plus a matching
//! `LogicalSJProbe` on `probe`. Multi-condition equi-joins yield multiple
//! entries in `keys`.
struct OuterYanSemiPair {
	RelationId build = 0;
	RelationId probe = 0;
	vector<OuterYanSemiKey> keys;
};

} // namespace duckdb
