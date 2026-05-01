//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/operator_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Kind of node in the OuterYan Operator Tree (OT) — a dedicated IR
//! abstracting the logical plan to focus on join-algorithm reasoning.
enum class OTNodeKind : uint8_t {
	BASE_RELATION,
	JOIN,
	SJ_BUILD,
	SJ_PROBE,
};

class OperatorTree;

//! Node in the Operator Tree.
//!
//! A wrapper around a `LogicalOperator` — the OT is a structural lens over
//! the logical plan, not a duplicate IR. `kind` and `join_type` are derived
//! from `op` for fast dispatch; the underlying operator carries the full
//! detail (predicates, expressions, child operators, paired SJ filters).
struct OTNode {
	//! Cached classification of `op` — read once and used for dispatch in the
	//! algorithm passes. Must stay in sync with `op->type`.
	OTNodeKind kind = OTNodeKind::BASE_RELATION;
	//! For JOIN nodes — cached from the underlying logical join operator.
	JoinType join_type = JoinType::INNER;
	//! For BASE_RELATION — opaque identifier (table index from the binder).
	idx_t relation_id = 0;
	//! Underlying logical operator. Owned here while the OT exists; released
	//! back to the surrounding LogicalOperator tree on lowering.
	unique_ptr<LogicalOperator> op;
	//! OT-level structural children (left/right for JOIN; single child for
	//! SJ_BUILD/SJ_PROBE; empty for BASE_RELATION).
	vector<unique_ptr<OTNode>> children;
};

//! OuterYan Operator Tree — a dedicated IR distinct from LogicalOperator.
//! Used inside OuterYan passes so the algorithm reasoning stays uncoupled
//! from DuckDB's planner internals.
class OperatorTree {
public:
	OperatorTree();
	explicit OperatorTree(unique_ptr<OTNode> root);

	OTNode &Root() {
		return *root;
	}
	const OTNode &Root() const {
		return *root;
	}

	//! Boundary I/O — only happens at OuterYan pass boundaries.
	static unique_ptr<OperatorTree> FromLogical(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> ToLogical() &&;

	//! Canonical state (TBD precise definition).
	bool IsCanonical() const;
	void Canonicalize();

	//! Manipulation primitives, used by sim / de-sim / re-sim.
	//! Pure tree rewrites — unaware of rule semantics.
	void RotateLeft(OTNode &pivot);
	void RotateRight(OTNode &pivot);

private:
	unique_ptr<OTNode> root;
};

} // namespace duckdb
