//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/associative_query.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Predicate: does this LogicalOperator tree satisfy the associative-query state?
//!
//! Definition: every (parent, child) join-operator pair in the tree
//! satisfies the associativity property, so swapping any such pair preserves
//! query semantics. When this holds the entire tree can be freely reordered
//! on cost alone — the precondition that OuterYanDP relies on.
//!
//! Used after Desimplification and before OuterYanDP.
class AssociativeQuery {
public:
	static bool Holds(const LogicalOperator &plan);

	//! Local check on a single (parent, child) join-operator pair.
	static bool PairSatisfiesAssociativity(const LogicalOperator &parent, const LogicalOperator &child);
};

} // namespace duckdb
