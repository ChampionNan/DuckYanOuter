//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/associative_query.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/operator_tree.hpp"

namespace duckdb {

//! Predicate: does this Operator Tree satisfy the associative-query state?
//!
//! Definition: every (parent, child) join-operator pair in the tree
//! satisfies the associativity property, so swapping any such pair preserves
//! query semantics. When this holds the entire tree can be freely reordered
//! on cost alone — the precondition that OuterYanDP relies on.
//!
//! Used after Desimplification and before OuterYanDP.
class AssociativeQuery {
public:
	static bool Holds(const OperatorTree &ot);

	//! Local check on a single (parent, child) operator pair.
	static bool PairSatisfiesAssociativity(const OTNode &parent, const OTNode &child);
};

} // namespace duckdb
