//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/operator_tree.hpp"

namespace duckdb {

//! Walks an Operator Tree and converts outer joins to inner joins where
//! a downstream null-rejecting predicate proves the null-padded rows would
//! be filtered anyway. More aggressive than OuterJoinSimplification because
//! it has whole-query visibility.
class Simplification {
public:
	void Apply(OperatorTree &ot);
};

} // namespace duckdb
