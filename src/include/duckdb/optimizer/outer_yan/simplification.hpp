//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Walks a LogicalOperator tree and converts outer joins to inner joins where
//! a downstream null-rejecting predicate proves the null-padded rows would
//! be filtered anyway. More aggressive than OuterJoinSimplification because
//! it has whole-query visibility.
class Simplification {
public:
	void Apply(LogicalOperator &plan);
};

} // namespace duckdb
