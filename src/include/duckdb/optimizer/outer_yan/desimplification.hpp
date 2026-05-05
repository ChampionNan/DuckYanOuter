//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/desimplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Iteratively applies the six (TBD) associativity rules to drive the
//! LogicalOperator tree into the *associative query state*: every remaining
//! join can be reordered freely on cost. Inner joins are absorbed/removed
//! by the rule set so only outer joins remain.
class Desimplification {
public:
	//! Apply rules to fixpoint.
	void Apply(LogicalOperator &plan);

private:
	//! TBD: precise definitions of the six associativity rules. Each is a
	//! local rewrite on a join operator and its child join operator.
	bool ApplyRule1(LogicalOperator &node);
	bool ApplyRule2(LogicalOperator &node);
	bool ApplyRule3(LogicalOperator &node);
	bool ApplyRule4(LogicalOperator &node);
	bool ApplyRule5(LogicalOperator &node);
	bool ApplyRule6(LogicalOperator &node);
};

} // namespace duckdb
