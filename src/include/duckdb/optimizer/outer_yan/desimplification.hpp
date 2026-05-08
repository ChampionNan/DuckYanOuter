//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/desimplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

namespace duckdb {

//! Iteratively applies the six (TBD) associativity rules to drive the
//! OperatorTree into the *associative query state*: every remaining join
//! can be reordered freely on cost. Inner joins are absorbed/removed by
//! the rule set so only outer joins remain.
class Desimplification {
public:
	//! Apply rules to fixpoint on `tree.ot`.
	void Apply(OuterYanTree &tree);

private:
	//! TBD: precise definitions of the six associativity rules. Each is a
	//! local rewrite on a JOIN OTNode and its JOIN child.
	bool ApplyRule1(OTNode &node);
	bool ApplyRule2(OTNode &node);
	bool ApplyRule3(OTNode &node);
	bool ApplyRule4(OTNode &node);
	bool ApplyRule5(OTNode &node);
	bool ApplyRule6(OTNode &node);
};

} // namespace duckdb
