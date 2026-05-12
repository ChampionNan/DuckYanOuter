#include "duckdb/optimizer/outer_yan/outer_yan_pre.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void OuterYanPre::Optimize(unique_ptr<LogicalOperator> plan, OuterYanTree &tree) {
	// Applicability is the caller's responsibility (see `optimizer.cpp`: the
	// dispatch site calls `OuterYanTree::CheckApplicability` and only enters
	// `Optimize` when the gate passes).
	//
	// Pipeline (steps mirror the header comment):
	//   2. LogicalPlanToOT  -- lifts the plan into `tree.ot`, captures residual
	//                          filters, and classifies the root aggregate (if
	//                          present) into `tree.root_aggregation`.
	//   3. Simplify         -- outer -> inner where null-rejection permits.
	//   4. Desimplify       -- iterate the six Galindo-Legaria / Rosenthal
	//                          associativity rules to fixpoint.
	//   5. AllPairsSatisfy  -- post-condition assertion: every (P, D) pair now
	//                          sits in an OK cell. If not, the desimplification
	//                          fixpoint failed to converge (a bug, not a
	//                          query-level rejection).
	tree.BuildOT(std::move(plan));
	Simplify(tree);
	Desimplify(tree);
	if (!desimplification.AllPairsSatisfy(tree)) {
		throw InternalException(
		    "OuterYanPre::Optimize: desimplification did not reach a fully "
		    "associative state (AllPairsSatisfy returned false after fixpoint)");
	}
}

ApplicabilityResult OuterYanPre::ApplicabilityCheck(LogicalOperator &plan) {
	return OuterYanApplicability::Check(plan);
}

void OuterYanPre::Simplify(OuterYanTree &tree) {
	simplification.Apply(tree);
}

void OuterYanPre::Desimplify(OuterYanTree &tree) {
	desimplification.Apply(tree);
}

} // namespace duckdb
