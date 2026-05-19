#include "duckdb/optimizer/outer_yan/agg_pushdown.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

#include <algorithm>

namespace duckdb {

void AggPushdown::Apply(OuterYanTree &tree) {
	tree.agg_pushdown_decisions.clear();

	if (tree.root_aggregation.type == OuterYanAggregationType::NONE) {
		// No root aggregation -> nothing to push -> leave the vector empty so
		// the visitor short-circuits.
		return;
	}
	if (!tree.HasOJT()) {
		throw InternalException("AggPushdown::Apply: tree has no OJT");
	}

	vector<OuterYanAggPushdownDecision> tmp;
	Walk(tree.OJT().Root(), tmp);

	// Sort by edge.order ascending so the vector lines up 1:1 with the
	// post-order join traversal that `LogicalOperatorVisitor` will perform
	// on the plan rebuilt by `OJTToLogicalPlan` (see
	// `outer_yan_tree.cpp:730-731`). The Walk above is already bottom-up,
	// but for left-skewed vs right-skewed shapes the DFS order may differ
	// from `OJTEdge::order`, so we sort explicitly.
	std::sort(tmp.begin(), tmp.end(),
	          [](const OuterYanAggPushdownDecision &a, const OuterYanAggPushdownDecision &b) {
		          return a.order < b.order;
	          });

	tree.agg_pushdown_decisions = std::move(tmp);
}

void AggPushdown::Walk(OJTNode &node, vector<OuterYanAggPushdownDecision> &out) {
	// Bottom-up: recurse into child subtree first, then emit the decision
	// for the edge into that child. Deepest edges are recorded before their
	// ancestors so column-level analyses in later slices can propagate state
	// upward along the same traversal.
	for (auto &edge : node.children) {
		if (!edge.child) {
			throw InternalException("AggPushdown: OJT edge has no child");
		}
		Walk(*edge.child, out);

		OuterYanAggPushdownDecision decision;
		decision.parent_relation_id = edge.parent_relation_id;
		decision.child_relation_id = edge.child->relation_id;
		decision.order = edge.order;
		// Slice 1: relation-level gate only. PK-FK edges carry no pushdown
		// benefit; every other edge is a pushdown candidate. The
		// `GROUP_BY_NUM` threshold is decided later by the visitor after
		// column pruning has run.
		decision.push_into_child = !edge.has_pk_fk_constraint;
		out.push_back(decision);
	}
}

} // namespace duckdb
