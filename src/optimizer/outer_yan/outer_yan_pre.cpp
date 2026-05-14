#include "duckdb/optimizer/outer_yan/outer_yan_pre.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

void OuterYanPre::Optimize(unique_ptr<LogicalOperator> plan, OuterYanTree &tree) {
	// Applicability is the caller's responsibility (see `optimizer.cpp`: the
	// dispatch site calls `OuterYanTree::CheckApplicability` and only enters
	// `Optimize` when the gate passes).
	//
	// Pipeline (steps mirror the header comment):
	//   2. LogicalPlanToOT      -- lifts the plan into `tree.ot`, captures
	//                              residual filters, and classifies the root
	//                              aggregate into `tree.root_aggregation`.
	//   3. Simplify             -- outer -> inner where null-rejection permits.
	//   4. RecordSemiJoinPairs  -- transient OJT built off the post-
	//                              simplification OT; `BottomUpPass` /
	//                              `TopDownPass` walk it to populate
	//                              `tree.bottom_up_pairs` and
	//                              `tree.top_down_pairs`. The local OJT goes
	//                              out of scope at the end of this block, so
	//                              subsequent Desimplification mutations
	//                              never reach the recorded pairs.
	//   5. Desimplify           -- six Galindo-Legaria / Rosenthal associativity
	//                              rules to fixpoint; mutates only
	//                              `OTNode::join_kind`.
	//   6. AllPairsSatisfy      -- post-condition: every (P, D) pair in an OK
	//                              cell.
	tree.BuildOT(std::move(plan));
	Simplify(tree);
	{
		auto post_simp_ojt = tree.ConstructOJT();
		BottomUpPass(tree, post_simp_ojt->Root());
		TopDownPass(tree, post_simp_ojt->Root());
	}
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

// ============================================================================
// SemiPair recording
// ============================================================================

void OuterYanPre::BottomUpPass(OuterYanTree &tree, OJTNode &node) {
	// Post-order: recurse into children first so that pairs are appended in
	// bottom-up order (deepest edge fires first).
	for (auto &edge : node.children) {
		BottomUpPass(tree, *edge.child);
		// Phase 1 fires on R_p RIGHT R_i (child preserved) or R_p INNER R_i.
		if (edge.kind == JoinType::RIGHT || edge.kind == JoinType::INNER) {
			OuterYanSemiPair pair;
			pair.build = edge.child->relation_id; // R_i provides keys
			pair.probe = node.relation_id;        // R_p is filtered
			ExtractSemiKeys(tree, pair.build, pair.probe, edge, pair);
			tree.bottom_up_pairs.push_back(std::move(pair));
		}
	}
}

void OuterYanPre::TopDownPass(OuterYanTree &tree, OJTNode &node) {
	// Pre-order (reverse post-order): emit the parent->child edge before
	// recursing.
	for (auto &edge : node.children) {
		// Phase 2 fires on R_p LEFT R_i (parent preserved) or R_p INNER R_i.
		if (edge.kind == JoinType::LEFT || edge.kind == JoinType::INNER) {
			OuterYanSemiPair pair;
			pair.build = node.relation_id;        // R_p provides keys
			pair.probe = edge.child->relation_id; // R_i is filtered
			ExtractSemiKeys(tree, pair.build, pair.probe, edge, pair);
			tree.top_down_pairs.push_back(std::move(pair));
		}
		TopDownPass(tree, *edge.child);
	}
}

void OuterYanPre::ExtractSemiKeys(OuterYanTree &tree, RelationId build_rel,
                                  RelationId probe_rel, const OJTEdge &edge,
                                  OuterYanSemiPair &out) {
	if (!edge.info) {
		throw InternalException(
		    "OuterYanPre::ExtractSemiKeys: edge has no OTJoin payload");
	}
	const auto &conditions = edge.info->conditions;
	out.keys.reserve(conditions.size());
	for (const auto &cond : conditions) {
		// The applicability gate forces each side to be a plain
		// BoundColumnRefExpression referencing exactly one base relation.
		auto &lhs = cond.GetLHS().Cast<BoundColumnRefExpression>();
		auto &rhs = cond.GetRHS().Cast<BoundColumnRefExpression>();
		auto lhs_it = tree.table_to_relation.find(lhs.binding.table_index.index);
		auto rhs_it = tree.table_to_relation.find(rhs.binding.table_index.index);
		if (lhs_it == tree.table_to_relation.end() ||
		    rhs_it == tree.table_to_relation.end()) {
			throw InternalException(
			    "OuterYanPre::ExtractSemiKeys: join condition references an unknown "
			    "table_index");
		}
		OuterYanSemiKey key;
		if (lhs_it->second == build_rel && rhs_it->second == probe_rel) {
			key.build_binding = lhs.binding;
			key.probe_binding = rhs.binding;
		} else if (lhs_it->second == probe_rel && rhs_it->second == build_rel) {
			key.build_binding = rhs.binding;
			key.probe_binding = lhs.binding;
		} else {
			throw InternalException(
			    "OuterYanPre::ExtractSemiKeys: condition does not connect build=%llu and "
			    "probe=%llu (saw lhs_rel=%llu, rhs_rel=%llu)",
			    build_rel, probe_rel, lhs_it->second, rhs_it->second);
		}
		out.keys.push_back(key);
	}
}

} // namespace duckdb
