#include "duckdb/optimizer/outer_yan/simplification.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

// NOTE: Implementation strategy
// -----------------------------
// 1. Seed `null_rej` once, from every null-rejecting record in
//    `tree.filter_records`. The filter's physical position in the input plan
//    is intentionally ignored — a filter that constrains a relation's column
//    is taken to constrain that relation in the final result, so any join
//    above the filter that would null-pad those columns must be rewritten.
//    `DominatesJoin`-style structural checks belong to filter-pushdown
//    placement, not to this pass.
// 2. Walk the OT top-down. At each JOIN node:
//      (a) Look at which child subtree (left / right / both) contains a
//          relation in `null_rej`. Collapse the join kind accordingly:
//            LEFT  + rhs_nullrej              -> INNER
//            RIGHT + lhs_nullrej              -> INNER
//            OUTER + both                     -> INNER
//            OUTER + lhs_nullrej only         -> LEFT
//            OUTER + rhs_nullrej only         -> RIGHT
//      (b) The join condition itself null-rejects on the relation referenced
//          on each preserved side; insert those relation_ids into `null_rej`
//          before recursing so deeper joins observe the same constraint:
//            INNER -> insert both child relation_ids
//            LEFT  -> insert right child relation_id
//            RIGHT -> insert left  child relation_id
//            OUTER -> nothing
// 3. `null_rej` is passed by value into recursive calls so additions made
//    while walking one subtree do not contaminate the sibling subtree.

void Simplification::Apply(OuterYanTree &tree) {
	if (!tree.HasOT()) {
		return;
	}
	subtree_relations.clear();
	ComputeSubtreeRelations(tree.OT().Root());

	// Seed: every null-rejecting filter contributes its relations.
	unordered_set<idx_t> null_rej;
	for (auto &rec : tree.filter_records) {
		if (!rec || !rec->filter) {
			continue;
		}
		if (!FilterRejectsNulls(*rec->filter)) {
			continue;
		}
		for (auto rel : rec->referenced_relations) {
			null_rej.insert(rel);
		}
	}

	VisitNode(tree, tree.OT().Root(), std::move(null_rej));
}

const unordered_set<idx_t> &Simplification::ComputeSubtreeRelations(OTNode &node) {
	auto &out = subtree_relations[&node];
	if (node.kind == OTNode::Kind::RELATION) {
		out.insert(node.relation_id);
		return out;
	}
	const auto &left = ComputeSubtreeRelations(*node.children[0]);
	const auto &right = ComputeSubtreeRelations(*node.children[1]);
	out.insert(left.begin(), left.end());
	out.insert(right.begin(), right.end());
	return out;
}

bool Simplification::FilterRejectsNulls(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		const auto cmp_type = expr.GetExpressionType();
		if (cmp_type == ExpressionType::COMPARE_DISTINCT_FROM ||
		    cmp_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return false;
		}
		return true;
	}
	return false;
}

bool Simplification::SubtreeIntersectsNullRejection(OTNode &node,
                                                    const unordered_set<idx_t> &null_rej) {
	if (null_rej.empty()) {
		return false;
	}
	const auto &rels = subtree_relations[&node];
	for (auto rel : rels) {
		if (null_rej.find(rel) != null_rej.end()) {
			return true;
		}
	}
	return false;
}

void Simplification::VisitNode(OuterYanTree &tree, OTNode &node,
                               unordered_set<idx_t> null_rej) {
	if (node.kind == OTNode::Kind::RELATION) {
		return;
	}
	D_ASSERT(node.kind == OTNode::Kind::JOIN);
	auto &join = node.origin->Cast<LogicalComparisonJoin>();

	// (a) Collapse the join kind based on which child subtree contains a
	//     null-rejected relation.
	const bool lhs_nullrej =
	    SubtreeIntersectsNullRejection(*node.children[0], null_rej);
	const bool rhs_nullrej =
	    SubtreeIntersectsNullRejection(*node.children[1], null_rej);

	switch (join.join_type) {
	case JoinType::INNER:
		break;
	case JoinType::LEFT:
		if (rhs_nullrej) {
			join.join_type = JoinType::INNER;
			node.join_kind = JoinType::INNER;
		}
		break;
	case JoinType::RIGHT:
		if (lhs_nullrej) {
			join.join_type = JoinType::INNER;
			node.join_kind = JoinType::INNER;
		}
		break;
	case JoinType::OUTER:
		if (lhs_nullrej && rhs_nullrej) {
			join.join_type = JoinType::INNER;
			node.join_kind = JoinType::INNER;
		} else if (lhs_nullrej) {
			// LHS cannot be null-introducing → drop right-preservation.
			join.join_type = JoinType::LEFT;
			node.join_kind = JoinType::LEFT;
		} else if (rhs_nullrej) {
			// RHS cannot be null-introducing → drop left-preservation.
			join.join_type = JoinType::RIGHT;
			node.join_kind = JoinType::RIGHT;
		}
		break;
	default:
		throw InternalException("OuterYan Simplification: unexpected join type %s",
		                        EnumUtil::ToString(join.join_type));
	}

	// (b) Extend null_rej with the relations the (possibly rewritten) join
	//     condition itself null-rejects on the preserved side(s).
	switch (join.join_type) {
	case JoinType::INNER:
		null_rej.insert(node.left_child_relation_id);
		null_rej.insert(node.right_child_relation_id);
		break;
	case JoinType::LEFT:
		// R1 ⟕ R2 on R1.x = R2.y: matched R2 rows must have R2.y non-null.
		null_rej.insert(node.right_child_relation_id);
		break;
	case JoinType::RIGHT:
		// R1 ⟖ R2 on R1.x = R2.y: matched R1 rows must have R1.x non-null.
		null_rej.insert(node.left_child_relation_id);
		break;
	case JoinType::OUTER:
		break;
	default:
		throw InternalException("OuterYan Simplification: unexpected join type %s",
		                        EnumUtil::ToString(join.join_type));
	}

	VisitNode(tree, *node.children[0], null_rej);
	VisitNode(tree, *node.children[1], null_rej);
}

} // namespace duckdb
