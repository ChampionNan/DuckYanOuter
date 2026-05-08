#include "duckdb/optimizer/outer_yan/simplification.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

void Simplification::Apply(OuterYanTree &tree) {
	if (!tree.HasOT()) {
		return;
	}
	null_filtered_columns.clear();
	subtree_relations.clear();
	ComputeSubtreeRelations(tree.OT().Root());
	VisitNode(tree, tree.OT().Root());
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

void Simplification::HandleExpression(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	null_filtered_columns.insert(colref.binding);
}

void Simplification::HandleFilterRecord(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		const auto &is_not_null = expr.Cast<BoundOperatorExpression>();
		HandleExpression(*is_not_null.children[0]);
	} else if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		if (expr.GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM ||
		    expr.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return;
		}
		const auto &cmp = expr.Cast<BoundComparisonExpression>();
		HandleExpression(*cmp.left);
		HandleExpression(*cmp.right);
	}
}

bool Simplification::DominatesJoin(const OuterYanFilterRecord &record, OTNode &join_node) {
	const auto &left = subtree_relations[join_node.children[0].get()];
	const auto &right = subtree_relations[join_node.children[1].get()];
	bool inside_left = true;
	bool inside_right = true;
	for (auto rel : record.referenced_relations) {
		if (left.find(rel) == left.end()) {
			inside_left = false;
		}
		if (right.find(rel) == right.end()) {
			inside_right = false;
		}
		if (!inside_left && !inside_right) {
			return true;
		}
	}
	return false;
}

void Simplification::HarvestDominatingFilters(OuterYanTree &tree, OTNode &join_node) {
	for (auto &rec : tree.filter_records) {
		if (!rec || !rec->filter) {
			continue;
		}
		if (!DominatesJoin(*rec, join_node)) {
			continue;
		}
		HandleFilterRecord(*rec->filter);
	}
}

void Simplification::VisitNode(OuterYanTree &tree, OTNode &node) {
	if (node.kind == OTNode::Kind::RELATION) {
		return;
	}
	D_ASSERT(node.kind == OTNode::Kind::JOIN);
	auto &join = node.origin->Cast<LogicalComparisonJoin>();

	HarvestDominatingFilters(tree, node);

	switch (join.join_type) {
	case JoinType::INNER: {
		// INNER conditions reject NULLs on both sides.
		for (const auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			if (condition.GetComparisonType() == ExpressionType::COMPARE_DISTINCT_FROM ||
			    condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				continue;
			}
			HandleExpression(condition.GetLHS());
			HandleExpression(condition.GetRHS());
		}
		VisitNode(tree, *node.children[0]);
		VisitNode(tree, *node.children[1]);
		return;
	}
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER: {
		bool preserves_null_extended_rows[2] = {
		    join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER,
		    join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER};
		for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
			for (const auto &binding : node.children[child_idx]->origin->GetColumnBindings()) {
				if (null_filtered_columns.find(binding) != null_filtered_columns.end()) {
					// Rejecting NULLs in one child removes preservation of NULL extended rows for the other child
					preserves_null_extended_rows[1 - child_idx] = false;
					break;
				}
			}
		}

		if (!preserves_null_extended_rows[0] && !preserves_null_extended_rows[1]) {
			join.join_type = JoinType::INNER;
			node.join_kind = JoinType::INNER;
			VisitNode(tree, node); // Re-enter — the new INNER's conditions are NULL-filtering
			return;
		}
		if (preserves_null_extended_rows[0] && !preserves_null_extended_rows[1]) {
			D_ASSERT(join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER);
			join.join_type = JoinType::LEFT;
			node.join_kind = JoinType::LEFT;
		} else if (!preserves_null_extended_rows[0] && preserves_null_extended_rows[1]) {
			D_ASSERT(join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER);
			join.join_type = JoinType::RIGHT;
			node.join_kind = JoinType::RIGHT;
		} else {
			D_ASSERT(join.join_type == JoinType::OUTER);
		}
		VisitNode(tree, *node.children[0]);
		VisitNode(tree, *node.children[1]);
		return;
	}
	default:
		throw InternalException("OuterYan Simplification: unexpected join type %s",
		                        EnumUtil::ToString(join.join_type));
	}
}

} // namespace duckdb
