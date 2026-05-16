#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"


namespace duckdb {

// ============================================================================
// OperatorTree — base-relation / expression introspection helpers
// ============================================================================

optional<idx_t> OperatorTree::BaseRelationTableIndex(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return op.Cast<LogicalGet>().table_index.index;
	}
	if ((op.type == LogicalOperatorType::LOGICAL_FILTER ||
	     op.type == LogicalOperatorType::LOGICAL_PROJECTION) &&
	    op.children.size() == 1) {
		return BaseRelationTableIndex(*op.children[0]);
	}
	return optional<idx_t>();
}

optional<idx_t> OperatorTree::SingleColumnRefTableIndex(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return expr.Cast<BoundColumnRefExpression>().binding.table_index.index;
	}
	optional<idx_t> result;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!result) {
			result = SingleColumnRefTableIndex(child);
		}
	});
	return result;
}

// ============================================================================
// OperatorTree — IsValid sub-checks
// ============================================================================

idx_t OperatorTree::LookupRelationByTableIndex(const OTNode &subtree_root, idx_t table_index) {
	if (subtree_root.kind == OTNode::Kind::RELATION) {
		auto ti = BaseRelationTableIndex(*subtree_root.origin);
		if (ti && *ti == table_index) {
			return subtree_root.relation_id;
		}
		return DConstants::INVALID_INDEX;
	}
	for (idx_t side = 0; side < 2; side++) {
		if (subtree_root.children[side]) {
			idx_t r = LookupRelationByTableIndex(*subtree_root.children[side], table_index);
			if (r != DConstants::INVALID_INDEX) {
				return r;
			}
		}
	}
	return DConstants::INVALID_INDEX;
}

bool OperatorTree::CheckShape(const OTNode &node, string *reason) {
	if (node.kind == OTNode::Kind::JOIN) {
		if (!node.info) {
			return Fail(reason, "JOIN OTNode has null info");
		}
		if (node.info->conditions.empty()) {
			return Fail(reason, "JOIN OTNode has no conditions");
		}
		if (!node.children[0] || !node.children[1]) {
			return Fail(reason, "JOIN OTNode missing one or both children");
		}
		return CheckShape(*node.children[0], reason) && CheckShape(*node.children[1], reason);
	}
	if (!node.origin) {
		return Fail(reason, "RELATION OTNode has null origin");
	}
	if (node.children[0] || node.children[1]) {
		return Fail(reason, "RELATION OTNode has non-null children");
	}
	if (!IsBaseRelationSubtree(*node.origin)) {
		return Fail(reason, "RELATION OTNode origin is not a base-relation subtree");
	}
	return true;
}

void OperatorTree::CollectRelationIds(const OTNode &node, vector<idx_t> &out) {
	if (node.kind == OTNode::Kind::RELATION) {
		out.push_back(node.relation_id);
		return;
	}
	if (node.children[0]) {
		CollectRelationIds(*node.children[0], out);
	}
	if (node.children[1]) {
		CollectRelationIds(*node.children[1], out);
	}
}

bool OperatorTree::CheckRelationIdsUnique(const OTNode &root, string *reason) {
	vector<idx_t> ids;
	CollectRelationIds(root, ids);
	unordered_set<idx_t> seen;
	for (auto rid : ids) {
		if (!seen.insert(rid).second) {
			return Fail(reason, StringUtil::Format("duplicate relation_id %llu in OT", rid));
		}
	}
	return true;
}

//! Recompute the subtree-relation union from scratch and compare against the
//! cached set on every node. Returns the recomputed union via `out`.
bool OperatorTree::CheckSubtreeRelations(const OTNode &node, unordered_set<idx_t> &out, string *reason) {
	unordered_set<idx_t> expected;
	if (node.kind == OTNode::Kind::RELATION) {
		expected.insert(node.relation_id);
	} else {
		unordered_set<idx_t> left_set;
		unordered_set<idx_t> right_set;
		if (!CheckSubtreeRelations(*node.children[0], left_set, reason)) {
			return false;
		}
		if (!CheckSubtreeRelations(*node.children[1], right_set, reason)) {
			return false;
		}
		expected.insert(left_set.begin(), left_set.end());
		expected.insert(right_set.begin(), right_set.end());
	}
	if (expected.size() != node.subtree_relations.size()) {
		return Fail(reason, "OTNode::subtree_relations size mismatch with recomputed union");
	}
	for (auto rid : expected) {
		if (!node.subtree_relations.count(rid)) {
			return Fail(reason, StringUtil::Format(
			                        "OTNode::subtree_relations missing relation_id %llu", rid));
		}
	}
	out = std::move(expected);
	return true;
}

//! Per-JOIN canonical-form assertion: condition LHS must resolve to
//! `info->cond_left_relation_id` inside `children[0]` (and that relation lies
//! in `children[0]->subtree_relations`); same for RHS / right.
bool OperatorTree::CheckJoinConditionAlignment(const OTNode &node, string *reason) {
	if (node.kind != OTNode::Kind::JOIN) {
		return true;
	}
	if (!node.info) {
		return Fail(reason, "JOIN OTNode has null info");
	}
	const auto &info = *node.info;
	if (!node.children[0]->subtree_relations.count(info.cond_left_relation_id)) {
		return Fail(reason, StringUtil::Format(
		                        "JOIN cond_left_relation_id %llu not in children[0] subtree",
		                        info.cond_left_relation_id));
	}
	if (!node.children[1]->subtree_relations.count(info.cond_right_relation_id)) {
		return Fail(reason, StringUtil::Format(
		                        "JOIN cond_right_relation_id %llu not in children[1] subtree",
		                        info.cond_right_relation_id));
	}
	if (info.conditions.empty()) {
		return Fail(reason, "JOIN node has no conditions");
	}
	const auto &cond = info.conditions[0];
	auto lhs_ti = SingleColumnRefTableIndex(cond.GetLHS());
	auto rhs_ti = SingleColumnRefTableIndex(cond.GetRHS());
	if (!lhs_ti || !rhs_ti) {
		return Fail(reason, "JOIN conditions[0] LHS/RHS not resolvable to a single column ref");
	}
	idx_t lhs_rel = LookupRelationByTableIndex(*node.children[0], *lhs_ti);
	if (lhs_rel == DConstants::INVALID_INDEX) {
		return Fail(reason, StringUtil::Format(
		                        "JOIN conditions[0] LHS table_index %llu absent from children[0]",
		                        *lhs_ti));
	}
	if (lhs_rel != info.cond_left_relation_id) {
		return Fail(reason, StringUtil::Format(
		                        "JOIN conditions[0] LHS resolves to relation %llu but "
		                        "cond_left_relation_id is %llu",
		                        lhs_rel, info.cond_left_relation_id));
	}
	idx_t rhs_rel = LookupRelationByTableIndex(*node.children[1], *rhs_ti);
	if (rhs_rel == DConstants::INVALID_INDEX) {
		return Fail(reason, StringUtil::Format(
		                        "JOIN conditions[0] RHS table_index %llu absent from children[1]",
		                        *rhs_ti));
	}
	if (rhs_rel != info.cond_right_relation_id) {
		return Fail(reason, StringUtil::Format(
		                        "JOIN conditions[0] RHS resolves to relation %llu but "
		                        "cond_right_relation_id is %llu",
		                        rhs_rel, info.cond_right_relation_id));
	}
	return CheckJoinConditionAlignment(*node.children[0], reason) &&
	       CheckJoinConditionAlignment(*node.children[1], reason);
}

//! Common-relation rule between every (parent_join, child_join) pair.
bool OperatorTree::CheckCommonRelations(const OTNode &node, string *reason) {
	if (node.kind != OTNode::Kind::JOIN) {
		return true;
	}
	D_ASSERT(node.info);
	const auto &info = *node.info;
	for (idx_t side = 0; side < 2; side++) {
		const auto &child = *node.children[side];
		if (child.kind != OTNode::Kind::JOIN) {
			continue;
		}
		const bool overlap =
		    child.subtree_relations.count(info.cond_left_relation_id) > 0 ||
		    child.subtree_relations.count(info.cond_right_relation_id) > 0;
		if (!overlap) {
			return Fail(reason,
			            "JOIN/JOIN pair shares no relation with parent's condition "
			            "(implicit cross product)");
		}
	}
	return CheckCommonRelations(*node.children[0], reason) &&
	       CheckCommonRelations(*node.children[1], reason);
}

// ============================================================================
// OperatorTree — Finalize sub-steps
// ============================================================================

void OperatorTree::PopulateSubtreeRelations(OTNode &node) {
	node.subtree_relations.clear();
	if (node.kind == OTNode::Kind::RELATION) {
		node.subtree_relations.insert(node.relation_id);
		return;
	}
	PopulateSubtreeRelations(*node.children[0]);
	PopulateSubtreeRelations(*node.children[1]);
	node.subtree_relations.insert(node.children[0]->subtree_relations.begin(),
	                              node.children[0]->subtree_relations.end());
	node.subtree_relations.insert(node.children[1]->subtree_relations.begin(),
	                              node.children[1]->subtree_relations.end());
}

void OperatorTree::CanonicaliseConditions(OTNode &node) {
	if (node.kind == OTNode::Kind::RELATION) {
		return;
	}
	D_ASSERT(node.info);
	auto &info = *node.info;
	auto &left_set = node.children[0]->subtree_relations;
	auto &right_set = node.children[1]->subtree_relations;
	const bool lhs_in_left = left_set.count(info.cond_left_relation_id) > 0;
	const bool rhs_in_right = right_set.count(info.cond_right_relation_id) > 0;
	if (!lhs_in_left || !rhs_in_right) {
		const bool lhs_in_right = right_set.count(info.cond_left_relation_id) > 0;
		const bool rhs_in_left = left_set.count(info.cond_right_relation_id) > 0;
		if (!lhs_in_right || !rhs_in_left) {
			throw InternalException(
			    "OperatorTree::Finalize: JOIN condition relations do not match "
			    "either children orientation (LHS rel=%llu, RHS rel=%llu)",
			    info.cond_left_relation_id, info.cond_right_relation_id);
		}
		if (info.conditions.empty()) {
			throw InternalException("OperatorTree::Finalize: JOIN node has no conditions");
		}
		// Swap every condition (keeps cond_left_relation_id meaningful for
		// the entire vector) and swap the side-id labels in lock-step.
		for (auto &cond : info.conditions) {
			cond.Swap();
		}
		std::swap(info.cond_left_relation_id, info.cond_right_relation_id);
	}
	CanonicaliseConditions(*node.children[0]);
	CanonicaliseConditions(*node.children[1]);
}

// ============================================================================
// OperatorTree — public entry points
// ============================================================================

void OperatorTree::Finalize() {
	if (!root) {
		throw InternalException("OperatorTree::Finalize: null root");
	}
	PopulateSubtreeRelations(*root);
	CanonicaliseConditions(*root);
}

bool OperatorTree::IsValid(string *reason) const {
	if (!root) {
		return Fail(reason, "OperatorTree has null root");
	}
	if (!CheckShape(*root, reason)) {
		return false;
	}
	if (!CheckRelationIdsUnique(*root, reason)) {
		return false;
	}
	unordered_set<idx_t> recomputed;
	if (!CheckSubtreeRelations(*root, recomputed, reason)) {
		return false;
	}
	if (!CheckJoinConditionAlignment(*root, reason)) {
		return false;
	}
	if (!CheckCommonRelations(*root, reason)) {
		return false;
	}
	return true;
}

} // namespace duckdb
