#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_get.hpp"


namespace duckdb {

// ============================================================================
// File-local helpers
// ============================================================================

namespace {

//! A "leaf" base-relation subtree from the OT's perspective: a LogicalGet,
//! possibly under a chain of single-child LogicalFilter / LogicalProjection.
//! Duplicated in tree_conversions.cpp; kept here so OperatorTreeIsValid is
//! independent of the conversion translation unit.
bool IsBaseRelationSubtree(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return true;
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return op.children.size() == 1 && IsBaseRelationSubtree(*op.children[0]);
	default:
		return false;
	}
}

bool CheckShape(const OTNode &node, string *reason) {
	if (!node.origin) {
		return Fail(reason, "OTNode has null origin");
	}
	if (node.kind == OTNode::Kind::JOIN) {
		if (node.origin->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			return Fail(reason, "JOIN OTNode origin is not LogicalComparisonJoin");
		}
		if (!node.children[0] || !node.children[1]) {
			return Fail(reason, "JOIN OTNode missing one or both children");
		}
		return CheckShape(*node.children[0], reason) && CheckShape(*node.children[1], reason);
	}
	if (node.children[0] || node.children[1]) {
		return Fail(reason, "RELATION OTNode has non-null children");
	}
	if (!IsBaseRelationSubtree(*node.origin)) {
		return Fail(reason, "RELATION OTNode origin is not a base-relation subtree");
	}
	return true;
}

void CollectRelationIds(const OTNode &node, vector<idx_t> &out) {
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

bool CheckRelationIdsUnique(const OperatorTree &ot, string *reason) {
	vector<idx_t> ids;
	CollectRelationIds(ot.Root(), ids);
	unordered_set<idx_t> seen;
	for (auto rid : ids) {
		if (!seen.insert(rid).second) {
			return Fail(reason, StringUtil::Format("duplicate relation_id %llu in OT", rid));
		}
	}
	return true;
}

} // namespace

bool OperatorTree::IsValid(string *reason) const {
	if (!root) {
		return Fail(reason, "OperatorTree has null root");
	}
	if (!CheckShape(*root, reason)) {
		return false;
	}
	if (!CheckRelationIdsUnique(*this, reason)) {
		return false;
	}
	return true;
}

} // namespace duckdb
