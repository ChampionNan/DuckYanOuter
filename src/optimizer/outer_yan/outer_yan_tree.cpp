#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/relation_index.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>

namespace duckdb {

// ============================================================================
// File-local helpers
// ============================================================================

namespace {

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

void MapTableToRelId(const LogicalOperator &op, idx_t relation_id,
                     unordered_map<idx_t, RelationId> &table_to_relation) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		table_to_relation[get.table_index.index] = relation_id;
	}
	for (auto &child : op.children) {
		if (child) {
			MapTableToRelId(*child, relation_id, table_to_relation);
		}
	}
}

void CollectReferencedRelations(const Expression &expr,
                                const unordered_map<idx_t, RelationId> &table_to_relation,
                                unordered_set<idx_t> &out) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &cref = expr.Cast<BoundColumnRefExpression>();
		auto it = table_to_relation.find(cref.binding.table_index.index);
		if (it != table_to_relation.end()) {
			out.insert(it->second);
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		CollectReferencedRelations(child, table_to_relation, out);
	});
}

optional<idx_t> LookupSingleRelation(const Expression &expr,
                                     const unordered_map<idx_t, RelationId> &table_to_relation) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &cref = expr.Cast<BoundColumnRefExpression>();
		auto it = table_to_relation.find(cref.binding.table_index.index);
		if (it != table_to_relation.end()) {
			return it->second;
		}
		return optional<idx_t>();
	}
	optional<idx_t> result;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!result) {
			result = LookupSingleRelation(child, table_to_relation);
		}
	});
	return result;
}

bool CheckPKFK(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER ||
	    op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (op.children.size() != 1) {
			return false;
		}
		return CheckPKFK(*op.children[0]);
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = op.Cast<LogicalGet>();
	auto table_entry = get.GetTable();
	if (!table_entry) {
		return false;
	}
	auto &constraints = table_entry->GetConstraints();
	auto bindings = op.GetColumnBindings();
	const auto &column_ids = get.GetColumnIds();
	for (idx_t i = 0; i < bindings.size(); i++) {
		idx_t col_idx = bindings[i].column_index;
		if (col_idx >= column_ids.size()) {
			continue;
		}
		idx_t actual_col_idx = column_ids[col_idx].GetPrimaryIndex();
		for (auto &constraint : constraints) {
			if (constraint->type != ConstraintType::UNIQUE) {
				continue;
			}
			auto &unique_constraint = constraint->Cast<UniqueConstraint>();
			if (unique_constraint.index.index != DConstants::INVALID_INDEX) {
				if (unique_constraint.index.index == actual_col_idx) {
					return true;
				}
			} else if (!unique_constraint.columns.empty()) {
				bool all_available = true;
				for (auto &constraint_col_name : unique_constraint.columns) {
					bool found = false;
					for (idx_t j = 0; j < bindings.size(); j++) {
						idx_t other_col_idx = bindings[j].column_index;
						if (other_col_idx >= column_ids.size()) {
							continue;
						}
						if (other_col_idx >= get.names.size()) {
							continue;
						}
						if (StringUtil::CIEquals(get.names[other_col_idx], constraint_col_name)) {
							found = true;
							break;
						}
					}
					if (!found) {
						all_available = false;
						break;
					}
				}
				if (all_available) {
					return true;
				}
			}
		}
	}
	return false;
}

} // namespace

// ============================================================================
// CheckApplicability
// ============================================================================

bool OuterYanTree::CheckApplicability(LogicalOperator &plan) {
	applicability = OuterYanApplicability::Check(plan);
	return applicability.applicable;
}

// ============================================================================
// BuildOT — Collect phase
// ============================================================================

namespace {

class OTBuilder {
public:
	OTBuilder(LogicalOperator &plan_p, OuterYanTree &tree_p)
	    : plan_root(plan_p), tree(tree_p) {
	}

	unique_ptr<OperatorTree> Build() {
		auto root = BuildNode(plan_root, /*depth*/ 0);
		AssignJoinOrders();
		SetJoinTwoSides();

		auto ot = make_uniq<OperatorTree>();
		ot->root = std::move(root);
		return ot;
	}

private:
	LogicalOperator &plan_root;
	OuterYanTree &tree;

	idx_t next_relation_id = 0;

	struct JoinAtDepth {
		OTNode *node;
		idx_t depth;
	};
	vector<JoinAtDepth> joins_with_depth;

	unique_ptr<OTNode> BuildNode(LogicalOperator &op, idx_t depth) {
		if (IsBaseRelationSubtree(op)) {
			return BuildRelationNode(op);
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			return BuildJoinNode(op, depth);
		case LogicalOperatorType::LOGICAL_FILTER:
			return BuildThroughFilter(op, depth);
		default:
			throw NotImplementedException("LogicalPlanToOT: unsupported operator %s",
			                              EnumUtil::ToString(op.type));
		}
	}

	unique_ptr<OTNode> BuildRelationNode(LogicalOperator &op) {
		auto node = make_uniq<OTNode>();
		node->kind = OTNode::Kind::RELATION;
		node->origin = &op;
		node->relation_id = next_relation_id++;
		MapTableToRelId(op, node->relation_id, tree.table_to_relation);
		if (node->relation_id >= tree.ot_relations_by_id.size()) {
			tree.ot_relations_by_id.resize(node->relation_id + 1, nullptr);
		}
		tree.ot_relations_by_id[node->relation_id] = node.get();
		return node;
	}

	unique_ptr<OTNode> BuildJoinNode(LogicalOperator &op, idx_t depth) {
		if (op.children.size() != 2) {
			throw NotImplementedException(
			    "LogicalPlanToOT: LogicalComparisonJoin with %llu children (expected 2)",
			    op.children.size());
		}
		auto &join = op.Cast<LogicalComparisonJoin>();
		auto node = make_uniq<OTNode>();
		node->kind = OTNode::Kind::JOIN;
		node->origin = &op;
		node->join_kind = join.join_type;
		node->original_join_kind = join.join_type;
		joins_with_depth.push_back({node.get(), depth});
		node->children[0] = BuildNode(*op.children[0], depth + 1);
		node->children[1] = BuildNode(*op.children[1], depth + 1);
		return node;
	}

	unique_ptr<OTNode> BuildThroughFilter(LogicalOperator &op, idx_t depth) {
		if (op.children.size() != 1) {
			throw InternalException("LogicalFilter with %llu children", op.children.size());
		}
		auto child_node = BuildNode(*op.children[0], depth);
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			unordered_set<idx_t> refs;
			CollectReferencedRelations(*expr, tree.table_to_relation, refs);
			if (refs.empty()) {
				throw NotImplementedException(
				    "LogicalPlanToOT: residual filter expression references no OT base "
				    "relation (likely a projection-introduced table_index above the join "
				    "tree); not yet supported");
			}
			auto rec = make_uniq<OuterYanFilterRecord>(std::move(expr), std::move(refs),
			                                           tree.filter_records.size());
			rec->from_residual_predicate = true;
			tree.filter_records.push_back(std::move(rec));
		}
		filter.expressions.clear();
		return child_node;
	}

	void AssignJoinOrders() {
		auto sorted = joins_with_depth;
		std::stable_sort(sorted.begin(), sorted.end(),
		                 [](const JoinAtDepth &a, const JoinAtDepth &b) {
			                 return a.depth > b.depth;
		                 });
		tree.ot_joins_by_order.clear();
		tree.ot_joins_by_order.reserve(sorted.size());
		for (idx_t i = 0; i < sorted.size(); i++) {
			sorted[i].node->order = i + 1;
			tree.ot_joins_by_order.push_back(sorted[i].node);
		}
	}

	void SetJoinTwoSides() {
		for (auto &entry : joins_with_depth) {
			auto *node = entry.node;
			auto &join = node->origin->Cast<LogicalComparisonJoin>();
			if (join.conditions.empty()) {
				throw NotImplementedException(
				    "LogicalPlanToOT: cross product (join with no conditions)");
			}
			auto &cond = join.conditions[0];
			auto left = LookupSingleRelation(cond.GetLHS(), tree.table_to_relation);
			auto right = LookupSingleRelation(cond.GetRHS(), tree.table_to_relation);
			if (!left || !right) {
				throw NotImplementedException(
				    "LogicalPlanToOT: could not resolve both sides of a join condition "
				    "to a single OT base relation");
			}
			node->left_child_relation_id = *left;
			node->right_child_relation_id = *right;
		}
	}
};

} // namespace

void OuterYanTree::BuildOT(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		throw InternalException("OuterYanTree::BuildOT: null plan");
	}
	OTBuilder builder(*plan, *this);
	ot = builder.Build();
	source_plan = std::move(plan);
}

// ============================================================================
// BuildOJT
// ============================================================================

void OuterYanTree::BuildOJT() {
	if (!ot) {
		throw InternalException("OuterYanTree::BuildOJT: ot is null");
	}

	const auto &ot_relations = ot_relations_by_id;
	const auto &ot_joins = ot_joins_by_order;

	if (ot_relations.empty()) {
		throw NotImplementedException("OuterYanTree::BuildOJT: OT has no base relations");
	}
	for (idx_t r = 0; r < ot_relations.size(); r++) {
		if (!ot_relations[r]) {
			throw InternalException(
			    "OuterYanTree::BuildOJT: missing OT relation for relation_id %llu", r);
		}
	}

	vector<unique_ptr<OJTNode>> nodes_by_id;
	nodes_by_id.reserve(ot_relations.size());
	for (idx_t r = 0; r < ot_relations.size(); r++) {
		auto ojt_node = make_uniq<OJTNode>();
		ojt_node->relation_id = r;
		ojt_node->base_op = ot_relations[r]->origin;
		nodes_by_id.push_back(std::move(ojt_node));
	}

	if (ot_joins.empty()) {
		if (nodes_by_id.size() != 1) {
			throw InternalException(
			    "OuterYanTree::BuildOJT: %llu relations with 0 joins (disconnected OT)",
			    nodes_by_id.size());
		}
		ojt = make_uniq<OrderedJoinTree>(std::move(nodes_by_id[0]));
		return;
	}

	vector<OJTNode *> node_pointer(nodes_by_id.size(), nullptr);
	for (idx_t i = 0; i < nodes_by_id.size(); i++) {
		node_pointer[i] = nodes_by_id[i].get();
	}

	unordered_set<idx_t> attached;
	idx_t ojt_root_id = DConstants::INVALID_INDEX;
	const idx_t n_joins = ot_joins.size();

	for (idx_t step = 0; step < n_joins; step++) {
		idx_t idx = n_joins - 1 - step;
		auto *ot_join = ot_joins[idx];
		idx_t left_rel = ot_join->left_child_relation_id;
		idx_t right_rel = ot_join->right_child_relation_id;

		idx_t parent_id;
		idx_t child_id;
		if (step == 0) {
			bool left_extends = false;
			bool right_extends = false;
			if (n_joins >= 2) {
				auto *next_join = ot_joins[idx - 1];
				idx_t nl = next_join->left_child_relation_id;
				idx_t nr = next_join->right_child_relation_id;
				left_extends = (left_rel == nl || left_rel == nr);
				right_extends = (right_rel == nl || right_rel == nr);
			}
			if (left_extends && !right_extends) {
				parent_id = right_rel;
				child_id = left_rel;
			} else if (!left_extends && right_extends) {
				parent_id = left_rel;
				child_id = right_rel;
			} else {
				parent_id = left_rel;
				child_id = right_rel;
			}
			ojt_root_id = parent_id;
			attached.insert(parent_id);
			attached.insert(child_id);
		} else {
			bool l_in = attached.count(left_rel) > 0;
			bool r_in = attached.count(right_rel) > 0;
			if (l_in && r_in) {
				throw NotImplementedException(
				    "OuterYanTree::BuildOJT: cyclic join graph (both endpoints already in OJT)");
			}
			if (!l_in && !r_in) {
				throw InternalException(
				    "OuterYanTree::BuildOJT: top-down step found neither endpoint in the partial "
				    "OJT (connectivity invariant violated)");
			}
			if (l_in) {
				parent_id = left_rel;
				child_id = right_rel;
			} else {
				parent_id = right_rel;
				child_id = left_rel;
			}
			attached.insert(child_id);
		}

		auto *origin_join = ot_join->origin;
		bool left_pk = CheckPKFK(*origin_join->children[0]);
		bool right_pk = CheckPKFK(*origin_join->children[1]);
		bool child_pk = (child_id == left_rel) ? left_pk : right_pk;

		bool parent_is_left = (parent_id == left_rel);

		OJTEdge edge;
		edge.kind = CheckOuterYanJoinFlip(ot_join->join_kind, parent_is_left);
		edge.original_kind =
		    CheckOuterYanJoinFlip(ot_join->original_join_kind, parent_is_left);
		edge.order = ot_join->order;
		edge.join_op = origin_join;
		edge.has_pk_fk_constraint = left_pk || right_pk;
		edge.child_is_pk = child_pk;
		edge.child = std::move(nodes_by_id[child_id]);

		node_pointer[parent_id]->children.push_back(std::move(edge));
	}

	if (ojt_root_id == DConstants::INVALID_INDEX) {
		throw InternalException("OuterYanTree::BuildOJT: OJT root not assigned");
	}
	ojt = make_uniq<OrderedJoinTree>(std::move(nodes_by_id[ojt_root_id]));
}

// ============================================================================
// OJTToLogicalPlan
// ============================================================================

namespace {

struct EdgeBuildInfo {
	idx_t parent_rel;
	idx_t child_rel;
	OuterYanJoinKind kind;
	idx_t order;
	LogicalOperator *join_op;
	bool parent_was_left;
};

void CollectTablesUnder(const LogicalOperator &op, unordered_set<idx_t> &out) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		out.insert(get.table_index.index);
	}
	for (auto &child : op.children) {
		if (child) {
			CollectTablesUnder(*child, out);
		}
	}
}

bool ParentIsOnOriginalLeft(const OJTNode &parent_node, const LogicalOperator &join_op) {
	if (!parent_node.base_op || join_op.children.size() != 2 || !join_op.children[0]) {
		throw InternalException("OJTToLogicalPlan: malformed parent node or join_op");
	}
	unordered_set<idx_t> parent_tables;
	CollectTablesUnder(*parent_node.base_op, parent_tables);
	unordered_set<idx_t> left_tables;
	CollectTablesUnder(*join_op.children[0], left_tables);
	for (auto table : parent_tables) {
		if (left_tables.count(table)) {
			return true;
		}
	}
	return false;
}

void CollectOJTInfo(OJTNode &node, vector<OJTNode *> &nodes_by_rel,
                    vector<EdgeBuildInfo> &edges) {
	if (node.relation_id >= nodes_by_rel.size()) {
		nodes_by_rel.resize(node.relation_id + 1, nullptr);
	}
	nodes_by_rel[node.relation_id] = &node;
	for (auto &edge : node.children) {
		if (!edge.child) {
			throw InternalException("OJTToLogicalPlan: edge has no child");
		}
		if (!edge.join_op) {
			throw InternalException("OJTToLogicalPlan: edge has no join_op");
		}
		if (edge.join_op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			throw InternalException(
			    "OJTToLogicalPlan: edge.join_op is not a LogicalComparisonJoin");
		}
		EdgeBuildInfo info;
		info.parent_rel = node.relation_id;
		info.child_rel = edge.child->relation_id;
		info.kind = edge.kind;
		info.order = edge.order;
		info.join_op = edge.join_op;
		info.parent_was_left = ParentIsOnOriginalLeft(node, *edge.join_op);
		edges.push_back(info);
		CollectOJTInfo(*edge.child, nodes_by_rel, edges);
	}
}

void PushFiltersAboveLeaf(unique_ptr<LogicalOperator> &subplan,
                          vector<unique_ptr<Expression>> filters) {
	if (filters.empty()) {
		return;
	}
	auto new_filter = make_uniq<LogicalFilter>();
	new_filter->expressions = std::move(filters);
	if (!subplan || subplan->children.empty()) {
		new_filter->children.push_back(std::move(subplan));
		subplan = std::move(new_filter);
		return;
	}
	LogicalOperator *cur = subplan.get();
	while (!cur->children.empty() && cur->children[0] && !cur->children[0]->children.empty()) {
		cur = cur->children[0].get();
	}
	if (cur->children.empty() || !cur->children[0]) {
		throw InternalException(
		    "OJTToLogicalPlan: base subplan has no leaf to insert filter above");
	}
	auto leaf = std::move(cur->children[0]);
	new_filter->children.push_back(std::move(leaf));
	cur->children[0] = std::move(new_filter);
}

void TryPushFilter(unique_ptr<LogicalOperator> &subplan, unique_ptr<Expression> expr) {
	if (subplan->type == LogicalOperatorType::LOGICAL_FILTER) {
		subplan->Cast<LogicalFilter>().expressions.push_back(std::move(expr));
		return;
	}
	auto wrap = make_uniq<LogicalFilter>();
	wrap->expressions.push_back(std::move(expr));
	wrap->children.push_back(std::move(subplan));
	subplan = std::move(wrap);
}

unique_ptr<LogicalOperator> BuildJoinFromEdge(const EdgeBuildInfo &edge,
                                              unique_ptr<LogicalOperator> parent_plan,
                                              unique_ptr<LogicalOperator> child_plan) {
	auto &orig_join = edge.join_op->Cast<LogicalComparisonJoin>();
	auto new_join = make_uniq<LogicalComparisonJoin>(edge.kind);
	for (const auto &cond : orig_join.conditions) {
		JoinCondition copy = cond.Copy();
		if (!edge.parent_was_left) {
			copy.Swap();
		}
		new_join->conditions.push_back(std::move(copy));
	}
	new_join->children.push_back(std::move(parent_plan));
	new_join->children.push_back(std::move(child_plan));
	new_join->ResolveOperatorTypes();
	return new_join;
}

} // namespace

unique_ptr<LogicalOperator> OuterYanTree::OJTToLogicalPlan(ClientContext &context) {
	if (!ojt) {
		throw InternalException("OuterYanTree::OJTToLogicalPlan: ojt is null");
	}
	auto &ojt_ref = *ojt;

	vector<OJTNode *> nodes_by_rel;
	vector<EdgeBuildInfo> edges;
	CollectOJTInfo(ojt_ref.Root(), nodes_by_rel, edges);
	const idx_t n_rels = nodes_by_rel.size();
	if (n_rels == 0) {
		throw InternalException("OuterYanTree::OJTToLogicalPlan: OJT has no relations");
	}

	JoinRelationSetManager set_manager;
	reference_map_t<JoinRelationSet, unique_ptr<LogicalOperator>> subplan_memo;
	vector<reference<JoinRelationSet>> current_set_by_rel;
	current_set_by_rel.reserve(n_rels);
	vector<bool> filter_records_status(filter_records.size(), false);

	for (idx_t r = 0; r < n_rels; r++) {
		auto *node = nodes_by_rel[r];
		if (!node || !node->base_op) {
			throw InternalException("OuterYanTree::OJTToLogicalPlan: missing OJTNode or base_op");
		}
		auto &singleton = set_manager.GetJoinRelation(RelationIndex(r));
		auto subplan = node->base_op->Copy(context);
		if (!subplan) {
			throw InternalException("OuterYanTree::OJTToLogicalPlan: base_op->Copy returned null");
		}
		if (!node->filters.empty()) {
			PushFiltersAboveLeaf(subplan, std::move(node->filters));
		}
		for (idx_t i = 0; i < filter_records.size(); i++) {
			if (filter_records_status[i]) {
				continue;
			}
			auto &rec = filter_records[i];
			if (!rec || !rec->filter) {
				continue;
			}
			if (rec->referenced_relations.size() == 1 &&
			    *rec->referenced_relations.begin() == r) {
				TryPushFilter(subplan, std::move(rec->filter));
				filter_records_status[i] = true;
			}
		}
		subplan_memo.emplace(singleton, std::move(subplan));
		current_set_by_rel.emplace_back(singleton);
	}

	std::sort(edges.begin(), edges.end(),
	          [](const EdgeBuildInfo &a, const EdgeBuildInfo &b) { return a.order < b.order; });

	for (auto &e : edges) {
		auto &parent_set = current_set_by_rel[e.parent_rel].get();
		auto &child_set = current_set_by_rel[e.child_rel].get();
		if (&parent_set == &child_set) {
			throw InternalException(
			    "OuterYanTree::OJTToLogicalPlan: edge endpoints already in the same group "
			    "(cyclic OJT)");
		}

		auto parent_it = subplan_memo.find(parent_set);
		auto child_it = subplan_memo.find(child_set);
		if (parent_it == subplan_memo.end() || child_it == subplan_memo.end()) {
			throw InternalException(
			    "OuterYanTree::OJTToLogicalPlan: subplan_memo missing entry for edge endpoint");
		}
		auto parent_plan = std::move(parent_it->second);
		auto child_plan = std::move(child_it->second);
		subplan_memo.erase(parent_it);
		subplan_memo.erase(child_it);

		auto new_join = BuildJoinFromEdge(e, std::move(parent_plan), std::move(child_plan));
		auto &merged = set_manager.Union(parent_set, child_set);

		for (idx_t i = 0; i < parent_set.count; i++) {
			current_set_by_rel[parent_set.relations[i].index] = merged;
		}
		for (idx_t i = 0; i < child_set.count; i++) {
			current_set_by_rel[child_set.relations[i].index] = merged;
		}
		auto memo_it = subplan_memo.emplace(merged, std::move(new_join)).first;
		for (idx_t i = 0; i < filter_records.size(); i++) {
			if (filter_records_status[i]) {
				continue;
			}
			auto &rec = filter_records[i];
			if (!rec || !rec->filter) {
				continue;
			}
			bool covered = true;
			for (auto rel : rec->referenced_relations) {
				if (&current_set_by_rel[rel].get() != &merged) {
					covered = false;
					break;
				}
			}
			if (covered) {
				TryPushFilter(memo_it->second, std::move(rec->filter));
				filter_records_status[i] = true;
			}
		}
	}

	if (subplan_memo.size() != 1) {
		throw InternalException(
		    "OuterYanTree::OJTToLogicalPlan: relations remain in disjoint groups after assembly");
	}
	auto plan = std::move(subplan_memo.begin()->second);
	if (!plan) {
		throw InternalException("OuterYanTree::OJTToLogicalPlan: final memo entry is null");
	}

	for (idx_t i = 0; i < filter_records.size(); i++) {
		if (filter_records_status[i]) {
			continue;
		}
		auto &rec = filter_records[i];
		if (!rec || !rec->filter) {
			continue;
		}
		TryPushFilter(plan, std::move(rec->filter));
		filter_records_status[i] = true;
	}

	// Release wrapper-owned resources whose lifetime ends here.
	source_plan.reset();
	ojt.reset();
	return plan;
}

} // namespace duckdb
