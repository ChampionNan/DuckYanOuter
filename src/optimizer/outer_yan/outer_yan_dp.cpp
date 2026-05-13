#include "duckdb/optimizer/outer_yan/outer_yan_dp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

// ============================================================================
// OuterYanDP -- construction
// ============================================================================

OuterYanDP::OuterYanDP(ClientContext &context_p) : context(context_p) {
}

void OuterYanDP::Optimize(OuterYanTree &tree) {
	if (!tree.HasOT()) {
		throw InternalException("OuterYanDP::Optimize: tree.ot is null");
	}

	BuildLeafRelations(tree);
	BuildEdgesFromOT(tree);
	InitLeafPlans(tree);

	if (num_relations <= 1) {
		return;
	}

	bool ok = false;
	if (num_relations >= THRESHOLD_TO_SWAP_TO_APPROXIMATE) {
		ok = SolveApproximately();
	} else {
		ok = SolveExactly();
		if (!ok) {
			// Pair cap tripped -- fall back to greedy.
			plans.clear();
			pairs = 0;
			InitLeafPlans(tree);
			ok = SolveApproximately();
		}
	}

	if (!ok || bailed_out) {
		// Path A bail-out: OT untouched, downstream passes see the original order.
		return;
	}

	unordered_set<RelationIndex> bindings;
	for (idx_t i = 0; i < num_relations; i++) {
		bindings.emplace(RelationIndex(i));
	}
	auto &full_set = set_manager.GetJoinRelation(bindings);
	if (plans.find(full_set) == plans.end()) {
		// DPccp could not connect all relations without a cross product.
		return;
	}
	RebuildOT(tree, full_set);
	tree.BuildOJT();
}

// ============================================================================
// Setup
// ============================================================================

void OuterYanDP::BuildLeafRelations(OuterYanTree &tree) {
	auto &ot_relations = tree.ot_relations_by_id;
	num_relations = ot_relations.size();
	for (idx_t r = 0; r < num_relations; r++) {
		auto *rel = ot_relations[r];
		if (!rel || rel->kind != OTNode::Kind::RELATION) {
			throw InternalException("OuterYanDP: missing RELATION OTNode for id %llu", r);
		}
		auto *base = FindBaseGet(rel->origin);
		if (base && base->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = base->Cast<LogicalGet>();
			rel->stats = RelationStatisticsHelper::ExtractGetStats(get, context);
		}
		if (rel->stats.cardinality == 0) {
			rel->stats.cardinality = 1;
		}
	}
}

void OuterYanDP::BuildEdgesFromOT(OuterYanTree &tree) {
	auto &ot_joins = tree.ot_joins_by_order;
	auto &ot_relations = tree.ot_relations_by_id;

	for (auto *jn : ot_joins) {
		if (!jn || jn->kind != OTNode::Kind::JOIN || !jn->origin) {
			throw InternalException("OuterYanDP: malformed JOIN OTNode in ot_joins_by_order");
		}
		auto &cmp = jn->origin->Cast<LogicalComparisonJoin>();
		if (cmp.conditions.empty()) {
			throw InternalException("OuterYanDP: JOIN OTNode has no conditions");
		}

		// D_edge = product over conditions of max(HLL(lhs_col), HLL(rhs_col)).
		// Each condition is its own equivalence class -- no transitivity.
		double d_edge = 1.0;
		auto lhs_col_first = FirstColumnRef(cmp.conditions[0].GetLHS());
		auto rhs_col_first = FirstColumnRef(cmp.conditions[0].GetRHS());
		if (!lhs_col_first || !rhs_col_first) {
			throw InternalException("OuterYanDP: conditions[0] has no resolvable column refs");
		}
		ColumnBinding lhs_binding = lhs_col_first->binding;
		ColumnBinding rhs_binding = rhs_col_first->binding;

		for (auto &cond : cmp.conditions) {
			auto l_col = FirstColumnRef(cond.GetLHS());
			auto r_col = FirstColumnRef(cond.GetRHS());
			if (!l_col || !r_col) {
				continue;
			}
			idx_t l_d = LookupDistinctCount(ot_relations[jn->left_child_relation_id]->stats,
			                                l_col->binding.column_index);
			idx_t r_d = LookupDistinctCount(ot_relations[jn->right_child_relation_id]->stats,
			                                r_col->binding.column_index);
			d_edge *= static_cast<double>(MaxValue<idx_t>(MaxValue<idx_t>(l_d, r_d), 1));
		}

		auto &left_set = set_manager.GetJoinRelation(RelationIndex(jn->left_child_relation_id));
		auto &right_set = set_manager.GetJoinRelation(RelationIndex(jn->right_child_relation_id));
		auto &combined_set = set_manager.Union(left_set, right_set);

		// One FilterInfo carrier per OT JOIN. `filter` stays null -- our cost
		// code never dereferences it; QueryGraphEdges uses only left_set /
		// right_set / set for indexing.
		auto fi = make_uniq<FilterInfo>(unique_ptr<Expression>(), combined_set, filter_infos.size(),
		                                static_cast<JoinType>(jn->join_kind));
		fi->SetLeftSet(&left_set);
		fi->SetRightSet(&right_set);
		fi->left_binding = lhs_binding;
		fi->right_binding = rhs_binding;

		OuterYanEdgeInfo info;
		info.kind = jn->join_kind;
		info.left_relation = jn->left_child_relation_id;
		info.right_relation = jn->right_child_relation_id;
		info.origin_join = jn;
		info.distinct_count = MaxValue<double>(d_edge, 1.0);

		auto *fi_ptr = fi.get();
		edge_meta[fi_ptr] = info;
		filter_infos.push_back(std::move(fi));

		query_graph.CreateEdge(left_set, right_set, fi_ptr);
		query_graph.CreateEdge(right_set, left_set, fi_ptr);
	}
}

void OuterYanDP::InitLeafPlans(OuterYanTree &tree) {
	auto &ot_relations = tree.ot_relations_by_id;
	for (idx_t i = 0; i < num_relations; i++) {
		auto &leaf_set = set_manager.GetJoinRelation(RelationIndex(i));
		auto node = make_uniq<DPJoinNode>(leaf_set);
		node->cost = 0.0;
		node->cardinality = MaxValue<idx_t>(ot_relations[i]->stats.cardinality, 1);
		plans[leaf_set] = std::move(node);
	}
}

// ============================================================================
// DPccp enumeration (reorderability check dropped vs PlanEnumerator)
// ============================================================================

bool OuterYanDP::SolveExactly() {
	for (idx_t i = num_relations; i > 0; i--) {
		auto &start_node = set_manager.GetJoinRelation(RelationIndex(i - 1));
		if (!EmitCSG(start_node)) {
			return false;
		}
		unordered_set<RelationIndex> exclusion_set;
		for (idx_t j = 0; j < i; j++) {
			exclusion_set.emplace(RelationIndex(j));
		}
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool OuterYanDP::EmitCSG(JoinRelationSet &node) {
	if (node.count == num_relations) {
		return true;
	}
	unordered_set<RelationIndex> exclusion_set;
	for (idx_t i = 0; i < node.relations[0].index; i++) {
		exclusion_set.emplace(RelationIndex(i));
	}
	UpdateExclusionSet(node, exclusion_set);
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	std::sort(neighbors.begin(), neighbors.end(), std::greater<RelationIndex>());

	unordered_set<RelationIndex> new_exclusion_set = exclusion_set;
	for (auto &nb : neighbors) {
		new_exclusion_set.insert(nb);
	}

	for (auto neighbor : neighbors) {
		auto &neighbor_relation = set_manager.GetJoinRelation(neighbor);
		auto connections = query_graph.GetConnections(node, neighbor_relation);
		if (!connections.empty()) {
			if (!TryEmitPair(node, neighbor_relation, connections)) {
				return false;
			}
		}
		if (!EnumerateCmpRecursive(node, neighbor_relation, new_exclusion_set)) {
			return false;
		}
		new_exclusion_set.erase(neighbor);
	}
	return true;
}

bool OuterYanDP::EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right,
                                       unordered_set<RelationIndex> &exclusion_set) {
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	auto all_subset = GetAllNeighborSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto &neighbor = set_manager.GetJoinRelation(rel_set);
		auto &combined = set_manager.Union(right, neighbor);
		if (plans.find(combined) != plans.end()) {
			auto connections = query_graph.GetConnections(left, combined);
			if (!connections.empty()) {
				if (!TryEmitPair(left, combined, connections)) {
					return false;
				}
			}
		}
		union_sets.push_back(combined);
	}
	unordered_set<RelationIndex> new_exclusion_set = exclusion_set;
	for (const auto &nb : neighbors) {
		new_exclusion_set.insert(nb);
	}
	for (idx_t i = 0; i < union_sets.size(); i++) {
		if (!EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool OuterYanDP::EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<RelationIndex> &exclusion_set) {
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	auto all_subset = GetAllNeighborSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto &neighbor = set_manager.GetJoinRelation(rel_set);
		auto &new_set = set_manager.Union(node, neighbor);
		if (plans.find(new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets.push_back(new_set);
	}
	unordered_set<RelationIndex> new_exclusion_set = exclusion_set;
	for (const auto &nb : neighbors) {
		new_exclusion_set.insert(nb);
	}
	for (idx_t i = 0; i < union_sets.size(); i++) {
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool OuterYanDP::TryEmitPair(JoinRelationSet &left, JoinRelationSet &right,
                             const vector<reference<NeighborInfo>> &info) {
	pairs++;
	if (pairs >= MAX_PAIRS) {
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

DPJoinNode &OuterYanDP::EmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                 const vector<reference<NeighborInfo>> &info) {
	auto left_plan_it = plans.find(left);
	auto right_plan_it = plans.find(right);
	if (left_plan_it == plans.end() || right_plan_it == plans.end()) {
		throw InternalException("OuterYanDP::EmitPair: missing left/right plan");
	}
	auto &new_set = set_manager.Union(left, right);
	auto new_plan = CreateJoinTree(new_set, info, *left_plan_it->second, *right_plan_it->second);

	auto entry = plans.find(new_set);
	double new_cost = new_plan->cost;
	double old_cost = NumericLimits<double>::Maximum();
	if (entry != plans.end()) {
		old_cost = entry->second->cost;
	}
	if (entry == plans.end() || new_cost < old_cost) {
		plans[new_set] = std::move(new_plan);
		return *plans[new_set];
	}
	return *entry->second;
}

unique_ptr<DPJoinNode> OuterYanDP::CreateJoinTree(JoinRelationSet &set,
                                                  const vector<reference<NeighborInfo>> &possible_connections,
                                                  DPJoinNode &left, DPJoinNode &right) {
	// OuterYan applicability guarantees an acyclic predicate graph: there is
	// at most one OT JOIN connecting any partition cut, so the first non-null
	// FilterInfo is the answer.
	optional_ptr<NeighborInfo> best_connection = possible_connections.back().get();
	optional_ptr<FilterInfo> chosen_filter;
	for (auto &connection : possible_connections) {
		for (auto &filter : connection.get().filters) {
			if (filter) {
				best_connection = connection.get();
				chosen_filter = filter;
				break;
			}
		}
		if (chosen_filter) {
			break;
		}
	}
	if (!chosen_filter) {
		throw InternalException("OuterYanDP::CreateJoinTree: no non-cross-product connection found");
	}
	auto meta_it = edge_meta.find(chosen_filter.get());
	if (meta_it == edge_meta.end()) {
		throw InternalException("OuterYanDP::CreateJoinTree: missing edge metadata");
	}

	double combined_card = EdgeCardinality(left.set, right.set, left, right, meta_it->second);
	double cost = ComputeCost(left, right, combined_card);

	auto result = make_uniq<DPJoinNode>(set, best_connection, left.set, right.set, cost);
	auto card_idx = static_cast<idx_t>(MinValue<double>(combined_card,
	                                                     static_cast<double>(NumericLimits<idx_t>::Maximum())));
	result->cardinality = MaxValue<idx_t>(card_idx, 1);
	return result;
}

// ============================================================================
// Cost (user's outer-join formula)
// ============================================================================

double OuterYanDP::EdgeCardinality(JoinRelationSet &left_set, JoinRelationSet &right_set,
                                    const DPJoinNode &left_plan, const DPJoinNode &right_plan,
                                    const OuterYanEdgeInfo &edge_info) {
	bool left_holds_lhs = false;
	for (idx_t i = 0; i < left_set.count; i++) {
		if (left_set.relations[i].index == edge_info.left_relation) {
			left_holds_lhs = true;
			break;
		}
	}
	if (!left_holds_lhs) {
		bool right_holds_lhs = false;
		for (idx_t i = 0; i < right_set.count; i++) {
			if (right_set.relations[i].index == edge_info.left_relation) {
				right_holds_lhs = true;
				break;
			}
		}
		if (!right_holds_lhs) {
			throw InternalException("OuterYanDP::EdgeCardinality: edge LHS relation absent from both sides");
		}
	}

	const DPJoinNode &L = left_holds_lhs ? left_plan : right_plan;
	const DPJoinNode &R = left_holds_lhs ? right_plan : left_plan;
	OuterYanJoinKind kind = edge_info.kind;
	if (!left_holds_lhs) {
		// Orientation flipped: relabel LEFT <-> RIGHT so the formula reads
		// in terms of the chosen (L, R) operands.
		kind = FlipOuterYanJoinKind(kind);
	}

	const double t_l = static_cast<double>(L.cardinality);
	const double t_r = static_cast<double>(R.cardinality);
	const double d_e = edge_info.distinct_count;
	const double t_inner = (t_l * t_r) / d_e;

	const double p_l_not_r = MaxValue<double>(0.0, 1.0 - (t_r / d_e));
	const double p_r_not_l = MaxValue<double>(0.0, 1.0 - (t_l / d_e));

	switch (kind) {
	case JoinType::INNER:
		return t_inner;
	case JoinType::LEFT:
		return t_inner + t_l * p_l_not_r;
	case JoinType::RIGHT:
		return t_inner + t_r * p_r_not_l;
	case JoinType::OUTER:
		return t_inner + t_l * p_l_not_r + t_r * p_r_not_l;
	default:
		throw InternalException("OuterYanDP::EdgeCardinality: unsupported join kind");
	}
}

double OuterYanDP::ComputeCost(const DPJoinNode &left, const DPJoinNode &right, double combined_cardinality) {
	// Same shape as CostModel::ComputeCost: produced cardinality + child costs.
	return combined_cardinality + left.cost + right.cost;
}

// ============================================================================
// Greedy fallback (no cross products)
// ============================================================================

bool OuterYanDP::SolveApproximately() {
	pairs = 0;
	vector<reference<JoinRelationSet>> live;
	for (idx_t i = 0; i < num_relations; i++) {
		live.push_back(set_manager.GetJoinRelation(RelationIndex(i)));
	}

	while (live.size() > 1) {
		idx_t best_left = 0, best_right = 0;
		optional_ptr<DPJoinNode> best_node;

		for (idx_t i = 0; i < live.size(); i++) {
			auto &l = live[i].get();
			for (idx_t j = i + 1; j < live.size(); j++) {
				auto &r = live[j].get();
				auto connections = query_graph.GetConnections(l, r);
				if (connections.empty()) {
					continue;
				}
				auto &candidate = EmitPair(l, r, connections);
				if (!best_node || candidate.cost < best_node->cost) {
					best_node = &candidate;
					best_left = i;
					best_right = j;
				}
			}
		}

		if (!best_node) {
			// No non-cross-product connection -- Path A bail-out.
			bailed_out = true;
			return false;
		}

		auto &new_set = set_manager.Union(live[best_left].get(), live[best_right].get());
		live.erase(live.begin() + static_cast<int64_t>(best_right));
		live.erase(live.begin() + static_cast<int64_t>(best_left));
		live.push_back(new_set);
	}
	return true;
}

// ============================================================================
// Materialisation -- DP plan back to OT
// ============================================================================

FilterInfo &OuterYanDP::PickConnectingEdge(const DPJoinNode &node) {
	if (!node.info) {
		throw InternalException("OuterYanDP::PickConnectingEdge: leaf DPJoinNode");
	}
	for (auto &filter : node.info->filters) {
		if (filter) {
			return *filter;
		}
	}
	throw InternalException("OuterYanDP::PickConnectingEdge: NeighborInfo has no filter");
}

unique_ptr<OTNode> OuterYanDP::MaterializeNode(JoinRelationSet &set, OuterYanTree &tree, idx_t &next_order) {
	auto plan_it = plans.find(set);
	if (plan_it == plans.end()) {
		throw InternalException("OuterYanDP::MaterializeNode: no plan for set %s", set.ToString());
	}
	auto &node = *plan_it->second;

	if (set.count == 1) {
		// Leaf: clone the original RELATION OTNode (origin pointer + stats
		// kept; subtree_relations is repopulated by Finalize).
		auto rel_id = set.relations[0].index;
		auto *src = tree.ot_relations_by_id[rel_id];
		auto leaf = make_uniq<OTNode>();
		leaf->kind = OTNode::Kind::RELATION;
		leaf->origin = src->origin;
		leaf->relation_id = rel_id;
		leaf->stats = src->stats;
		return leaf;
	}

	auto left_child = MaterializeNode(node.left_set, tree, next_order);
	auto right_child = MaterializeNode(node.right_set, tree, next_order);

	auto &chosen_filter = PickConnectingEdge(node);
	auto meta_it = edge_meta.find(&chosen_filter);
	if (meta_it == edge_meta.end()) {
		throw InternalException("OuterYanDP::MaterializeNode: missing edge metadata");
	}
	auto *origin_join = meta_it->second.origin_join;

	auto join = make_uniq<OTNode>();
	join->kind = OTNode::Kind::JOIN;
	join->origin = origin_join->origin;
	join->join_kind = origin_join->join_kind;
	join->original_join_kind = origin_join->original_join_kind;
	join->left_child_relation_id = origin_join->left_child_relation_id;
	join->right_child_relation_id = origin_join->right_child_relation_id;
	join->children[0] = std::move(left_child);
	join->children[1] = std::move(right_child);
	join->order = ++next_order;
	return join;
}

void OuterYanDP::RebuildOT(OuterYanTree &tree, JoinRelationSet &full_set) {
	idx_t next_order = 0;
	auto new_root = MaterializeNode(full_set, tree, next_order);

	tree.OT().root = std::move(new_root);
	// Re-canonicalise (cond LHS/RHS aligned with new children's subtree_relations)
	// and re-populate subtree_relations.
	tree.OT().Finalize();

	tree.ot_joins_by_order.clear();
	RefreshOTJoinsByOrder(*tree.OT().root, tree.ot_joins_by_order);
	// Renumber `order` densely after Finalize; MaterializeNode produced
	// post-order numbering but Finalize may have left it stale.
	for (idx_t i = 0; i < tree.ot_joins_by_order.size(); i++) {
		tree.ot_joins_by_order[i]->order = i + 1;
	}
}

// ============================================================================
// Static private helpers
// ============================================================================

LogicalOperator *OuterYanDP::FindBaseGet(LogicalOperator *op) {
	while (op) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER ||
		    op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			if (op->children.size() != 1 || !op->children[0]) {
				return op;
			}
			op = op->children[0].get();
			continue;
		}
		return op;
	}
	return nullptr;
}

optional_ptr<const BoundColumnRefExpression> OuterYanDP::FirstColumnRef(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return &expr.Cast<BoundColumnRefExpression>();
	}
	optional_ptr<const BoundColumnRefExpression> result;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!result) {
			result = FirstColumnRef(child);
		}
	});
	return result;
}

idx_t OuterYanDP::LookupDistinctCount(const RelationStats &stats, idx_t column_index) {
	if (!stats.stats_initialized) {
		return 1;
	}
	if (column_index >= stats.column_distinct_count.size()) {
		return MaxValue<idx_t>(stats.cardinality, 1);
	}
	auto dc = stats.column_distinct_count[column_index].distinct_count;
	return MaxValue<idx_t>(dc, 1);
}

void OuterYanDP::UpdateExclusionSet(JoinRelationSet &node, unordered_set<RelationIndex> &exclusion_set) {
	for (idx_t i = 0; i < node.count; i++) {
		exclusion_set.insert(node.relations[i]);
	}
}

vector<unordered_set<RelationIndex>>
OuterYanDP::AddSuperSets(const vector<unordered_set<RelationIndex>> &current,
                          const vector<RelationIndex> &all_neighbors) {
	vector<unordered_set<RelationIndex>> ret;
	for (const auto &neighbor_set : current) {
		auto max_val = std::max_element(neighbor_set.begin(), neighbor_set.end());
		for (const auto &neighbor : all_neighbors) {
			if (*max_val >= neighbor) {
				continue;
			}
			if (neighbor_set.count(neighbor) == 0) {
				unordered_set<RelationIndex> new_set;
				for (auto &n : neighbor_set) {
					new_set.insert(n);
				}
				new_set.insert(neighbor);
				ret.push_back(std::move(new_set));
			}
		}
	}
	return ret;
}

vector<unordered_set<RelationIndex>> OuterYanDP::GetAllNeighborSets(vector<RelationIndex> neighbors) {
	vector<unordered_set<RelationIndex>> ret;
	std::sort(neighbors.begin(), neighbors.end());
	vector<unordered_set<RelationIndex>> added;
	for (auto &neighbor : neighbors) {
		added.push_back(unordered_set<RelationIndex>({neighbor}));
		ret.push_back(unordered_set<RelationIndex>({neighbor}));
	}
	do {
		added = AddSuperSets(added, neighbors);
		for (auto &d : added) {
			ret.push_back(d);
		}
	} while (!added.empty());
	return ret;
}

void OuterYanDP::RefreshOTJoinsByOrder(OTNode &node, vector<OTNode *> &out) {
	if (node.kind != OTNode::Kind::JOIN) {
		return;
	}
	RefreshOTJoinsByOrder(*node.children[0], out);
	RefreshOTJoinsByOrder(*node.children[1], out);
	out.push_back(&node);
}

} // namespace duckdb
