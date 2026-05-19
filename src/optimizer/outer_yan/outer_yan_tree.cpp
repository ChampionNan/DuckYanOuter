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
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>

namespace duckdb {

// ============================================================================
// BuildOT helpers
// ============================================================================

void OuterYanTree::MapTableToRelId(const LogicalOperator &op, idx_t relation_id,
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

void OuterYanTree::CollectReferencedRelations(const Expression &expr,
                                              const unordered_map<idx_t, RelationId> &table_to_relation,
                                              unordered_set<idx_t> &out) {
	// Delegate the walk to the applicability gate's shared helper, then
	// translate each raw `table_index` to its OT/OJT `RelationId`. Skips
	// table_indexes that lie outside the join skeleton.
	unordered_set<idx_t> tables;
	OuterYanApplicability::CollectColumnTables(expr, tables);
	for (auto ti : tables) {
		auto it = table_to_relation.find(ti);
		if (it != table_to_relation.end()) {
			out.insert(it->second);
		}
	}
}

optional<idx_t> OuterYanTree::LookupSingleRelation(const Expression &expr,
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

// TODO: Check
bool OuterYanTree::CheckPKFK(LogicalOperator &op) {
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

class OuterYanTree::OTBuilder {
public:
	OTBuilder(LogicalOperator &plan_p, OuterYanTree &tree_p)
	    : plan_root(plan_p), tree(tree_p) {
	}

	unique_ptr<OperatorTree> Build() {
		auto root = BuildNode(plan_root, /*depth*/ 0);
		AssignJoinOrders();
		SetJoinTwoSides();
		// `tree.table_to_relation` is fully populated only after every RELATION
		// node has been built; resolve aggregate column bindings into relation
		// ids now.
		ResolveAggregationRelations();

		auto ot = make_uniq<OperatorTree>();
		ot->root = std::move(root);
		// Populate `OTNode::subtree_relations` and canonicalise every JOIN so
		// that `conditions[0]` LHS / RHS line up with `children[0]` / `children[1]`.
		// Downstream passes (Simplification, Desimplification, OTToOJT, etc.)
		// assume this canonical form.
		ot->Finalize();
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
		case LogicalOperatorType::LOGICAL_PROJECTION:
			return BuildThroughProjection(op, depth);
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			return BuildThroughAggregate(op, depth);
		case LogicalOperatorType::LOGICAL_DISTINCT:
			return BuildThroughDistinct(op, depth);
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
		// Cache PK/FK once; later passes never re-walk `origin`.
		node->is_pk = CheckPKFK(op);
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

		auto info = make_uniq<OTJoin>();
		info->join_kind = join.join_type;
		info->original_join_kind = join.join_type;
		info->conditions.reserve(join.conditions.size());
		for (const auto &cond : join.conditions) {
			info->conditions.push_back(cond.Copy());
		}
		node->info = std::move(info);

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

	//! Peel a root LogicalProjection. The projection's output schema is not
	//! preserved across OuterYan (the rebuilt plan handles column lifetime via
	//! downstream passes), so no metadata is captured here.
	unique_ptr<OTNode> BuildThroughProjection(LogicalOperator &op, idx_t depth) {
		if (op.children.size() != 1) {
			throw InternalException("LogicalProjection with %llu children",
			                        op.children.size());
		}
		return BuildNode(*op.children[0], depth);
	}

	//! Peel a root LogicalAggregate, classify it into `tree.root_aggregation`,
	//! and descend into the join skeleton beneath. The applicability gate
	//! already rejects aggregates that appear inside the join skeleton, so
	//! this case fires only on the root chain.
	unique_ptr<OTNode> BuildThroughAggregate(LogicalOperator &op, idx_t depth) {
		if (op.children.size() != 1) {
			throw InternalException("LogicalAggregate with %llu children",
			                        op.children.size());
		}
		auto &agg = op.Cast<LogicalAggregate>();
		ClassifyRootAggregate(agg);
		return BuildNode(*op.children[0], depth);
	}

	//! Peel a root LogicalDistinct, classify it as `SELECT_DISTINCT`, and
	//! record each child binding as a distinct target in
	//! `tree.root_aggregation.columns`. Mirrors the reference's
	//! `StoreDistinctAggregates` (`aggregation_pushdown.cpp:272`).
	unique_ptr<OTNode> BuildThroughDistinct(LogicalOperator &op, idx_t depth) {
		if (op.children.size() != 1) {
			throw InternalException("LogicalDistinct with %llu children",
			                        op.children.size());
		}
		auto &distinct = op.Cast<LogicalDistinct>();
		auto &info = tree.root_aggregation;
		// Outermost classifier wins — defensive against nested distincts.
		if (info.type == OuterYanAggregationType::NONE) {
			info.type = OuterYanAggregationType::SELECT_DISTINCT;
			info.has_group_by = false;
			// Record each child binding as a "distinct" target so the
			// rewriter knows which columns the root distinct cares about.
			auto child_bindings = distinct.children[0]->GetColumnBindings();
			distinct.children[0]->ResolveOperatorTypes();
			for (idx_t i = 0; i < child_bindings.size(); i++) {
				OuterYanAggregateColumn col;
				col.function_name = "distinct";
				col.binding = child_bindings[i];
				info.columns.push_back(std::move(col));
			}
		}
		return BuildNode(*op.children[0], depth);
	}

	//! Record the root aggregate's type tag and per-expression column info on
	//! `tree.root_aggregation`. Classification is by aggregate function name
	//! only -- mirrors `DuckDBYanPlus::DetectQueryType` (optimizer.cpp:533) and
	//! `aggregation_pushdown.cpp:102`.
	void ClassifyRootAggregate(LogicalAggregate &agg) {
		auto &info = tree.root_aggregation;
		// Outermost aggregate wins; binding layers never emit nested
		// LogicalAggregate so this is defensive only.
		if (info.type != OuterYanAggregationType::NONE) {
			return;
		}
		info.agg_op = &agg;
		info.has_group_by = !agg.groups.empty();

		// Capture GROUP BY column bindings. These columns anchor the OJT root:
		// `ResolveAggregationRelations` feeds them into `tree.root_relations`,
		// which OuterYanDP consults to force the corresponding relations to the
		// top of the chosen tree. Non-column GROUP BY expressions (e.g.
		// `GROUP BY a + b`) are skipped here -- handling them would require a
		// full expression walk; prototype scope leaves it as future work.
		for (auto &group_expr : agg.groups) {
			if (!group_expr ||
			    group_expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			OuterYanAggregateColumn col;
			col.function_name = "group_by";
			col.binding = group_expr->Cast<BoundColumnRefExpression>().binding;
			info.columns.push_back(std::move(col));
		}

		bool any_count_star = false;
		bool any_minmax = false;
		bool any_sum = false;
		bool any_other = false;

		for (auto &expr : agg.expressions) {
			if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				any_other = true;
				continue;
			}
			auto &bag = expr->Cast<BoundAggregateExpression>();
			OuterYanAggregateColumn col;
			col.function_name = bag.function.name;
			if (!bag.children.empty() && bag.children[0] &&
			    bag.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				col.binding = bag.children[0]->Cast<BoundColumnRefExpression>().binding;
			}
			info.columns.push_back(std::move(col));

			const auto &name = bag.function.name;
			if (name == "count_star") {
				if (agg.groups.empty()) {
					any_count_star = true;
				} else {
					// count_star + GROUP BY decomposes to a SUM-shape during
					// aggregation pushdown (see reference reclassification).
					any_sum = true;
				}
			} else if (name == "count") {
				if (!agg.groups.empty()) {
					any_sum = true;
				} else {
					any_other = true;
				}
			} else if (name == "min" || name == "max") {
				any_minmax = true;
			} else if (name == "sum") {
				any_sum = true;
			} else {
				any_other = true;
			}
		}

		if (any_other) {
			info.type = OuterYanAggregationType::OTHER;
		} else if (any_minmax && !any_sum && !any_count_star) {
			info.type = OuterYanAggregationType::MINMAX;
		} else if (any_sum && !any_minmax && !any_count_star) {
			info.type = OuterYanAggregationType::SUM;
		} else if (any_count_star && !any_minmax && !any_sum) {
			info.type = OuterYanAggregationType::COUNT_STAR;
		} else {
			info.type = OuterYanAggregationType::OTHER;
		}
	}

	//! Map each recorded aggregate column's `binding.table_index` back to its
	//! OT relation id. Runs once after every RELATION node has been built so
	//! `tree.table_to_relation` is complete. Unresolved entries (e.g.,
	//! count_star with no column, or a column whose table_index sits outside
	//! the skeleton) are left with an empty `relation`.
	void ResolveAggregationRelations() {
		for (auto &col : tree.root_aggregation.columns) {
			if (col.binding.table_index.index == DConstants::INVALID_INDEX) {
				continue;
			}
			auto it = tree.table_to_relation.find(col.binding.table_index.index);
			if (it == tree.table_to_relation.end()) {
				continue;
			}
			col.relation = it->second;
			// Only GROUP BY / SELECT DISTINCT bindings anchor the OJT root --
			// raw aggregate-function arguments (e.g. min/max/sum input columns)
			// do not impose a position constraint on their relation.
			if (col.function_name == "group_by" || col.function_name == "distinct") {
				tree.root_relations.insert(it->second);
			}
		}
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
			D_ASSERT(node->info);
			auto &info = *node->info;
			if (info.conditions.empty()) {
				throw NotImplementedException(
				    "LogicalPlanToOT: cross product (join with no conditions)");
			}
			auto &cond = info.conditions[0];
			auto left = LookupSingleRelation(cond.GetLHS(), tree.table_to_relation);
			auto right = LookupSingleRelation(cond.GetRHS(), tree.table_to_relation);
			if (!left || !right) {
				throw NotImplementedException(
				    "LogicalPlanToOT: could not resolve both sides of a join condition "
				    "to a single OT base relation");
			}
			info.cond_left_relation_id = *left;
			info.cond_right_relation_id = *right;
		}
	}
};

void OuterYanTree::BuildOT(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		throw InternalException("OuterYanTree::BuildOT: null plan");
	}
	OTBuilder builder(*plan, *this);
	ot = builder.Build();
	source_plan = std::move(plan);
}

// ============================================================================
// BuildOJT / ConstructOJT
// ============================================================================

//! Shared OJT-assembly body. Reads `ot_relations_by_id` and
//! `ot_joins_by_order` (both populated by `BuildOT` and stable across
//! Simplification / Desimplification join-kind mutations), returns a fresh
//! OJT without persisting it. `BuildOJT` and `ConstructOJT` are thin
//! wrappers that choose where to store the result.
unique_ptr<OrderedJoinTree> OuterYanTree::BuildOJTImpl() {
	if (!HasOT()) {
		throw InternalException("OuterYanTree::BuildOJTImpl: ot is null");
	}

	const auto &ot_relations = ot_relations_by_id;
	const auto &ot_joins = ot_joins_by_order;

	if (ot_relations.empty()) {
		throw NotImplementedException("OuterYanTree::BuildOJTImpl: OT has no base relations");
	}
	for (idx_t r = 0; r < ot_relations.size(); r++) {
		if (!ot_relations[r]) {
			throw InternalException(
			    "OuterYanTree::BuildOJTImpl: missing OT relation for relation_id %llu", r);
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
			    "OuterYanTree::BuildOJTImpl: %llu relations with 0 joins (disconnected OT)",
			    nodes_by_id.size());
		}
		return make_uniq<OrderedJoinTree>(std::move(nodes_by_id[0]));
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
		if (!ot_join->info) {
			throw InternalException(
			    "OuterYanTree::BuildOJTImpl: OT JOIN node has null info");
		}
		auto &ot_info = *ot_join->info;
		idx_t left_rel = ot_info.cond_left_relation_id;
		idx_t right_rel = ot_info.cond_right_relation_id;

		idx_t parent_id;
		idx_t child_id;
		if (step == 0) {
			bool left_extends = false;
			bool right_extends = false;
			if (n_joins >= 2) {
				auto *next_join = ot_joins[idx - 1];
				D_ASSERT(next_join->info);
				idx_t nl = next_join->info->cond_left_relation_id;
				idx_t nr = next_join->info->cond_right_relation_id;
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
				    "OuterYanTree::BuildOJTImpl: cyclic join graph (both endpoints already in OJT)");
			}
			if (!l_in && !r_in) {
				throw InternalException(
				    "OuterYanTree::BuildOJTImpl: top-down step found neither endpoint in the partial "
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

		// PK/FK is a per-relation property cached at BuildOT time.
		bool left_pk = ot_relations[left_rel]->is_pk;
		bool right_pk = ot_relations[right_rel]->is_pk;
		bool child_pk = (child_id == left_rel) ? left_pk : right_pk;

		bool parent_is_left = (parent_id == left_rel);

		OJTEdge edge;
		edge.kind = CheckOuterYanJoinFlip(ot_info.join_kind, parent_is_left);
		edge.original_kind =
		    CheckOuterYanJoinFlip(ot_info.original_join_kind, parent_is_left);
		edge.order = ot_join->order;
		edge.parent_relation_id = parent_id;
		// Move owned join metadata into the OJT edge; OT JOIN's `info` is null
		// from here on and OT is not consulted after BuildOJT.
		edge.info = std::move(ot_join->info);
		edge.has_pk_fk_constraint = left_pk || right_pk;
		edge.child_is_pk = child_pk;
		edge.child = std::move(nodes_by_id[child_id]);

		node_pointer[parent_id]->children.push_back(std::move(edge));
	}

	if (ojt_root_id == DConstants::INVALID_INDEX) {
		throw InternalException("OuterYanTree::BuildOJTImpl: OJT root not assigned");
	}
	return make_uniq<OrderedJoinTree>(std::move(nodes_by_id[ojt_root_id]));
}

void OuterYanTree::BuildOJT() {
	ojt = BuildOJTImpl();
}

unique_ptr<OrderedJoinTree> OuterYanTree::ConstructOJT() {
	return BuildOJTImpl();
}

// ============================================================================
// OJTToLogicalPlan
// ============================================================================

//! Flatten the OJT into a vector of edge pointers ordered by post-order
//! traversal; sorted later by `OJTEdge::order` so OJTToLogicalPlan emits
//! deepest joins first.
void OuterYanTree::CollectOJTInfo(OJTNode &node, vector<OJTNode *> &nodes_by_rel,
                                  vector<OJTEdge *> &edges) {
	if (node.relation_id >= nodes_by_rel.size()) {
		nodes_by_rel.resize(node.relation_id + 1, nullptr);
	}
	nodes_by_rel[node.relation_id] = &node;
	for (auto &edge : node.children) {
		if (!edge.child) {
			throw InternalException("OJTToLogicalPlan: edge has no child");
		}
		if (!edge.info) {
			throw InternalException("OJTToLogicalPlan: edge has no OTJoin info");
		}
		edges.push_back(&edge);
		CollectOJTInfo(*edge.child, nodes_by_rel, edges);
	}
}

void OuterYanTree::PushFiltersAboveLeaf(unique_ptr<LogicalOperator> &subplan,
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

void OuterYanTree::TryPushFilter(unique_ptr<LogicalOperator> &subplan, unique_ptr<Expression> expr) {
	if (subplan->type == LogicalOperatorType::LOGICAL_FILTER) {
		subplan->Cast<LogicalFilter>().expressions.push_back(std::move(expr));
		return;
	}
	auto wrap = make_uniq<LogicalFilter>();
	wrap->expressions.push_back(std::move(expr));
	wrap->children.push_back(std::move(subplan));
	subplan = std::move(wrap);
}

unique_ptr<LogicalOperator> OuterYanTree::BuildJoinFromEdge(const OJTEdge &edge,
                                                            const JoinRelationSet &parent_set,
                                                            unique_ptr<LogicalOperator> parent_plan,
                                                            unique_ptr<LogicalOperator> child_plan) {
	D_ASSERT(edge.info);
	const auto &info = *edge.info;

	// Orientation: conditions[i].left is bound to info.cond_left_relation_id.
	// If the parent subtree contains that relation, the conditions already
	// align (parent on left). Otherwise swap each condition.
	bool parent_contains_lhs = false;
	for (idx_t i = 0; i < parent_set.count; i++) {
		if (parent_set.relations[i].index == info.cond_left_relation_id) {
			parent_contains_lhs = true;
			break;
		}
	}

	auto new_join = make_uniq<LogicalComparisonJoin>(edge.kind);
	new_join->conditions.reserve(info.conditions.size());
	for (const auto &cond : info.conditions) {
		JoinCondition copy = cond.Copy();
		if (!parent_contains_lhs) {
			copy.Swap();
		}
		new_join->conditions.push_back(std::move(copy));
	}
	new_join->children.push_back(std::move(parent_plan));
	new_join->children.push_back(std::move(child_plan));
	new_join->ResolveOperatorTypes();
	return new_join;
}

unique_ptr<LogicalOperator> OuterYanTree::OJTToLogicalPlan(ClientContext &context) {
	if (!ojt) {
		throw InternalException("OuterYanTree::OJTToLogicalPlan: ojt is null");
	}
	auto &ojt_ref = *ojt;

	vector<OJTNode *> nodes_by_rel;
	vector<OJTEdge *> edges;
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
	          [](const OJTEdge *a, const OJTEdge *b) { return a->order < b->order; });

	for (auto *e : edges) {
		auto &parent_set = current_set_by_rel[e->parent_relation_id].get();
		auto &child_set = current_set_by_rel[e->child->relation_id].get();
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

		auto new_join = BuildJoinFromEdge(*e, parent_set, std::move(parent_plan), std::move(child_plan));
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
