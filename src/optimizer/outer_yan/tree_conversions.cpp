#include "duckdb/optimizer/outer_yan/tree_conversions.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
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
// Small helpers (file-local)
// ============================================================================

namespace {

OJTEdgeKind JoinTypeToOJTEdgeKind(JoinType type) {
	switch (type) {
	case JoinType::INNER:
		return OJTEdgeKind::INNER;
	case JoinType::LEFT:
		return OJTEdgeKind::LEFT_OUTER;
	case JoinType::RIGHT:
		return OJTEdgeKind::RIGHT_OUTER;
	case JoinType::OUTER:
		return OJTEdgeKind::FULL_OUTER;
	default:
		throw NotImplementedException("OuterYan OJT does not support JoinType %s",
		                              EnumUtil::ToString(type));
	}
}

JoinType OJTEdgeKindToJoinType(OJTEdgeKind kind) {
	switch (kind) {
	case OJTEdgeKind::INNER:
		return JoinType::INNER;
	case OJTEdgeKind::LEFT_OUTER:
		return JoinType::LEFT;
	case OJTEdgeKind::RIGHT_OUTER:
		return JoinType::RIGHT;
	case OJTEdgeKind::FULL_OUTER:
		return JoinType::OUTER;
	}
	throw InternalException("OJTEdgeKindToJoinType: unknown kind");
}

//! A "leaf" base-relation subtree from the OJT's perspective: a
//! LogicalGet, possibly under a chain of single-child LogicalFilter /
//! LogicalProjection. Anything else (joins, aggregates, set-ops) forces
//! structural recursion.
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

bool LogicalSubtreeHasFilters(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		return true;
	}
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (!get.table_filters.filters.empty()) {
			return true;
		}
	}
	for (auto &child : op.children) {
		if (child && LogicalSubtreeHasFilters(*child)) {
			return true;
		}
	}
	return false;
}

//! PK-FK check on one side of a join, adapted from
//! DuckDBYanPlus AggregationPushdown::CheckPKFK
//! (src/optimizer/aggregation_pushdown.cpp ~L547).
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
// OJTBuilder — three-phase build (Collect → Assemble → Finalise)
// ============================================================================
//
//   Phase 1 (Collect)   walks the original plan with raw pointers (no
//                       moves on operators), populating:
//                         - `nodes_by_id`        one OJTNode per base relation
//                         - `table_to_relation`  table_index → OJT relation_id
//                         - `joins_with_depth`   each join paired with its
//                                                depth from the root join
//                                                (root depth = 0; deeper joins
//                                                 have larger depth values).
//                                                Sorted bottom-up-by-level
//                                                into `joins_in_order` after
//                                                Collect: deepest level first
//                                                (`order = 1, 2, ...`); joins
//                                                at the same depth are
//                                                contiguous; every join's
//                                                order is strictly less than
//                                                its parent join's order.
//                         - `filter_records`     residual filter expressions
//                                                moved out of LogicalFilters
//                                                that sit above the join tree
//
//   Phase 2 (Assemble)  walks `joins_in_order` in order; for each join,
//                       resolves its two endpoint relations from the join
//                       condition and attaches the OJTEdge so that the OJT
//                       remains a single connected tree:
//                         - if neither endpoint is in the OJT yet: pick the
//                           higher-degree relation as parent (tie → left)
//                         - if exactly one endpoint is in the OJT: that side
//                           is the parent, the other becomes the new child
//                         - if both already in the OJT: cyclic join graph
//                           (unsupported)
//                       Edge kind is mapped 1:1 from the original JoinType;
//                       no flip is applied when orientation reverses the
//                       original plan's left/right.
//
//   Phase 3 (Finalise)  wraps the assembled root and the moved-in source
//                       plan into an OrderedJoinTree.
//
// Notes
//   - Original-plan operators (joins, base relations) are NOT moved during
//     build. The OJT references them via raw pointers; the unique_ptr
//     ownership chain inside the source plan stays intact.
//   - Filter expressions ARE moved out of source LogicalFilters (the empty
//     LogicalFilter wrappers ride along inside the source plan and get
//     destroyed when `OrderedJoinTree::source_plan` dies).

namespace {

class OJTBuilder {
public:
	explicit OJTBuilder(LogicalOperator &plan) : plan_root(plan) {
	}

	unique_ptr<OrderedJoinTree> Build() {
		Collect(plan_root, 0);
		SortJoinsBFSReverse();
		auto root = Assemble();
		auto ojt = make_uniq<OrderedJoinTree>(std::move(root));
		ojt->filter_records = std::move(filter_records);
		return ojt;
	}

private:
	LogicalOperator &plan_root;

	struct JoinAtDepth {
		LogicalComparisonJoin *join;
		idx_t depth; //!< distance from the root join (root depth = 0).
	};

	//! One OJTNode per base relation; vector index == OJT relation_id.
	vector<unique_ptr<OJTNode>> nodes_by_id;
	//! `LogicalGet::table_index` → OJT relation_id, used to attribute join
	//! conditions and residual filter expressions to base relations.
	unordered_map<idx_t, idx_t> table_to_relation;
	//! Every join encountered in Collect, paired with its depth from the
	//! root join. Filled by Collect in DFS top-down order; consumed by
	//! `SortJoinsBFSReverse` to produce `joins_in_order`.
	vector<JoinAtDepth> joins_with_depth;
	//! Joins in BFS-by-level reverse order: deepest level first, joins at
	//! the same depth contiguous, every parent join after both its
	//! children. `joins_in_order[i]` becomes the OJT edge with
	//! `order = i + 1`.
	vector<LogicalComparisonJoin *> joins_in_order;
	//! Residual filter records collected from LogicalFilters above the
	//! join tree (LogicalFilters directly above a base relation stay inside
	//! the base subtree and are not touched).
	vector<unique_ptr<OJTFilterRecord>> filter_records;

	// -------------------------------------------------------------------
	// Phase 1: Collect
	// -------------------------------------------------------------------

	void Collect(LogicalOperator &op, idx_t depth) {
		if (IsBaseRelationSubtree(op)) {
			CollectBaseRelation(op);
			return;
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			CollectJoin(op, depth);
			return;
		case LogicalOperatorType::LOGICAL_FILTER:
			CollectFilterAboveOp(op, depth);
			return;
		default:
			throw NotImplementedException("LogicalPlanToOJT: unsupported operator %s",
			                              EnumUtil::ToString(op.type));
		}
	}

	void CollectBaseRelation(LogicalOperator &op) {
		idx_t rid = nodes_by_id.size();
		auto node = make_uniq<OJTNode>();
		node->relation_id = rid;
		node->base_op = &op;
		node->has_filters = LogicalSubtreeHasFilters(op);
		RegisterTableIndices(op, rid);
		nodes_by_id.push_back(std::move(node));
	}

	void CollectJoin(LogicalOperator &op, idx_t depth) {
		if (op.children.size() != 2) {
			throw NotImplementedException(
			    "LogicalPlanToOJT: LogicalComparisonJoin with %llu children (expected 2)",
			    op.children.size());
		}
		// Top-down DFS visit; depth recorded so `SortJoinsBFSReverse` can
		// re-emit BFS-by-level reverse order.
		joins_with_depth.push_back({&op.Cast<LogicalComparisonJoin>(), depth});
		Collect(*op.children[0], depth + 1);
		Collect(*op.children[1], depth + 1);
	}

	void CollectFilterAboveOp(LogicalOperator &op, idx_t depth) {
		if (op.children.size() != 1) {
			throw InternalException("LogicalFilter with %llu children", op.children.size());
		}
		// LogicalFilter does not introduce a new join level — pass depth through.
		Collect(*op.children[0], depth);
		auto &filter = op.Cast<LogicalFilter>();
		// Move filter expressions out into OJTFilterRecords. The empty
		// LogicalFilter wrapper stays in the source plan (harmless).
		for (auto &expr : filter.expressions) {
			unordered_set<idx_t> refs;
			CollectReferencedRelations(*expr, refs);
			auto rec = make_uniq<OJTFilterRecord>(std::move(expr), std::move(refs),
			                                     filter_records.size());
			rec->from_residual_predicate = true;
			filter_records.push_back(std::move(rec));
		}
		filter.expressions.clear();
	}

	void RegisterTableIndices(const LogicalOperator &op, idx_t relation_id) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			table_to_relation[get.table_index.index] = relation_id;
		}
		for (auto &child : op.children) {
			if (child) {
				RegisterTableIndices(*child, relation_id);
			}
		}
	}

	void CollectReferencedRelations(const Expression &expr, unordered_set<idx_t> &out) const {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &cref = expr.Cast<BoundColumnRefExpression>();
			auto it = table_to_relation.find(cref.binding.table_index.index);
			if (it != table_to_relation.end()) {
				out.insert(it->second);
			}
		}
		ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
			CollectReferencedRelations(child, out);
		});
	}

	// -------------------------------------------------------------------
	// Phase 1.5: SortJoinsBFSReverse
	// -------------------------------------------------------------------
	//
	// Re-emit `joins_with_depth` into `joins_in_order` as deepest-level-first
	// BFS order:
	//   - greater `depth` ⇒ smaller `order` (deepest gets `order = 1`)
	//   - within a depth, the original DFS visit order is preserved by
	//     `stable_sort`, so left-subtree joins at depth D come before
	//     right-subtree joins at the same depth.
	// Result: every parent join's order is strictly larger than both of
	// its children's orders, and same-depth joins occupy a contiguous run.

	void SortJoinsBFSReverse() {
		std::stable_sort(joins_with_depth.begin(), joins_with_depth.end(),
		                 [](const JoinAtDepth &a, const JoinAtDepth &b) {
			                 return a.depth > b.depth;
		                 });
		joins_in_order.reserve(joins_with_depth.size());
		for (auto &entry : joins_with_depth) {
			joins_in_order.push_back(entry.join);
		}
	}

	// -------------------------------------------------------------------
	// Phase 2: Assemble
	// -------------------------------------------------------------------

	unique_ptr<OJTNode> Assemble() {
		if (nodes_by_id.empty()) {
			throw NotImplementedException("LogicalPlanToOJT: no base relations found");
		}
		if (joins_in_order.empty()) {
			// Single relation, no joins.
			if (nodes_by_id.size() != 1) {
				throw InternalException("OJTBuilder: %llu relations with 0 joins", nodes_by_id.size());
			}
			return std::move(nodes_by_id[0]);
		}

		// Degree of each relation across all joins; used to break ties when
		// the very first join has both endpoints unattached.
		vector<idx_t> degree(nodes_by_id.size(), 0);
		for (auto *j : joins_in_order) {
			auto endpoints = ResolveJoinEndpoints(*j);
			degree[endpoints.first]++;
			degree[endpoints.second]++;
		}

		// Track which relations are independent OJT subtree roots and how
		// to reach each relation's OJTNode (which may now live nested inside
		// another node's children chain).
		vector<OJTNode *> node_pointer(nodes_by_id.size(), nullptr);
		for (idx_t i = 0; i < nodes_by_id.size(); i++) {
			node_pointer[i] = nodes_by_id[i].get();
		}
		unordered_set<idx_t> roots;
		for (idx_t i = 0; i < nodes_by_id.size(); i++) {
			roots.insert(i);
		}

		for (idx_t i = 0; i < joins_in_order.size(); i++) {
			auto *join = joins_in_order[i];
			auto endpoints = ResolveJoinEndpoints(*join);
			idx_t left_rel = endpoints.first;   // relation in join->children[0]
			idx_t right_rel = endpoints.second; // relation in join->children[1]

			bool l_root = roots.count(left_rel) > 0;
			bool r_root = roots.count(right_rel) > 0;
			idx_t parent_id;
			idx_t child_id;
			if (l_root && r_root) {
				if (degree[right_rel] > degree[left_rel]) {
					parent_id = right_rel;
					child_id = left_rel;
				} else {
					parent_id = left_rel;
					child_id = right_rel;
				}
			} else if (l_root && !r_root) {
				parent_id = right_rel;
				child_id = left_rel;
			} else if (!l_root && r_root) {
				parent_id = left_rel;
				child_id = right_rel;
			} else {
				throw NotImplementedException(
				    "LogicalPlanToOJT: cyclic join graph (both endpoints already in OJT) "
				    "is not supported");
			}

			// PK-FK on each original side.
			bool left_pk = CheckPKFK(*join->children[0]);
			bool right_pk = CheckPKFK(*join->children[1]);
			bool child_pk = (child_id == left_rel) ? left_pk : right_pk;

			OJTEdge edge;
			edge.kind = JoinTypeToOJTEdgeKind(join->join_type);
			edge.original_kind = edge.kind;
			edge.order = i + 1;
			edge.join_op = join;
			edge.has_pk_fk_constraint = left_pk || right_pk;
			edge.child_is_pk = child_pk;
			edge.child = std::move(nodes_by_id[child_id]);

			roots.erase(child_id);
			node_pointer[parent_id]->children.push_back(std::move(edge));
		}

		if (roots.size() != 1) {
			throw InternalException("OJTBuilder: %llu remaining roots after assembly (expected 1)",
			                        roots.size());
		}
		return std::move(nodes_by_id[*roots.begin()]);
	}

	pair<idx_t, idx_t> ResolveJoinEndpoints(LogicalComparisonJoin &join) const {
		if (join.conditions.empty()) {
			throw NotImplementedException("LogicalPlanToOJT: cross product (join with no conditions)");
		}
		auto &cond = join.conditions[0];
		auto left_rel = LookupRelation(cond.GetLHS());
		auto right_rel = LookupRelation(cond.GetRHS());
		if (!left_rel || !right_rel) {
			throw NotImplementedException(
			    "LogicalPlanToOJT: could not resolve both endpoints of a join condition "
			    "to OJT base relations");
		}
		return {*left_rel, *right_rel};
	}

	optional<idx_t> LookupRelation(const Expression &expr) const {
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
				result = LookupRelation(child);
			}
		});
		return result;
	}
};

// ============================================================================
// Lowering — OJT → LogicalPlan
// ============================================================================

//! Map every operator in `op`'s subtree to the unique_ptr slot that owns
//! it inside its parent. Used by `Detach` to pluck operators out of the
//! source plan as we rebuild.
void BuildParentSlotMap(LogicalOperator &op,
                        unordered_map<LogicalOperator *, unique_ptr<LogicalOperator> *> &out) {
	for (auto &child : op.children) {
		if (child) {
			out[child.get()] = &child;
			BuildParentSlotMap(*child, out);
		}
	}
}

unique_ptr<LogicalOperator> Detach(LogicalOperator *op,
                                   unordered_map<LogicalOperator *, unique_ptr<LogicalOperator> *> &slots) {
	auto it = slots.find(op);
	if (it == slots.end()) {
		throw InternalException("OJTToLogicalPlan: operator not found in source plan");
	}
	auto *slot = it->second;
	auto moved = std::move(*slot);
	if (!moved) {
		throw InternalException("OJTToLogicalPlan: operator already detached");
	}
	return moved;
}

unique_ptr<LogicalOperator>
EmitNode(OJTNode &node, unordered_map<LogicalOperator *, unique_ptr<LogicalOperator> *> &slots) {
	if (!node.base_op) {
		throw InternalException("OJTToLogicalPlan: OJTNode has no base_op");
	}
	auto plan = Detach(node.base_op, slots);
	if (!node.filters.empty()) {
		auto filter_op = make_uniq<LogicalFilter>();
		filter_op->expressions = std::move(node.filters);
		filter_op->children.push_back(std::move(plan));
		plan = std::move(filter_op);
	}
	for (auto &edge : node.children) {
		if (!edge.join_op) {
			throw InternalException("OJTToLogicalPlan: edge has no join_op");
		}
		auto child_plan = EmitNode(*edge.child, slots);
		auto join_uptr = Detach(edge.join_op, slots);
		// The detached join's children are still its original ones (raw
		// pointers in the OJT pointed to the parent slot; the parent slot
		// has been moved, but the join's own children unique_ptrs may
		// already have been moved-out by recursive calls). Reset and
		// reattach with the realised parent / child plans.
		join_uptr->children.clear();
		join_uptr->children.push_back(std::move(plan));
		join_uptr->children.push_back(std::move(child_plan));
		// Sync join_type with the (possibly transformed) edge kind.
		join_uptr->Cast<LogicalComparisonJoin>().join_type = OJTEdgeKindToJoinType(edge.kind);
		plan = std::move(join_uptr);
		// SemijoinInsertion later wraps SJBuild / SJProbe here based on
		// edge.bottom_up / edge.top_down. Deferred to that module.
	}
	return plan;
}

} // namespace

// ============================================================================
// Public entry points
// ============================================================================

unique_ptr<OrderedJoinTree> LogicalPlanToOJT(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		throw InternalException("LogicalPlanToOJT: null plan");
	}
	OJTBuilder builder(*plan);
	auto ojt = builder.Build();
	// OJT must own the source plan so the raw pointers in OJTEdge::join_op
	// and OJTNode::base_op stay valid for the OJT's lifetime.
	ojt->source_plan = std::move(plan);
	return ojt;
}

unique_ptr<LogicalOperator> OJTToLogicalPlan(unique_ptr<OrderedJoinTree> ojt) {
	if (!ojt) {
		throw InternalException("OJTToLogicalPlan: null OJT");
	}
	if (!ojt->source_plan) {
		throw InternalException("OJTToLogicalPlan: OJT has no source plan");
	}

	// Snapshot every operator's owning unique_ptr slot inside the source
	// plan, then detach as the OJT prescribes.
	unordered_map<LogicalOperator *, unique_ptr<LogicalOperator> *> slots;
	slots[ojt->source_plan.get()] = &ojt->source_plan;
	BuildParentSlotMap(*ojt->source_plan, slots);

	auto plan = EmitNode(ojt->Root(), slots);

	// Apply leftover residual filter records at the top of the rebuilt plan.
	// Finer placement (lowest covering subtree per record) is left for a
	// later pass; correctness is preserved by stacking at the root.
	if (!ojt->filter_records.empty()) {
		auto filter_op = make_uniq<LogicalFilter>();
		for (auto &rec : ojt->filter_records) {
			if (rec && rec->filter) {
				filter_op->expressions.push_back(std::move(rec->filter));
			}
		}
		if (!filter_op->expressions.empty()) {
			filter_op->children.push_back(std::move(plan));
			plan = std::move(filter_op);
		}
	}

	// Drop the now-skeletal source plan; everything we needed has been
	// detached out of it.
	ojt->source_plan.reset();
	return plan;
}

} // namespace duckdb
