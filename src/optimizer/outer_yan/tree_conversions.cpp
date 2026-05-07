#include "duckdb/optimizer/outer_yan/tree_conversions.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
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

//! Flip an OJTEdgeKind across parent/child. Used when the OJT orients an
//! edge so that the OJT-parent corresponds to the original join's RHS
//! (children[1]), which reverses the side that LEFT/RIGHT designates as
//! preserved. INNER and FULL_OUTER are symmetric; LEFT_OUTER and
//! RIGHT_OUTER swap.
OJTEdgeKind FlipOJTEdgeKind(OJTEdgeKind kind) {
	switch (kind) {
	case OJTEdgeKind::INNER:
		return OJTEdgeKind::INNER;
	case OJTEdgeKind::LEFT_OUTER:
		return OJTEdgeKind::RIGHT_OUTER;
	case OJTEdgeKind::RIGHT_OUTER:
		return OJTEdgeKind::LEFT_OUTER;
	case OJTEdgeKind::FULL_OUTER:
		return OJTEdgeKind::FULL_OUTER;
	}
	throw InternalException("FlipOJTEdgeKind: unknown kind");
}

//! Map the original `JoinType` to an `OJTEdgeKind` oriented from
//! OJT-parent to OJT-child. `parent_is_left` must be true iff the
//! OJT-parent corresponds to the original join's `children[0]` (LHS); if
//! false, the kind is flipped so that LEFT_OUTER / RIGHT_OUTER continue
//! to label the OJT-parent's role rather than the original LHS's role.
//! This is the single source of truth for the OJT's edge-direction rule:
//! `edge.kind` is always read as "parent KIND child".
OJTEdgeKind OrientedJoinTypeToOJTEdgeKind(JoinType type, bool parent_is_left) {
	auto kind = JoinTypeToOJTEdgeKind(type);
	return parent_is_left ? kind : FlipOJTEdgeKind(kind);
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
//                       Edge kind is oriented as "parent KIND child" via
//                       `OrientedJoinTypeToOJTEdgeKind`: when the OJT-parent
//                       corresponds to the original join's RHS, LEFT_OUTER
//                       and RIGHT_OUTER are flipped so the kind keeps
//                       labelling the OJT-parent's role. INNER / FULL_OUTER
//                       are symmetric and unaffected.
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
		RegisterBaseTable(op, rid);
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
			if (refs.empty()) {
				// All BoundColumnRefs in this expression resolved to a
				// table_index not registered as an OJT base relation —
				// typically a LogicalProjection re-binding columns to a
				// fresh table_index above the join tree. Future work:
				// register projection table_indices (or push the filter
				// through the projection) so we can attribute the record.
				throw NotImplementedException(
				    "LogicalPlanToOJT: residual filter expression references no OJT base "
				    "relation (likely a projection-introduced table_index above the join "
				    "tree); not yet supported");
			}
			auto rec = make_uniq<OJTFilterRecord>(std::move(expr), std::move(refs),
			                                     filter_records.size());
			rec->from_residual_predicate = true;
			filter_records.push_back(std::move(rec));
		}
		filter.expressions.clear();
	}

	void RegisterBaseTable(const LogicalOperator &op, idx_t relation_id) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			table_to_relation[get.table_index.index] = relation_id;
		}
		for (auto &child : op.children) {
			if (child) {
				RegisterBaseTable(*child, relation_id);
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

		// node_pointer[i] always points to relation i's OJTNode, even after
		// it has been nested inside another node's children chain.
		vector<OJTNode *> node_pointer(nodes_by_id.size(), nullptr);
		for (idx_t i = 0; i < nodes_by_id.size(); i++) {
			node_pointer[i] = nodes_by_id[i].get();
		}

		// Top-down assembly: iterate joins from highest order (root join)
		// down to lowest (deepest-leaf join). Invariant: after each step
		// the partial OJT is a single connected tree.
		//
		//   Step 0 (root join):
		//     Both endpoints are unattached. Look one step further (the
		//     next-deeper join in iteration) and pick whichever endpoint
		//     also appears as an endpoint of that deeper join — that
		//     relation will keep extending the tree downward, so it
		//     becomes the OJT-child of the root edge; the other endpoint
		//     becomes the OJT root. If neither or both endpoints connect
		//     to the deeper join (e.g. it lives in a sibling subtree of
		//     a bushy plan), fall back to left = OJT root.
		//
		//   Step k > 0:
		//     Exactly one endpoint must already be in the partial OJT —
		//     that one is the parent in the new edge, the other becomes
		//     the new child node. Neither-in: connectivity invariant
		//     violated. Both-in: cyclic join graph (unsupported).
		unordered_set<idx_t> attached;
		idx_t ojt_root_id = DConstants::INVALID_INDEX;
		const idx_t n_joins = joins_in_order.size();

		for (idx_t step = 0; step < n_joins; step++) {
			idx_t idx = n_joins - 1 - step;
			auto *join = joins_in_order[idx];
			auto endpoints = ResolveJoinEndpoints(*join);
			idx_t left_rel = endpoints.first;   // relation in join->children[0]
			idx_t right_rel = endpoints.second; // relation in join->children[1]

			idx_t parent_id;
			idx_t child_id;
			if (step == 0) {
				bool left_extends = false;
				bool right_extends = false;
				if (n_joins >= 2) {
					auto next_endpoints = ResolveJoinEndpoints(*joins_in_order[idx - 1]);
					left_extends = (left_rel == next_endpoints.first ||
					                left_rel == next_endpoints.second);
					right_extends = (right_rel == next_endpoints.first ||
					                 right_rel == next_endpoints.second);
				}
				if (left_extends && !right_extends) {
					parent_id = right_rel;
					child_id = left_rel;
				} else if (!left_extends && right_extends) {
					parent_id = left_rel;
					child_id = right_rel;
				} else {
					// Lookahead inconclusive (no deeper join, or it lives
					// in a sibling subtree). Default: left = OJT root.
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
					    "LogicalPlanToOJT: cyclic join graph (both endpoints already in OJT) "
					    "is not supported");
				}
				if (!l_in && !r_in) {
					throw InternalException(
					    "OJTBuilder: top-down step found neither endpoint in the partial OJT "
					    "(connectivity invariant violated)");
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

			// PK-FK on each original side.
			bool left_pk = CheckPKFK(*join->children[0]);
			bool right_pk = CheckPKFK(*join->children[1]);
			bool child_pk = (child_id == left_rel) ? left_pk : right_pk;

			// `edge.kind` follows the OJT's directional convention
			// "parent KIND child". When the OJT orients this edge so that
			// the OJT-parent corresponds to the original join's RHS, the
			// kind must flip so LEFT/RIGHT keep labelling the OJT-parent's
			// side (not the original LHS's side).
			bool parent_is_left = (parent_id == left_rel);
			OJTEdge edge;
			edge.kind = OrientedJoinTypeToOJTEdgeKind(join->join_type, parent_is_left);
			edge.original_kind = edge.kind;
			edge.order = idx + 1; // original join order, not iteration index
			edge.join_op = join;
			edge.has_pk_fk_constraint = left_pk || right_pk;
			edge.child_is_pk = child_pk;
			edge.child = std::move(nodes_by_id[child_id]);

			node_pointer[parent_id]->children.push_back(std::move(edge));
		}

		if (ojt_root_id == DConstants::INVALID_INDEX) {
			throw InternalException("OJTBuilder: OJT root not assigned");
		}
		return std::move(nodes_by_id[ojt_root_id]);
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
//
// Driven entirely by the OJT (relations, edges, edge orders, edge kinds).
// `ojt->source_plan` is intentionally untouched: OJT passes
// (Simplification / Desimplification / DP / OuterYanPost) may have
// rearranged the tree after `LogicalPlanToOJT`, leaving `source_plan`
// only as a debug / printing aid for the original plan shape.
//
// Procedure (mirrors QueryGraphManager::Reconstruct):
//   1. Walk OJT once via `CollectOJTInfo`: collect per-relation
//      `OJTNode` pointers and a flat list of edges, with each edge's
//      parent/original-LHS orientation pre-computed by
//      `ParentIsOnOriginalLeft` (reads `edge.join_op->children[0]`,
//      which the OJT never detaches).
//   2. Seed `subplan_memo` with one leaf per OJT relation, keyed by a
//      singleton `JoinRelationSet`. Each leaf is `node->base_op->Copy(...)`
//      — a fresh deep copy, since the OJT does not own its base subtree
//      — followed by pushing `OJTNode::filters` directly above the leaf.
//      Residual records in `OrderedJoinTree::filter_records` whose
//      `referenced_relations` is exactly that leaf's singleton are also
//      consumed here (smallest covering set).
//   3. Sort edges by ascending `edge.order`. For each edge, look up the
//      two memo entries currently keyed by the sets covering its parent
//      and child relations, build a fresh `LogicalComparisonJoin` via
//      `BuildJoinFromEdge`, and re-key the merged plan under
//      `JoinRelationSetManager::Union(...)`. After each merge, residual
//      records whose referenced relations are all covered by the merged
//      set are pushed above the fused subplan via `TryPushFilter` — this
//      is the lowest-covering-subtree placement, mirroring the post-
//      recursion filter loop in `QueryGraphManager::GenerateJoins`. After
//      all edges, exactly one memo entry remains, holding the whole
//      rebuilt plan.
//   4. Root fallback: any residual record still unconsumed after steps 2
//      and 3 is stacked at the root via `TryPushFilter`. For a fully-
//      covered, connected OJT this is empty; the fallback handles records
//      whose covering set never matched a single fused subplan.

struct EdgeBuildInfo {
	idx_t parent_rel;
	idx_t child_rel;
	OJTEdgeKind kind;
	idx_t order;
	LogicalOperator *join_op;
	bool parent_was_left;
};

//! Collect all base-relation table_indices reachable under `op`.
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

//! Does the OJT-parent's base-relation subtree correspond to the original
//! join's children[0] (LHS)? Determines whether copied join conditions
//! need to be flipped before being placed in the new join.
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

//! DFS the OJT once: register each node by relation_id and collect every
//! edge with its pre-computed parent/original-LHS orientation.
void CollectOJTInfo(OJTNode &node,
                    vector<OJTNode *> &nodes_by_rel,
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

//! Insert a fresh `LogicalFilter` carrying `filters` directly above the
//! leaf of `subplan` (e.g. the `LogicalGet`). Pushes single-relation
//! predicates as close to the scan as possible.
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
	// Walk down the wrapper chain to the parent of the leaf.
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

//! Stack a single residual-filter expression above `subplan` as a
//! `LogicalFilter`, merging into an existing top-level `LogicalFilter`
//! rather than wrapping a fresh one — same shape-flattening contract as
//! `QueryGraphManager::PushFilter` in
//! src/optimizer/join_order/query_graph_manager.cpp.
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
	auto new_join = make_uniq<LogicalComparisonJoin>(OJTEdgeKindToJoinType(edge.kind));
	// Copy conditions; reorient via `JoinCondition::Swap` when the
	// OJT-parent corresponds to the original RHS subtree.
	for (const auto &cond : orig_join.conditions) {
		JoinCondition copy = cond.Copy();
		if (!edge.parent_was_left) {
			copy.Swap();
		}
		new_join->conditions.push_back(std::move(copy));
	}
	// children[0] = OJT-parent side, children[1] = OJT-child side.
	new_join->children.push_back(std::move(parent_plan));
	new_join->children.push_back(std::move(child_plan));
	new_join->ResolveOperatorTypes();
	// Other LogicalComparisonJoin fields (mark_types,
	// duplicate_eliminated_columns, delim_flipped, filter_pushdown) are
	// left at defaults — equi-/inequality joins from OuterYan's scope
	// don't use them. Revisit if mark/delim joins ever land in the OJT.
	return new_join;
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

unique_ptr<LogicalOperator> OJTToLogicalPlan(ClientContext &context,
                                             unique_ptr<OrderedJoinTree> ojt) {
	if (!ojt) {
		throw InternalException("OJTToLogicalPlan: null OJT");
	}

	// 1. Walk OJT once: enumerate per-relation nodes and edges. The OJT
	//    is the sole input — `ojt->source_plan` is intentionally
	//    untouched.
	vector<OJTNode *> nodes_by_rel;
	vector<EdgeBuildInfo> edges;
	CollectOJTInfo(ojt->Root(), nodes_by_rel, edges);
	const idx_t n_rels = nodes_by_rel.size();
	if (n_rels == 0) {
		throw InternalException("OJTToLogicalPlan: OJT has no relations");
	}

	// 2. Seed subplan_memo with one leaf per OJT relation, keyed by a
	//    singleton JoinRelationSet (mirrors QueryGraphManager). Records in
	//    `filter_records` whose `referenced_relations` is exactly the leaf's
	//    singleton are consumed here — that is the smallest covering set
	//    they can land on, so this matches the leaf-base-table case in
	//    `QueryGraphManager::GenerateJoins` (see query_graph_manager.cpp).
	JoinRelationSetManager set_manager;
	reference_map_t<JoinRelationSet, unique_ptr<LogicalOperator>> subplan_memo;
	vector<reference<JoinRelationSet>> current_set_by_rel;
	current_set_by_rel.reserve(n_rels);
	vector<bool> filter_records_status(ojt->filter_records.size(), false);

	for (idx_t r = 0; r < n_rels; r++) {
		auto *node = nodes_by_rel[r];
		if (!node || !node->base_op) {
			throw InternalException("OJTToLogicalPlan: missing OJTNode or base_op");
		}
		auto &singleton = set_manager.GetJoinRelation(RelationIndex(r));
		auto subplan = node->base_op->Copy(context);
		if (!subplan) {
			throw InternalException("OJTToLogicalPlan: base_op->Copy returned null");
		}
		if (!node->filters.empty()) {
			PushFiltersAboveLeaf(subplan, std::move(node->filters));
		}
		// Single-relation residual records bound to this leaf land here.
		for (idx_t i = 0; i < ojt->filter_records.size(); i++) {
			if (filter_records_status[i]) {
				continue;
			}
			auto &rec = ojt->filter_records[i];
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

	// 3. Fuse edges in ascending build order. Each step looks up the two
	//    subplans currently keyed by the relation sets covering the edge
	//    endpoints, builds a fresh LogicalComparisonJoin, and re-keys the
	//    merged plan under set_manager.Union(parent_set, child_set).
	std::sort(edges.begin(), edges.end(),
	          [](const EdgeBuildInfo &a, const EdgeBuildInfo &b) { return a.order < b.order; });

	for (auto &e : edges) {
		auto &parent_set = current_set_by_rel[e.parent_rel].get();
		auto &child_set = current_set_by_rel[e.child_rel].get();
		if (&parent_set == &child_set) {
			throw InternalException(
			    "OJTToLogicalPlan: edge endpoints already in the same group (cyclic OJT)");
		}

		auto parent_it = subplan_memo.find(parent_set);
		auto child_it = subplan_memo.find(child_set);
		if (parent_it == subplan_memo.end() || child_it == subplan_memo.end()) {
			throw InternalException(
			    "OJTToLogicalPlan: subplan_memo missing entry for edge endpoint");
		}
		auto parent_plan = std::move(parent_it->second);
		auto child_plan = std::move(child_it->second);
		subplan_memo.erase(parent_it);
		subplan_memo.erase(child_it);

		auto new_join = BuildJoinFromEdge(e, std::move(parent_plan), std::move(child_plan));
		auto &merged = set_manager.Union(parent_set, child_set);

		// JoinRelationSetManager keeps every issued set alive (append-only
		// internal tree), so `parent_set` / `child_set` references stay
		// valid even after Union returns the new merged set.
		for (idx_t i = 0; i < parent_set.count; i++) {
			current_set_by_rel[parent_set.relations[i].index] = merged;
		}
		for (idx_t i = 0; i < child_set.count; i++) {
			current_set_by_rel[child_set.relations[i].index] = merged;
		}
		auto memo_it = subplan_memo.emplace(merged, std::move(new_join)).first;
		// Lowest-covering-subtree pushdown: any unconsumed residual record
		// whose referenced relations all map to `merged` lands above this
		// fused subplan. Mirrors the post-recursion filter loop in
		// `QueryGraphManager::GenerateJoins`. Move-once is enforced by
		// `filter_records_status` plus the `rec->filter` null check.
		for (idx_t i = 0; i < ojt->filter_records.size(); i++) {
			if (filter_records_status[i]) {
				continue;
			}
			auto &rec = ojt->filter_records[i];
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
		// SemijoinInsertion later wraps SJBuild / SJProbe on this edge
		// using edge.bottom_up / edge.top_down; deferred to that module.
	}

	// 4. Exactly one entry should remain — the full-set plan.
	if (subplan_memo.size() != 1) {
		throw InternalException(
		    "OJTToLogicalPlan: relations remain in disjoint groups after assembly");
	}
	auto plan = std::move(subplan_memo.begin()->second);
	if (!plan) {
		throw InternalException("OJTToLogicalPlan: final memo entry is null");
	}

	// 5. Root fallback for any residual record not consumed in steps 2 or 3
	//    (e.g. record references a relation outside the OJT, or a future
	//    transformation leaves a record whose covering set never appears as
	//    a single fused subplan). Stack remaining records at the root via
	//    `TryPushFilter` — correct, just not as deep as the per-step
	//    pushdown.
	for (idx_t i = 0; i < ojt->filter_records.size(); i++) {
		if (filter_records_status[i]) {
			continue;
		}
		auto &rec = ojt->filter_records[i];
		if (!rec || !rec->filter) {
			continue;
		}
		TryPushFilter(plan, std::move(rec->filter));
		filter_records_status[i] = true;
	}

	return plan;
}

} // namespace duckdb
