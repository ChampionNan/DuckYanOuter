//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

//! Walks the OperatorTree and converts outer joins to inner joins where
//! a downstream null-rejecting predicate proves the null-padded rows would
//! be filtered anyway. More aggressive than OuterJoinSimplification because
//! it has whole-query visibility: predicates extracted by BuildOT into
//! `tree.filter_records` are still consulted via subtree-dominance checks.
class Simplification {
public:
	void Apply(OuterYanTree &tree);

private:
	//! Top-down walk over the OT. RELATION nodes return immediately;
	//! JOIN nodes harvest dominating filters, then dispatch on join_type.
	void VisitNode(OuterYanTree &tree, OTNode &node);

	//! Memoised set of relation_ids covered by `node`'s subtree, used by
	//! `DominatesJoin` to decide whether a filter sits at-or-above a join.
	const unordered_set<idx_t> &ComputeSubtreeRelations(OTNode &node);

	//! Bare BoundColumnRef → null-rejected binding. Mirrors
	//! OuterJoinSimplification::HandleExpression.
	// TODO: could unwrap casts or basic arithmetic
	void HandleExpression(const Expression &expr);

	//! Per-conjunct equivalent of OuterJoinSimplification's LOGICAL_FILTER
	//! arm; conjuncts are assumed pre-split (BuildOT pushes one
	//! `filter.expressions[i]` per record).
	void HandleFilterRecord(const Expression &expr);

	//! True iff `record`'s referenced relations are NOT entirely contained
	//! in either child subtree of `join_node` — i.e., the filter sits at-or-
	//! above this join in the lowered plan and may simplify it.
	bool DominatesJoin(const OuterYanFilterRecord &record, OTNode &join_node);

	//! Pull null-rejecting predicates from every filter_record that
	//! dominates `join_node` into `null_filtered_columns`.
	void HarvestDominatingFilters(OuterYanTree &tree, OTNode &join_node);

private:
	column_binding_set_t null_filtered_columns;
	unordered_map<const OTNode *, unordered_set<idx_t>> subtree_relations;
};

} // namespace duckdb
