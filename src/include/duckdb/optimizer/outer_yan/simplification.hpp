//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

namespace duckdb {

//! Walks the OperatorTree top-down and rewrites outer joins to the strongest
//! join kind whose semantics are preserved by the surrounding plan. Unlike
//! the binding-level OuterJoinSimplification, the analysis here works at the
//! granularity of OT base relations: a relation is null-rejected (i.e.,
//! cannot legally be the null-introducing side) iff a null-rejecting filter
//! anywhere in `filter_records` references it, or an enclosing join's
//! condition forbids NULLs in any of its columns.
//!
//! NOTE: filters in `tree.filter_records` are treated as if they constrain
//! the FINAL result — every null-rejecting filter contributes its
//! `referenced_relations` to the initial null-rejection set, regardless of
//! where the filter physically sat in the input plan. Any outer join that
//! would introduce NULLs into one of those relations is therefore rewritten
//! to a kind that does not. `DominatesJoin`-style position checks are
//! deliberately not used here; that orthogonal concern belongs to
//! filter-pushdown placement (`OJTToLogicalPlan`), not to this pass.
class Simplification {
public:
	void Apply(OuterYanTree &tree);

private:
	//! Top-down walk. RELATION nodes are no-ops. JOIN nodes:
	//!   a) collapse the join kind based on which child subtree intersects
	//!      `null_rej`;
	//!   b) extend `null_rej` with the relations that the (possibly rewritten)
	//!      join condition itself null-rejects, then recurse.
	//! `null_rej` is taken by value so per-subtree augmentations do not leak
	//! across siblings.
	void VisitNode(OuterYanTree &tree, OTNode &node,
	               unordered_set<idx_t> null_rej);

	//! Memoised set of relation_ids covered by `node`'s subtree.
	const unordered_set<idx_t> &ComputeSubtreeRelations(OTNode &node);

	//! Structural null-rejection test on a single conjunct: IS NOT NULL on a
	//! column ref, or any BOUND_COMPARISON other than IS [NOT] DISTINCT FROM.
	//! No recursion into AND/OR — conjuncts are pre-split by BuildOT.
	static bool FilterRejectsNulls(const Expression &expr);

	//! True iff `subtree_relations[&node]` ∩ `null_rej` ≠ ∅.
	bool SubtreeIntersectsNullRejection(OTNode &node,
	                                    const unordered_set<idx_t> &null_rej);

private:
	unordered_map<const OTNode *, unordered_set<idx_t>> subtree_relations;
};

} // namespace duckdb
