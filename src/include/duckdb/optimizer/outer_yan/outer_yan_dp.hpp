//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_dp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"
#include "duckdb/optimizer/join_order/relation_index.hpp"
#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! OuterYanDP -- second OuterYan pass. Bottom-up DPccp on OT, mirroring
//! `PlanEnumerator::SolveJoinOrderExactly` ([plan_enumerator.cpp]) with two
//! deviations:
//!   1. Reorderability check is dropped: every OT JOIN is an edge regardless
//!      of `join_kind`. OuterYan applicability guarantees the predicate
//!      graph is acyclic and the joins meet outer-join associativity, so the
//!      DPccp enumeration is sound across INNER / LEFT / RIGHT / OUTER.
//!   2. Cost uses the OuterYan formula per-step (see `EdgeCardinality`),
//!      bypassing `CardinalityEstimator::InitEquivalentRelations` so that
//!      predicate transitivity is *not* applied -- every join condition's
//!      columns are treated as private.
//!
//! Greedy fallback (`SolveApproximately`) is a port of
//! `PlanEnumerator::SolveJoinOrderApproximately` with the cross-product
//! branch deleted: if greedy cannot find a connecting edge, DP bails out
//! without rewriting the OT (Path A in the design discussion).
//!
//! Output: `tree.ot->root` is replaced with the chosen tree and
//! `tree.ot_joins_by_order` is refreshed in post-order. `tree.BuildOJT()` is
//! then called to lift the new OT into `tree.ojt` for OuterYanPost.
class OuterYanDP {
public:
	explicit OuterYanDP(ClientContext &context);

	//! Consumes `tree.ot` (built by OuterYanPre), runs bottom-up DPccp, and
	//! installs the chosen ordering back into `tree.ot` + `tree.ojt`. On
	//! greedy bail-out the OT is left untouched.
	void Optimize(OuterYanTree &tree);

	//! Switch-to-greedy threshold, identical to DuckDB's DPhyp choice.
	static constexpr idx_t THRESHOLD_TO_SWAP_TO_APPROXIMATE = 12;
	//! Pair cap before falling back to greedy.
	static constexpr idx_t MAX_PAIRS = 10000;

private:
	//! Which root-fix regime is applied to every candidate subset during
	//! DPccp / greedy enumeration. "Output relations" are the relations that
	//! carry GROUP BY / SELECT DISTINCT columns -- they must end up above all
	//! "non-output" relations in the chosen tree.
	//!
	//!   - `NONE`              — no output relation is anchored. Used either
	//!                            when the query is not aggregating or when
	//!                            the aggregation does not pin any relation
	//!                            (e.g. COUNT_STAR with no GROUP BY).
	//!                            Enumeration is fully unconstrained.
	//!   - `FIX_ALL_OUTPUTS`   — primary regime when the query is anchored.
	//!                            Every relation in `output_relations` must
	//!                            sit above every non-output relation. An
	//!                            intermediate subset is rejected iff it
	//!                            contains at least one output relation while
	//!                            still missing some non-output relation.
	//!   - `FIX_ONE_OUTPUT`    — relaxed fallback when `FIX_ALL_OUTPUTS`
	//!                            admits no plan. A single chosen output
	//!                            relation (`fixed_output_relation`) is
	//!                            forbidden from every intermediate subset
	//!                            and must enter at the final emit. The
	//!                            other output relations are unconstrained
	//!                            and may interleave with non-output
	//!                            relations. `Optimize` tries every output
	//!                            relation as the pinned candidate and keeps
	//!                            the cheapest resulting plan.
	enum class RootFixMode { NONE, FIX_ALL_OUTPUTS, FIX_ONE_OUTPUT };

	// ---- setup ----
	void BuildLeafRelations(OuterYanTree &tree);
	void BuildEdgesFromOT(OuterYanTree &tree);
	void InitLeafPlans(OuterYanTree &tree);
	//! Translate `tree.root_relations` (RelationId, the GROUP BY / DISTINCT
	//! anchor set) into the DP-side `output_relations` (RelationIndex) and
	//! cache `num_non_output`. Sets `fix_mode` to `FIX_ALL_OUTPUTS` when the
	//! anchor set is non-empty, otherwise `NONE`.
	void PopulateOutputRelations(OuterYanTree &tree);
	//! Reset enumeration state between attempts. Clears `plans`, `pairs`,
	//! `bailed_out`, then repopulates leaf plans so the next attempt starts
	//! from a clean memo.
	void ResetForRetry(OuterYanTree &tree);
	//! Drive a single enumeration attempt under the currently-set `fix_mode`.
	//! Chooses exact DPccp or greedy based on `num_relations`, mirroring the
	//! original `Optimize` policy; on pair-cap fallback it re-initialises
	//! leaf plans before retrying greedy. Returns `true` on success (a plan
	//! may or may not cover the full set; the caller checks `plans`).
	bool RunEnumeration(OuterYanTree &tree);
	//! Reject `set` if it would violate the active root-fix regime. Always
	//! permits the full set (`set.count == num_relations`).
	bool ViolatesRootFix(JoinRelationSet &set) const;

	// ---- DPccp body (mirrors PlanEnumerator) ----
	bool SolveExactly();
	bool SolveApproximately();
	bool EmitCSG(JoinRelationSet &node);
	bool EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<RelationIndex> &exclusion_set);
	bool EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right,
	                           unordered_set<RelationIndex> &exclusion_set);
	bool TryEmitPair(JoinRelationSet &left, JoinRelationSet &right,
	                 const vector<reference<NeighborInfo>> &info);
	DPJoinNode &EmitPair(JoinRelationSet &left, JoinRelationSet &right,
	                     const vector<reference<NeighborInfo>> &info);
	unique_ptr<DPJoinNode> CreateJoinTree(JoinRelationSet &set,
	                                      const vector<reference<NeighborInfo>> &possible_connections,
	                                      DPJoinNode &left, DPJoinNode &right);

	// ---- cost ----
	//! Output cardinality of `L_set joinKind R_set` via the OT JOIN's owned
	//! metadata. Applies the user's outer-join correction formula on top of
	//! `T_inner = T_L * T_R / D_edge`. Orientation is decided by checking
	//! which side contains `info.cond_left_relation_id`.
	double EdgeCardinality(JoinRelationSet &left_set, JoinRelationSet &right_set,
	                       const DPJoinNode &left_plan, const DPJoinNode &right_plan,
	                       const OTJoin &info);
	double ComputeCost(const DPJoinNode &left, const DPJoinNode &right, double combined_cardinality);

	// ---- materialisation ----
	//! Walk the chosen DP plan rooted at `plans[full_set]`; produce a new OT
	//! root and replace `tree.ot->root`. Refreshes `tree.ot_joins_by_order`.
	void RebuildOT(OuterYanTree &tree, JoinRelationSet &full_set);
	unique_ptr<OTNode> MaterializeNode(JoinRelationSet &set, OuterYanTree &tree, idx_t &next_order);

	//! Pick the FilterInfo on a NeighborInfo that represents the (unique)
	//! OT JOIN bridging two subsets in the acyclic graph. Errors if none.
	const FilterInfo &PickConnectingEdge(const DPJoinNode &node);

	// ---- private static helpers (pure utilities, DP-only) ----
	//! Walk past single-child LogicalFilter / LogicalProjection wrappers to
	//! the base-relation operator (typically LogicalGet).
	static LogicalOperator *FindBaseGet(LogicalOperator *op);
	//! First BoundColumnRefExpression encountered in `expr`. OuterYan
	//! applicability restricts join conditions to one column ref per side.
	static optional_ptr<const class BoundColumnRefExpression> FirstColumnRef(const Expression &expr);
	//! HLL-or-fallback distinct count for `stats`' logical column.
	static idx_t LookupDistinctCount(const RelationStats &stats, idx_t column_index);
	//! DPccp neighbour-set utilities (verbatim port of plan_enumerator helpers).
	static void UpdateExclusionSet(JoinRelationSet &node, unordered_set<RelationIndex> &exclusion_set);
	static vector<unordered_set<RelationIndex>>
	AddSuperSets(const vector<unordered_set<RelationIndex>> &current,
	             const vector<RelationIndex> &all_neighbors);
	static vector<unordered_set<RelationIndex>> GetAllNeighborSets(vector<RelationIndex> neighbors);
	//! Post-order walk of an OT subtree, appending each JOIN node into `out`.
	static void RefreshOTJoinsByOrder(OTNode &node, vector<OTNode *> &out);

private:
	ClientContext &context;
	//! Relation-set interner. Mirrors `QueryGraphManager::set_manager`.
	JoinRelationSetManager set_manager;
	//! Edge graph. Mirrors `QueryGraphManager::query_graph`.
	QueryGraphEdges query_graph;
	//! Storage for FilterInfo carriers; pointers handed to QueryGraphEdges.
	vector<unique_ptr<FilterInfo>> filter_infos;
	//! Recovers the originating OT JOIN OTNode for a chosen DP edge:
	//!   ot_joins_by_filter_index[fi->filter_index] == jn
	//! Indexed by FilterInfo::filter_index in lock-step with `filter_infos`,
	//! mirroring DuckDB's `filters_and_bindings` indexing scheme. No hash
	//! map.
	vector<OTNode *> ot_joins_by_filter_index;
	//! DP memo: best plan per relation set. Same shape as
	//! `PlanEnumerator::plans`.
	reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>> plans;
	//! Pairs emitted so far. Switches to greedy past `MAX_PAIRS`.
	idx_t pairs = 0;
	//! Number of base relations on the OT; cached for the enumeration loop.
	idx_t num_relations = 0;
	//! Set if greedy could not connect all relations without a cross product.
	//! On true, `Optimize` leaves the OT untouched (Path A bail-out).
	bool bailed_out = false;
	//! Active root-fix regime for the current enumeration attempt. Set by
	//! `PopulateOutputRelations` for the first (FIX_ALL_OUTPUTS) attempt and
	//! by `Optimize` before each subsequent FIX_ONE_OUTPUT retry.
	RootFixMode fix_mode = RootFixMode::NONE;
	//! GROUP BY / SELECT DISTINCT anchor relations, translated from
	//! `tree.root_relations`. Populated by `PopulateOutputRelations`. Drives
	//! `FIX_ALL_OUTPUTS`; in `FIX_ONE_OUTPUT` the set is still consulted to
	//! enumerate retry candidates from `Optimize`.
	unordered_set<RelationIndex> output_relations;
	//! Cached `num_relations - output_relations.size()`. Used by the
	//! `FIX_ALL_OUTPUTS` predicate to test "covers every non-output" without
	//! recounting on each call.
	idx_t num_non_output = 0;
	//! In `FIX_ONE_OUTPUT`, the single output relation forced to be the
	//! final-emit addition. Unused in `NONE` / `FIX_ALL_OUTPUTS`.
	RelationIndex fixed_output_relation = RelationIndex(0);
};

} // namespace duckdb
