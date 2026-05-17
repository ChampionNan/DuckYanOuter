//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/semijoin_insertion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/optimizer/outer_yan/semi_join_filter.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalSJBuild;

//! Semi-join (SJBuild / SJProbe) insertion pass.
//!
//! Runs as the **final OuterYan pass on the rebuilt LogicalPlan**, after
//! `OuterYanTree::OJTToLogicalPlan` (and after aggregation pushdown, when
//! that lands). Input pairs are pre-decided by
//! `OuterYanPre::{BottomUpPass, TopDownPass}` and stored on
//! `OuterYanTree::{bottom_up_pairs, top_down_pairs}`.
//!
//! Algorithm (plan 3 §"The pass itself — algorithm"):
//!   1. Group all pairs by `BuildKey` (sorted set of `keys[i].build_binding`).
//!      Each group corresponds to one shared `SemiJoinFilter`.
//!      Iteration order is **bottom_up first, then top_down**. Within a
//!      relation, earlier-iterated pairs end up *innermost* (closer to the
//!      LogicalGet) so data flowing up from the base passes through the
//!      earliest-decided reductions first.
//!   2. Pre-create one `LogicalSJBuild` per build relation carrying all
//!      filters for that relation; keep raw pointers so probes can wire up
//!      `related_sj_build`.
//!   3. Walk the rebuilt plan; splice wraps at the "filter + get" unit when
//!      present, otherwise above the bare LogicalGet. Anything else
//!      (LogicalProjection, LogicalAggregate, joins, ...) recurses past so
//!      the wraps land as close to the base relation as possible.
class SemijoinInsertion {
public:
	//! Splice LogicalSJBuild / LogicalSJProbe operators into `plan` based on
	//! `tree.{bottom_up_pairs, top_down_pairs}`. Returns the (possibly
	//! mutated) plan. The OJT inside `tree` has already been consumed by
	//! `OJTToLogicalPlan`; this pass reads only the pair lists and
	//! `table_to_relation`.
	unique_ptr<LogicalOperator> Apply(unique_ptr<LogicalOperator> plan, const OuterYanTree &tree);

private:
	//! Dedup key — two OuterYanSemiPairs share a SemiJoinFilter iff their
	//! `keys[i].build_binding` sets (as multisets) are equal. Probe side,
	//! probe relation, and pass-of-origin are NOT part of the key.
	struct BuildKey {
		vector<ColumnBinding> sorted_bindings; // sorted by (table_index, column_index)
		bool operator==(const BuildKey &o) const;
	};

	//! Hash on `BuildKey::sorted_bindings`. FNV-1a 64-bit
	//! (https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function).
	struct BuildKeyHash {
		size_t operator()(const BuildKey &k) const;
	};

	//! One probe contribution from a single pair, ready to splice as a
	//! LogicalSJProbe above the relation's filter+get unit (or bare get).
	struct ProbeWrap {
		shared_ptr<SemiJoinFilter> filter;
		vector<ColumnBinding> probe_bindings; // order matches pair.keys → filter->BuildBindings()
		RelationId related_build_rid;
	};

	//! Step 1 helper — canonicalise a pair's build bindings into a BuildKey.
	static BuildKey MakeBuildKey(const OuterYanSemiPair &pair);

	//! Recursive walk. At each node:
	//!   - LogicalGet: splice wraps right above the Get.
	//!   - LogicalFilter whose single child is a LogicalGet: splice wraps
	//!     above the Filter (treating filter+get as the base unit).
	//!   - Anything else: recurse into children.
	static void Splice(unique_ptr<LogicalOperator> &node,
	                   const unordered_map<idx_t, RelationId> &table_to_relation,
	                   unordered_map<RelationId, vector<ProbeWrap>> &probe_wraps,
	                   unordered_map<RelationId, unique_ptr<LogicalSJBuild>> &pending_builds,
	                   const unordered_map<RelationId, LogicalSJBuild *> &build_ptr_map);

	//! Wrap `node` with the given relation's probes (innermost-first in
	//! `probe_wraps[rid]`) and its LogicalSJBuild (outermost). The node may
	//! be either a bare LogicalGet or a LogicalFilter+LogicalGet unit;
	//! Splice picks which.
	static void SpliceAt(unique_ptr<LogicalOperator> &node, RelationId rid,
	                     unordered_map<RelationId, vector<ProbeWrap>> &probe_wraps,
	                     unordered_map<RelationId, unique_ptr<LogicalSJBuild>> &pending_builds,
	                     const unordered_map<RelationId, LogicalSJBuild *> &build_ptr_map);
};

} // namespace duckdb
