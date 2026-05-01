//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_dp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/optimizer/outer_yan/cost_model_outer.hpp"
#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! Per-subtree DP memo entry — best-known plan rooted at a given OJT node.
//!
//! Holds the chosen subtree directly (`plan`) so the memo IS the result.
//! Once enumeration completes, the entry for the chosen overall root
//! contains a fully-constructed OJT subtree that can be converted straight
//! to a LogicalPlan via the shared tree conversion utilities — no extra
//! "realise" step is needed.
struct DPMemoEntry {
	double cost = 0.0;
	unique_ptr<OJTNode> plan;
};

//! Memo table for OuterYanDP. Keyed by OJT node identity (relation_id of
//! the subtree root).
class DPMemo {
public:
	DPMemo();

	bool Has(idx_t relation_id) const;
	const DPMemoEntry &Get(idx_t relation_id) const;
	void Put(idx_t relation_id, DPMemoEntry entry);
	void Clear();

	//! Take ownership of the best-cost entry's plan. Used to extract the
	//! final plan after enumeration completes.
	unique_ptr<OJTNode> ExtractBest();

private:
	unordered_map<idx_t, DPMemoEntry> memo;
};

//! OuterYanDP — second OuterYan pass: distinct top-down DP enumerator on
//! Ordered Join Trees. Replaces (does not extend) DuckDB's bottom-up DPhyp
//! `JoinOrderOptimizer` for queries OuterYan accepts.
//!
//! Rationale for a distinct path:
//!   - Existing DPhyp assumes inner-join predicate transitivity in
//!     QueryGraph hyperedges; outer joins do not satisfy this property.
//!   - Our enumeration is top-down on OJT — start from a (forced or chosen)
//!     root, recursively split children, memoise.
//!
//! The existing JOIN_ORDER pass short-circuits when the OuterYan
//! applicability flag is set, so OuterYanDP is the sole reorderer for
//! matched queries.
//!
//! `Optimize` orchestrates. Tree conversions (LogicalPlan ↔ OT ↔ OJT)
//! delegate to the shared utilities in `tree_conversions.hpp`.
class OuterYanDP {
public:
	explicit OuterYanDP(ClientContext &context);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	//! Top-down DP entry. Fills `memo`; for aggregation queries the root is
	//! forced via the marker installed by OuterYanPre.
	void EnumerateRoot(OrderedJoinTree &ojt);

private:
	//! Recursive top-down enumeration.
	void EnumerateNode(OJTNode &node);

private:
	ClientContext &context;
	OuterYanCostModel cost_model;
	DPMemo memo;
};

} // namespace duckdb
