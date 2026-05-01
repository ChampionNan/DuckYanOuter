//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/cost_model_outer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"

namespace duckdb {

//! OuterYan-side cost model. Distinct from the existing CostModel because the
//! bottom-up DPhyp model assumes inner-join predicate transitivity; that
//! assumption fails for outer joins.
//!
//! Costing for individual edges is dispatched per join type — reuse the
//! existing DuckDB cost helpers (e.g. CostModel / CardinalityEstimator)
//! rather than reinventing per-operator formulas. Only the subtree
//! aggregation surface is exposed at this layer.
class OuterYanCostModel {
public:
	OuterYanCostModel();

	//! Cost of an entire OJT subtree rooted at `node`. Delegates per-edge
	//! cost to existing DuckDB cost helpers, dispatched on edge join type.
	double SubtreeCost(const OJTNode &node);
};

} // namespace duckdb
