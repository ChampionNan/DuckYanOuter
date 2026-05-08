//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/outer_yan/applicability.hpp"
#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! Wraps the full OuterYan IR pipeline: applicability → OT → OJT → plan.
//! Holds the shared metadata that all three stages rely on, so each
//! conversion no longer recomputes them. One instance per query lives in
//! `optimizer.cpp` and is threaded through OuterYanPre / DP / Post.
//!
//! Lifetimes:
//!   - `source_plan`           moved in by `BuildOT`, owned through every
//!                             OT- and OJT-stage transform; consumed (or
//!                             released) by `OJTToLogicalPlan`.
//!   - `ot`                    built by `BuildOT`, consumed by `BuildOJT`.
//!   - `ojt`                   built by `BuildOJT`, consumed by
//!                             `OJTToLogicalPlan`.
//!   - `filter_records`        populated by `BuildOT`, consumed by
//!                             `OJTToLogicalPlan`. Stays on the wrapper
//!                             through OT- and OJT-stage transforms.
//!   - `ot_relations_by_id`,
//!     `ot_joins_by_order`,
//!     `table_to_relation`     populated by `BuildOT`. OT-stage transforms
//!                             that restructure the skeleton must keep
//!                             these in sync (currently a no-op since
//!                             transforms are TBD).
class OuterYanTree {
public:
	OuterYanTree() = default;

	// ----------------------------------------------------------------------
	// Conversions — each mutates internal state in place.
	// ----------------------------------------------------------------------

	//! Stage 1: applicability gate. Stores the result internally and
	//! returns `applicable`.
	bool CheckApplicability(LogicalOperator &plan);

	//! Stage 2: lift LogicalPlan into `ot`. Pre-condition: applicable.
	//! Walks the plan, builds a binary OT skeleton (JOIN + RELATION
	//! OTNodes), attributes each join's `conditions[0]` to its left/right
	//! relation ids, extracts residual filters into `filter_records`, and
	//! assigns per-join `order` (deepest = 1, root = N). Takes ownership
	//! of `plan` into `source_plan`.
	void BuildOT(unique_ptr<LogicalOperator> plan);

	//! Stage 3: build `ojt` from `ot`. Pre-condition: `ot` populated.
	//! Reads `ot_relations_by_id` and `ot_joins_by_order` to drive
	//! top-down OJT assembly without re-walking the OT.
	void BuildOJT();

	//! Stage 4: realise `ojt` (and `filter_records`) back to a
	//! LogicalPlan. Pre-condition: `ojt` populated. Releases
	//! `source_plan` and `ojt` since the rebuilt plan is structurally
	//! independent (base-relation subtrees are deep-copied via
	//! `LogicalOperator::Copy`).
	unique_ptr<LogicalOperator> OJTToLogicalPlan(ClientContext &context);

	// ----------------------------------------------------------------------
	// Accessors for passes.
	// ----------------------------------------------------------------------

	OperatorTree &OT() {
		return *ot;
	}
	OrderedJoinTree &OJT() {
		return *ojt;
	}
	const ApplicabilityResult &Applicability() const {
		return applicability;
	}
	bool HasOT() const {
		return ot.get() != nullptr;
	}
	bool HasOJT() const {
		return ojt.get() != nullptr;
	}

	// ----------------------------------------------------------------------
	// Shared metadata, populated during BuildOT.
	// ----------------------------------------------------------------------

	//! `LogicalGet::table_index` → OT/OJT relation_id.
	unordered_map<idx_t, RelationId> table_to_relation;
	//! OT RELATION nodes indexed by relation_id.
	vector<OTNode *> ot_relations_by_id;
	//! OT JOIN nodes sorted by `order` ascending (deepest first).
	vector<OTNode *> ot_joins_by_order;
	//! Centralised filter store. Owned here through OT and OJT phases;
	//! consumed by `OJTToLogicalPlan`.
	vector<unique_ptr<OuterYanFilterRecord>> filter_records;

private:
	//! Original LogicalPlan, owned for the wrapper's lifetime so that raw
	//! `OTNode::origin` / `OJTEdge::join_op` / `OJTNode::base_op` pointers
	//! stay valid.
	unique_ptr<LogicalOperator> source_plan;
	ApplicabilityResult applicability;
	unique_ptr<OperatorTree> ot;
	unique_ptr<OrderedJoinTree> ojt;
};

// ============================================================================
// Free-function aliases — same names callers were using before, now thin
// wrappers over the corresponding `OuterYanTree` methods.
// ============================================================================

inline void LogicalPlanToOT(unique_ptr<LogicalOperator> plan, OuterYanTree &tree) {
	tree.BuildOT(std::move(plan));
}

inline void OTToOJT(OuterYanTree &tree) {
	tree.BuildOJT();
}

inline unique_ptr<LogicalOperator> OJTToLogicalPlan(ClientContext &context, OuterYanTree &tree) {
	return tree.OJTToLogicalPlan(context);
}

} // namespace duckdb
