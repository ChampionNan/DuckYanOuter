//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/outer_yan_post.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/evaluation_order.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/optimizer/outer_yan/resimplification.hpp"
#include "duckdb/optimizer/outer_yan/semijoin_insertion.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! OuterYanPost — third OuterYan pass (runs after OuterYanDP).
//!
//! `Optimize` orchestrates the steps below. Tree conversions delegate to
//! the shared utilities in `tree_conversions.hpp`.
//!
//! Step order:
//!   1. LogicalPlanToOJT  — (shared utility).
//!   2. Resimplify        — outer→original via arrow-side + eval-order.
//!   3. SemijoinInsert    — bottom-up + top-down passes inserting
//!                          SJBuild / SJProbe pairs.
//!   4. OrderFixApply     — verify / enforce evaluation order on OJT.
//!   5. OJTToLogicalPlan  — (shared utility).
//!
//! Skipped entirely when the OuterYan-active flag is false.
class OuterYanPost {
public:
	explicit OuterYanPost(ClientContext &context);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	void Resimplify(OrderedJoinTree &ojt);
	void SemijoinInsert(OrderedJoinTree &ojt);
	void OrderFixApply(OrderedJoinTree &ojt, bool is_aggregation_query);

private:
	ClientContext &context;
	Resimplification resimplification;
	SemijoinInsertion semijoin_insertion;
	OrderFix order_fix;
};

} // namespace duckdb
