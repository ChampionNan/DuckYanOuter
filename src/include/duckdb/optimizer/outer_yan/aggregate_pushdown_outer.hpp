//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/aggregate_pushdown_outer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class LogicalComparisonJoin;
class Optimizer;
class OuterYanTree;

//! AggregatePushdownRewriter — hand-recursive insertion kernel shared by
//! the speculative (stage A on `plan_copy`) and real (stage B on the
//! original plan) passes of `AggregatePushdownOuter`.
//!
//! Walks `plan` post-order; at each handled join consults
//! `tree.agg_pushdown_decisions[current_join_id]` and, when
//! `push_into_child` is true, wraps the right child with a
//! `LogicalProjection ← LogicalAggregate ← original_right_child` scaffold,
//! rewrites the join's conditions to use the new bindings via a chained
//! `binding_map`, and emits a `LogicalProjection` above the join carrying
//! through passthrough columns plus annot column(s) per query type.
//!
//! State (`current_join_id`, `binding_map`, `agg_columns`) is reset by
//! constructing a fresh instance per stage — the orchestrator does this.
class AggregatePushdownRewriter {
public:
	//! Per-aggregate tracking for the root min/max/sum columns. The
	//! `original_binding` is what the root aggregate originally
	//! referenced (recorded by `BuildOT`); `current_binding` is the
	//! evolving binding as pushes rewrite the column through their
	//! aggregate output. `function_name` is one of `"min"`, `"max"`,
	//! `"sum"`.
	struct AggColumnInfo {
		ColumnBinding original_binding;
		ColumnBinding current_binding;
		string function_name;
	};

	AggregatePushdownRewriter(Binder &binder, ClientContext &context, const OuterYanTree &tree);

	//! Post-order rewrite — moves `op` and returns the (possibly
	//! replaced) root.
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

	//! Number of joins visited so far. The orchestrator asserts this
	//! equals `tree.agg_pushdown_decisions.size()` after each stage.
	idx_t JoinsVisited() const {
		return current_join_id;
	}

	//! Read-only access to the per-instance min/max/sum tracker. After
	//! stage B's `Rewrite` returns, the orchestrator queries this from
	//! `ReplaceRootMinMax` / `ReplaceRootSum` to rebind the root
	//! aggregate's children to the pushed annot bindings.
	const vector<AggColumnInfo> &AggColumns() const {
		return agg_columns;
	}

private:
	//! Build the `LogicalProjection ← LogicalAggregate ← child_node`
	//! scaffold (or `LogicalDistinct ← child_node` for `SELECT_DISTINCT`)
	//! for a pushdown candidate. Branches on
	//! `tree.root_aggregation.type`. Records old→new bindings into
	//! `binding_map`.
	unique_ptr<LogicalOperator> CreateDynamicAggregate(unique_ptr<LogicalOperator> child_node);

	//! Emit a projection above `op` carrying through `op`'s bindings minus
	//! `exclude`, plus `annot_expr` aliased `name`. Records the projection
	//! bindings into `binding_map`.
	unique_ptr<LogicalOperator> AddProjectionWithAnnot(unique_ptr<LogicalOperator> op,
	                                                   unique_ptr<Expression> annot_expr, const string &name,
	                                                   const vector<ColumnBinding> &exclude);

	//! Variant accepting a vector of annot expressions — needed for
	//! MIN/MAX where each minmax-target column carries its own annot
	//! through the projection.
	unique_ptr<LogicalOperator> AddProjectionWithAnnot(unique_ptr<LogicalOperator> op,
	                                                   vector<unique_ptr<Expression>> annot_exprs,
	                                                   const string &name,
	                                                   const vector<ColumnBinding> &exclude);

	//! Rewrite all bindings inside `join.conditions` via `GetUpdatedBinding`.
	void UpdateJoinConditions(LogicalComparisonJoin &join);

	//! Recursively rewrite `BoundColumnRefExpression` leaves via
	//! `GetUpdatedBinding`.
	void UpdateExpressionBindings(Expression &expr);

	//! Locate an `annot`-aliased projection expression at the top of
	//! `op`'s output.
	bool FindAnnotAttribute(LogicalOperator &op, ColumnBinding &out_binding, LogicalType &out_type);

	//! Find every `annot`-aliased projection expression at the top of
	//! `op`'s output. MIN/MAX queries can carry multiple annot columns
	//! through a projection (one per minmax target).
	bool FindAllAnnotAttributes(LogicalOperator &op, vector<ColumnBinding> &out_bindings,
	                            vector<LogicalType> &out_types);

	//! Collect positions in `op->GetColumnBindings()` of `annot`-aliased
	//! expressions on a child projection.
	void GetAnnotColumnBindingsIdx(LogicalOperator &op, vector<idx_t> &out);

	//! Record `old -> new` and apply transitive closure — any existing
	//! entry whose value was `old` is rewritten to `new`. Mirrors the
	//! reference's chained binding-map semantics.
	void UpdateBindingMap(ColumnBinding old_binding, ColumnBinding new_binding);

	//! Chained lookup — follows the map until it hits an unmapped key or a
	//! cycle. Safe against self-loops.
	ColumnBinding GetUpdatedBinding(ColumnBinding original) const;

	//! Snapshot of `tree.root_aggregation.columns` filtered to min/max/sum
	//! entries — the columns whose bindings need tracking across pushes
	//! for ReplaceRoot* to find them. Populated by `InitAggColumns` in
	//! the constructor.
	void InitAggColumns();

	//! Refresh every `agg_columns[i].current_binding` through the chained
	//! `binding_map`. Called after a push site updates the map.
	void RefreshAggColumnBindings();

	Binder &binder;
	ClientContext &context;
	const OuterYanTree &tree;
	idx_t current_join_id = 0;
	column_binding_map_t<ColumnBinding> binding_map;
	vector<AggColumnInfo> agg_columns;
};

//! AggregatePushdownOuter — fourth OuterYan pass. Runs after OuterYanPost
//! has lowered the OJT to a LogicalPlan and populated
//! `OuterYanTree::agg_pushdown_decisions`.
//!
//! Two-pass design (per `aggregation_pushdown.cpp@38d165ee`):
//!
//!   Stage A — Speculative refinement on a throwaway plan copy.
//!     1. `plan_copy = plan->Copy(context)`.
//!     2. Rewriter walks `plan_copy` post-order; every push site gets the
//!        aggregate scaffold + annot projection. Bindings are tracked
//!        through the rewriter's chained map.
//!     3. Run `max_height` rounds of `RemoveUnusedColumns` +
//!        `PruneInsertedAggregates` (annot-aware sandwich compression).
//!     4. `Prune(plan_copy)` walks post-order and flips
//!        `tree.agg_pushdown_decisions[k].push_into_child` to false when
//!        the surviving `groups.size() > AggPushdown::kGroupByNum`.
//!     5. Discard `plan_copy`.
//!
//!   Stage B — Real insertion on the original plan.
//!     1. Rewriter walks `plan` with the now-refined decisions.
//!     2. Final prune cycle + `ReplaceRootCountWithSum` (Slice 2D).
class AggregatePushdownOuter {
public:
	AggregatePushdownOuter(Optimizer &optimizer, OuterYanTree &tree);

	//! Entry point. Returns the (possibly rewritten) plan.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	//! Recognizer for the join kinds OuterYan emits. Public because the
	//! `AggregatePushdownRewriter` (declared above) also calls it.
	static bool AvailableJoin(LogicalOperatorType type);

private:
	//! Walk `plan_copy` after the speculative insertion + prune cycle and
	//! tighten the decision vector: any push site whose inserted
	//! aggregate still has `groups.size() > AggPushdown::kGroupByNum`
	//! after pruning has its `push_into_child` flipped back to false.
	void Prune(LogicalOperator &plan_copy);

	//! For root-aggregation `COUNT_STAR`: rewrite the root `count(*)` to
	//! `sum(annot)` so the query result actually consumes the pushdown.
	//! No-op for other query types.
	unique_ptr<LogicalOperator> ReplaceRootCountWithSum(unique_ptr<LogicalOperator> plan);

	//! For root-aggregation `MINMAX`: rebind each root min/max expression
	//! to read from the post-push annot column at that position. The
	//! `agg_columns` snapshot comes from stage B's rewriter and carries
	//! the post-push `current_binding` for every min/max target.
	unique_ptr<LogicalOperator>
	ReplaceRootMinMax(unique_ptr<LogicalOperator> plan,
	                  const vector<AggregatePushdownRewriter::AggColumnInfo> &agg_columns);

	//! For root-aggregation `SUM`: rebuild each root sum expression as
	//! `sum(annot)` where `annot` is the post-push column tracked by
	//! `agg_columns`. Limited to simple single-column sum targets in
	//! Slice 2C — complex `sum(expr)` over multiple columns is a
	//! follow-on.
	unique_ptr<LogicalOperator>
	ReplaceRootSum(unique_ptr<LogicalOperator> plan,
	               const vector<AggregatePushdownRewriter::AggColumnInfo> &agg_columns);

	//! Mirrors the reference's `Optimizer::DetermineMaxHeight` — counts
	//! only join operators, which is the worst-case number of iterations
	//! the alternating prune cycle needs to converge.
	static idx_t DetermineMaxHeight(const LogicalOperator &op);

	//! Narrowed equivalent of the reference's
	//! `PruneAggregationWithProjectionMap`. Walks `op` post-order looking
	//! for the `LogicalProjection ← LogicalAggregate` sandwich emitted by
	//! the rewriter and compresses `LogicalAggregate::groups` down to
	//! whatever the top projection still references.
	static unique_ptr<LogicalOperator> PruneInsertedAggregates(unique_ptr<LogicalOperator> op);

	Optimizer &optimizer;
	OuterYanTree &tree;
};

} // namespace duckdb
