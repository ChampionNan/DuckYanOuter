#include "duckdb/optimizer/outer_yan/aggregate_pushdown_outer.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/outer_yan/agg_pushdown.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <functional>
#include <unordered_set>

namespace duckdb {

//! Forward declaration of DuckDB's `sum` aggregate factory. The definition
//! lives in `extension/core_functions/aggregate/distributive/sum.cpp`. The
//! reference implementation uses the same symbol.
AggregateFunction GetSumAggregate(PhysicalType type);

// ===========================================================================
// AggregatePushdownRewriter
// ===========================================================================

AggregatePushdownRewriter::AggregatePushdownRewriter(Binder &binder_p, ClientContext &context_p,
                                                     const OuterYanTree &tree_p)
    : binder(binder_p), context(context_p), tree(tree_p) {
	InitAggColumns();
}

void AggregatePushdownRewriter::InitAggColumns() {
	agg_columns.clear();
	for (const auto &col : tree.root_aggregation.columns) {
		if (col.function_name != "min" && col.function_name != "max" && col.function_name != "sum") {
			continue;
		}
		AggColumnInfo info;
		info.original_binding = col.binding;
		info.current_binding = col.binding;
		info.function_name = col.function_name;
		agg_columns.push_back(info);
	}
}

void AggregatePushdownRewriter::RefreshAggColumnBindings() {
	for (auto &info : agg_columns) {
		info.current_binding = GetUpdatedBinding(info.current_binding);
	}
}

unique_ptr<LogicalOperator> AggregatePushdownRewriter::Rewrite(unique_ptr<LogicalOperator> op) {
	if (!op) {
		return op;
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		op->children[i] = Rewrite(std::move(op->children[i]));
	}
	if (!AggregatePushdownOuter::AvailableJoin(op->type)) {
		return op;
	}
	auto &join = op->Cast<LogicalComparisonJoin>();
	D_ASSERT(join.join_type != JoinType::MARK);
	D_ASSERT(current_join_id < tree.agg_pushdown_decisions.size());

	const auto &decision = tree.agg_pushdown_decisions[current_join_id];
	++current_join_id;

	if (decision.push_into_child) {
		// Per project direction: only push into the right child. The left
		// side stays untouched so the annot accumulates upward via the
		// projection emitted below.
		join.children[1] = CreateDynamicAggregate(std::move(join.children[1]));
	}
	UpdateJoinConditions(join);
	op->ResolveOperatorTypes();
	// Keep `agg_columns[*].current_binding` in lock-step with the chained
	// `binding_map` so MINMAX / SUM lookups at the next-higher push and
	// in `ReplaceRoot*` see the post-push bindings.
	RefreshAggColumnBindings();

	// Annot propagation above the join — per query type.
	switch (tree.root_aggregation.type) {
	case OuterYanAggregationType::COUNT_STAR: {
		// Multi-side annot multiplies; single-side passes through.
		ColumnBinding left_annot;
		ColumnBinding right_annot;
		LogicalType left_type;
		LogicalType right_type;
		const bool left_has = FindAnnotAttribute(*join.children[0], left_annot, left_type);
		const bool right_has = FindAnnotAttribute(*join.children[1], right_annot, right_type);
		if (left_has && right_has) {
			vector<unique_ptr<Expression>> mult_children;
			mult_children.push_back(make_uniq<BoundColumnRefExpression>(left_type, left_annot));
			mult_children.push_back(make_uniq<BoundColumnRefExpression>(right_type, right_annot));
			FunctionBinder function_binder(context);
			ErrorData error;
			auto mult_expr = function_binder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(mult_children), error,
			                                                    /*is_operator=*/true, /*binder=*/nullptr);
			if (!mult_expr) {
				throw InternalException("AggregatePushdownOuter: failed to bind annot multiplication: %s",
				                        error.Message());
			}
			return AddProjectionWithAnnot(std::move(op), std::move(mult_expr), "annot", {left_annot, right_annot});
		}
		if (left_has) {
			auto ref = make_uniq<BoundColumnRefExpression>(left_type, left_annot);
			return AddProjectionWithAnnot(std::move(op), std::move(ref), "annot", {left_annot});
		}
		if (right_has) {
			auto ref = make_uniq<BoundColumnRefExpression>(right_type, right_annot);
			return AddProjectionWithAnnot(std::move(op), std::move(ref), "annot", {right_annot});
		}
		return op;
	}
	case OuterYanAggregationType::MINMAX: {
		// Multi-annot pass-through. Every min/max target carries its own
		// annot column through the projection; there is no
		// multiplicative fold because min/max is idempotent under join.
		vector<ColumnBinding> left_annots;
		vector<ColumnBinding> right_annots;
		vector<LogicalType> left_types;
		vector<LogicalType> right_types;
		const bool left_has = FindAllAnnotAttributes(*join.children[0], left_annots, left_types);
		const bool right_has = FindAllAnnotAttributes(*join.children[1], right_annots, right_types);
		if (!left_has && !right_has) {
			return op;
		}
		vector<unique_ptr<Expression>> annot_exprs;
		vector<ColumnBinding> exclude;
		for (idx_t i = 0; i < left_annots.size(); i++) {
			annot_exprs.push_back(make_uniq<BoundColumnRefExpression>(left_types[i], left_annots[i]));
			exclude.push_back(left_annots[i]);
		}
		for (idx_t i = 0; i < right_annots.size(); i++) {
			annot_exprs.push_back(make_uniq<BoundColumnRefExpression>(right_types[i], right_annots[i]));
			exclude.push_back(right_annots[i]);
		}
		return AddProjectionWithAnnot(std::move(op), std::move(annot_exprs), "annot", exclude);
	}
	case OuterYanAggregationType::SUM: {
		// Multi-side multiplicative fold — analogous to COUNT_STAR but
		// gathered through `FindAllAnnotAttributes` since each side may
		// carry several sum annots (one per sum target whose source lies
		// in that subtree).
		vector<ColumnBinding> left_annots;
		vector<ColumnBinding> right_annots;
		vector<LogicalType> left_types;
		vector<LogicalType> right_types;
		const bool left_has = FindAllAnnotAttributes(*join.children[0], left_annots, left_types);
		const bool right_has = FindAllAnnotAttributes(*join.children[1], right_annots, right_types);
		if (!left_has && !right_has) {
			return op;
		}
		vector<unique_ptr<Expression>> annot_exprs;
		vector<ColumnBinding> exclude;
		for (idx_t i = 0; i < left_annots.size(); i++) {
			annot_exprs.push_back(make_uniq<BoundColumnRefExpression>(left_types[i], left_annots[i]));
			exclude.push_back(left_annots[i]);
		}
		for (idx_t i = 0; i < right_annots.size(); i++) {
			annot_exprs.push_back(make_uniq<BoundColumnRefExpression>(right_types[i], right_annots[i]));
			exclude.push_back(right_annots[i]);
		}
		if (annot_exprs.size() == 1) {
			return AddProjectionWithAnnot(std::move(op), std::move(annot_exprs[0]), "annot", exclude);
		}
		if (annot_exprs.size() == 2) {
			// Two annots — left * right.
			FunctionBinder function_binder(context);
			ErrorData error;
			auto mult_expr = function_binder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(annot_exprs), error,
			                                                    /*is_operator=*/true, /*binder=*/nullptr);
			if (!mult_expr) {
				throw InternalException("AggregatePushdownOuter: failed to bind SUM annot multiplication: %s",
				                        error.Message());
			}
			return AddProjectionWithAnnot(std::move(op), std::move(mult_expr), "annot", exclude);
		}
		// 3+ annots — the reference does not handle this; pass through
		// unchanged. Downstream prune will collapse what it can.
		return AddProjectionWithAnnot(std::move(op), std::move(annot_exprs), "annot", exclude);
	}
	case OuterYanAggregationType::SELECT_DISTINCT:
		// DISTINCT has no annot semantics — each push's `LogicalDistinct`
		// already dedups its subtree. Just return the join with
		// rewritten conditions; the next-higher join sees the same
		// bindings unchanged.
		return op;
	case OuterYanAggregationType::OTHER:
	case OuterYanAggregationType::NONE:
		return op;
	}
	return op;
}

namespace {

//! Catalog-driven lookup mirroring the reference's
//! `GetMinAggregate` / `GetMaxAggregate`. DuckDB's `MinFunction` /
//! `MaxFunction::GetFunction()` return a generic template that is not
//! pre-bound to an input type; the catalog version picks the overload
//! that matches `input_type` exactly.
AggregateFunction LookupAggregate(ClientContext &context, const string &name, const LogicalType &input_type) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto entry = catalog.GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, DEFAULT_SCHEMA, name,
	                              OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw InternalException("AggregatePushdownOuter: catalog has no `%s` aggregate function", name);
	}
	auto &agg_entry = entry->Cast<AggregateFunctionCatalogEntry>();
	return agg_entry.functions.GetFunctionByArguments(context, {input_type});
}

} // namespace

unique_ptr<LogicalOperator>
AggregatePushdownRewriter::CreateDynamicAggregate(unique_ptr<LogicalOperator> child_node) {
	child_node->ResolveOperatorTypes();
	auto child_bindings = child_node->GetColumnBindings();
	auto child_types = child_node->types;

	// SELECT_DISTINCT special-case: replace the aggregate scaffold with a
	// `LogicalDistinct` over every child column. No annot column is
	// emitted — DISTINCT carries its own dedup contract upward and the
	// join condition lookups continue against the same underlying child
	// bindings. Mirrors the reference's
	// `CreateDynamicAggregate` SELECT_DISTINCT branch
	// (`aggregation_pushdown.cpp:1220`).
	if (tree.root_aggregation.type == OuterYanAggregationType::SELECT_DISTINCT) {
		vector<unique_ptr<Expression>> distinct_targets;
		distinct_targets.reserve(child_bindings.size());
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			distinct_targets.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
		}
		auto distinct = make_uniq<LogicalDistinct>(std::move(distinct_targets), DistinctType::DISTINCT);
		distinct->AddChild(std::move(child_node));
		distinct->ResolveOperatorTypes();
		return distinct;
	}

	// Position of any pre-existing "annot"-aliased column from a deeper
	// push. Also accumulates fresh agg-target columns when MINMAX
	// introduces them in this scaffold; the group-by step later treats
	// every entry in this vector as a column to exclude from the keys.
	vector<idx_t> annot_indices;
	if (child_node->type != LogicalOperatorType::LOGICAL_GET) {
		GetAnnotColumnBindingsIdx(*child_node, annot_indices);
	}

	const TableIndex group_index = binder.GenerateTableIndex();
	const TableIndex aggregate_index = binder.GenerateTableIndex();

	vector<unique_ptr<Expression>> select_list;
	idx_t agg_pos = 0;

	switch (tree.root_aggregation.type) {
	case OuterYanAggregationType::COUNT_STAR: {
		if (!annot_indices.empty()) {
			// sum(annot) — collapses the deeper push's per-row annot.
			const idx_t annot_idx = annot_indices[0];
			vector<unique_ptr<Expression>> sum_args;
			sum_args.push_back(
			    make_uniq<BoundColumnRefExpression>(child_types[annot_idx], child_bindings[annot_idx]));

			auto sum_function = GetSumAggregate(child_types[annot_idx].InternalType());
			if (sum_function.name.empty()) {
				sum_function.name = "sum";
			}
			FunctionBinder function_binder(context);
			auto sum_expr = function_binder.BindAggregateFunction(sum_function, std::move(sum_args), nullptr,
			                                                     AggregateType::NON_DISTINCT);
			select_list.push_back(std::move(sum_expr));
			UpdateBindingMap(child_bindings[annot_idx], ColumnBinding(aggregate_index, ProjectionIndex(agg_pos)));
			++agg_pos;
		} else {
			auto count_star_fun = CountStarFun::GetFunction();
			FunctionBinder function_binder(context);
			auto count_star = function_binder.BindAggregateFunction(count_star_fun, {}, nullptr,
			                                                       AggregateType::NON_DISTINCT);
			select_list.push_back(std::move(count_star));
			++agg_pos;
		}
		break;
	}
	case OuterYanAggregationType::MINMAX:
	case OuterYanAggregationType::SUM: {
		// For each child column that an `agg_columns` entry currently
		// maps to (either the original column from the base table or a
		// pre-aggregated annot from a deeper push), emit the
		// type-appropriate aggregate on it. The two query types share
		// this loop because the only thing that differs per match is
		// which aggregate factory we call.
		const auto query_type = tree.root_aggregation.type;
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			const AggColumnInfo *matching = nullptr;
			for (const auto &agg_col : agg_columns) {
				if (query_type == OuterYanAggregationType::MINMAX) {
					if (agg_col.function_name != "min" && agg_col.function_name != "max") {
						continue;
					}
				} else {
					if (agg_col.function_name != "sum") {
						continue;
					}
				}
				if (GetUpdatedBinding(agg_col.original_binding) == child_bindings[i]) {
					matching = &agg_col;
					break;
				}
			}
			if (!matching) {
				continue;
			}
			vector<unique_ptr<Expression>> args;
			args.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
			AggregateFunction agg_func = (matching->function_name == "sum")
			                                 ? GetSumAggregate(child_types[i].InternalType())
			                                 : LookupAggregate(context, matching->function_name, child_types[i]);
			if (matching->function_name == "sum" && agg_func.name.empty()) {
				agg_func.name = "sum";
			}
			FunctionBinder function_binder(context);
			auto expr = function_binder.BindAggregateFunction(agg_func, std::move(args), nullptr,
			                                                 AggregateType::NON_DISTINCT);
			select_list.push_back(std::move(expr));
			bool already_marked = false;
			for (auto idx : annot_indices) {
				if (idx == i) {
					already_marked = true;
					break;
				}
			}
			if (!already_marked) {
				annot_indices.push_back(i);
			}
			UpdateBindingMap(child_bindings[i], ColumnBinding(aggregate_index, ProjectionIndex(agg_pos)));
			++agg_pos;
		}
		break;
	}
	case OuterYanAggregationType::SELECT_DISTINCT:
		// Handled at the top of the function — should never fall through here.
	case OuterYanAggregationType::OTHER:
	case OuterYanAggregationType::NONE:
		throw InternalException(
		    "AggregatePushdownOuter::CreateDynamicAggregate: unsupported root_aggregation.type");
	}

	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(select_list));

	idx_t group_pos = 0;
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		bool is_annot = false;
		for (auto annot_idx : annot_indices) {
			if (i == annot_idx) {
				is_annot = true;
				break;
			}
		}
		if (is_annot) {
			continue;
		}
		aggregate->groups.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
		UpdateBindingMap(child_bindings[i], ColumnBinding(group_index, ProjectionIndex(group_pos)));
		++group_pos;
	}
	if (!aggregate->groups.empty()) {
		GroupingSet grouping_set;
		for (idx_t i = 0; i < aggregate->groups.size(); i++) {
			grouping_set.insert(ProjectionIndex(i));
		}
		aggregate->grouping_sets.push_back(std::move(grouping_set));
	}
	aggregate->AddChild(std::move(child_node));
	aggregate->ResolveOperatorTypes();

	// Extra projection above the aggregate — gives the next-higher join a
	// stable set of bindings to read so it does not need to know about the
	// `group_index` / `aggregate_index` split internally.
	const TableIndex projection_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;
	const auto agg_bindings = aggregate->GetColumnBindings();
	for (idx_t i = 0; i < aggregate->groups.size(); i++) {
		const auto &group_binding = agg_bindings[i];
		const auto col_type = aggregate->groups[i]->return_type;
		proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(col_type, group_binding));
		UpdateBindingMap(group_binding, ColumnBinding(projection_index, ProjectionIndex(i)));
	}
	for (idx_t i = 0; i < aggregate->expressions.size(); i++) {
		const auto agg_binding = ColumnBinding(aggregate->aggregate_index, ProjectionIndex(i));
		auto proj_expr =
		    make_uniq<BoundColumnRefExpression>(string("annot"), aggregate->expressions[i]->return_type, agg_binding);
		proj_expressions.push_back(std::move(proj_expr));
		UpdateBindingMap(agg_binding, ColumnBinding(projection_index, ProjectionIndex(i + aggregate->groups.size())));
	}
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(proj_expressions));
	projection->AddChild(std::move(aggregate));
	projection->ResolveOperatorTypes();
	return projection;
}

unique_ptr<LogicalOperator>
AggregatePushdownRewriter::AddProjectionWithAnnot(unique_ptr<LogicalOperator> op,
                                                  unique_ptr<Expression> annot_expr, const string &name,
                                                  const vector<ColumnBinding> &exclude) {
	vector<unique_ptr<Expression>> exprs;
	if (annot_expr) {
		exprs.push_back(std::move(annot_expr));
	}
	return AddProjectionWithAnnot(std::move(op), std::move(exprs), name, exclude);
}

unique_ptr<LogicalOperator>
AggregatePushdownRewriter::AddProjectionWithAnnot(unique_ptr<LogicalOperator> op,
                                                  vector<unique_ptr<Expression>> annot_exprs, const string &name,
                                                  const vector<ColumnBinding> &exclude) {
	auto bindings = op->GetColumnBindings();
	const TableIndex projection_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;

	for (idx_t i = 0; i < bindings.size(); i++) {
		bool excluded = false;
		for (const auto &b : exclude) {
			if (bindings[i] == b) {
				excluded = true;
				break;
			}
		}
		if (excluded) {
			continue;
		}
		proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(op->types[i], bindings[i]));
		UpdateBindingMap(bindings[i], ColumnBinding(projection_index, ProjectionIndex(proj_expressions.size() - 1)));
	}
	for (auto &annot_expr : annot_exprs) {
		if (!annot_expr) {
			continue;
		}
		annot_expr->alias = name;
		const auto annot_pos = ProjectionIndex(proj_expressions.size());
		if (annot_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = annot_expr->Cast<BoundColumnRefExpression>();
			UpdateBindingMap(col_ref.binding, ColumnBinding(projection_index, annot_pos));
		} else if (annot_expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func_expr = annot_expr->Cast<BoundFunctionExpression>();
			for (auto &child : func_expr.children) {
				if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
					continue;
				}
				auto &child_ref = child->Cast<BoundColumnRefExpression>();
				UpdateBindingMap(child_ref.binding, ColumnBinding(projection_index, annot_pos));
			}
		} else {
			throw InternalException("AggregatePushdownOuter: annot expression has unsupported class");
		}
		proj_expressions.push_back(std::move(annot_expr));
	}
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(proj_expressions));
	projection->AddChild(std::move(op));
	projection->ResolveOperatorTypes();
	return projection;
}

void AggregatePushdownRewriter::UpdateJoinConditions(LogicalComparisonJoin &join) {
	for (auto &cond : join.conditions) {
		UpdateExpressionBindings(cond.GetLHS());
		UpdateExpressionBindings(cond.GetRHS());
	}
}

void AggregatePushdownRewriter::UpdateExpressionBindings(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		col_ref.binding = GetUpdatedBinding(col_ref.binding);
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { UpdateExpressionBindings(child); });
}

bool AggregatePushdownRewriter::FindAnnotAttribute(LogicalOperator &op, ColumnBinding &out_binding,
                                                  LogicalType &out_type) {
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	op.ResolveOperatorTypes();
	auto &proj = op.Cast<LogicalProjection>();
	const auto bindings = op.GetColumnBindings();
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		if (proj.expressions[i]->alias == "annot") {
			out_binding = bindings[i];
			out_type = op.types[i];
			return true;
		}
	}
	return false;
}

bool AggregatePushdownRewriter::FindAllAnnotAttributes(LogicalOperator &op, vector<ColumnBinding> &out_bindings,
                                                      vector<LogicalType> &out_types) {
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	op.ResolveOperatorTypes();
	auto &proj = op.Cast<LogicalProjection>();
	const auto bindings = op.GetColumnBindings();
	bool found = false;
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		if (proj.expressions[i]->alias == "annot") {
			out_bindings.push_back(bindings[i]);
			out_types.push_back(op.types[i]);
			found = true;
		}
	}
	return found;
}

void AggregatePushdownRewriter::GetAnnotColumnBindingsIdx(LogicalOperator &op, vector<idx_t> &out) {
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &proj = op.Cast<LogicalProjection>();
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		if (proj.expressions[i]->alias == "annot") {
			out.push_back(i);
		}
	}
}

void AggregatePushdownRewriter::UpdateBindingMap(ColumnBinding old_binding, ColumnBinding new_binding) {
	if (old_binding == new_binding) {
		return;
	}
	binding_map[old_binding] = new_binding;
	for (auto &entry : binding_map) {
		if (entry.first != old_binding && entry.second == old_binding) {
			entry.second = new_binding;
		}
	}
}

ColumnBinding AggregatePushdownRewriter::GetUpdatedBinding(ColumnBinding original) const {
	auto current = original;
	while (true) {
		auto it = binding_map.find(current);
		if (it == binding_map.end()) {
			return current;
		}
		const auto next = it->second;
		if (next == original) {
			return current;
		}
		current = next;
	}
}

// ===========================================================================
// AggregatePushdownOuter — static helpers
// ===========================================================================

bool AggregatePushdownOuter::AvailableJoin(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	       type == LogicalOperatorType::LOGICAL_ASOF_JOIN ||
	       type == LogicalOperatorType::LOGICAL_DELIM_JOIN;
}

idx_t AggregatePushdownOuter::DetermineMaxHeight(const LogicalOperator &op) {
	idx_t max_child_height = 0;
	for (const auto &child : op.children) {
		if (!child) {
			continue;
		}
		max_child_height = MaxValue(max_child_height, DetermineMaxHeight(*child));
	}
	// Per reference's `Optimizer::DetermineMaxHeight`: only join operators
	// contribute to the height because column-prunability propagates one
	// join level per iteration.
	return max_child_height + (AvailableJoin(op.type) ? 1 : 0);
}

unique_ptr<LogicalOperator> AggregatePushdownOuter::PruneInsertedAggregates(unique_ptr<LogicalOperator> op) {
	if (!op) {
		return op;
	}
	for (auto &child : op->children) {
		child = PruneInsertedAggregates(std::move(child));
	}
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return op;
	}
	if (op->children.size() != 1 ||
	    op->children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return op;
	}
	auto &top_proj = op->Cast<LogicalProjection>();
	auto &agg = op->children[0]->Cast<LogicalAggregate>();
	if (agg.groups.empty()) {
		return op;
	}

	// Collect which group-side `column_index` values the top projection
	// still references. `ExpressionIterator::VisitExpression<T>` is the
	// shared recursive walker DuckDB's column-prune passes use.
	std::unordered_set<idx_t> referenced_groups;
	for (auto &expr : top_proj.expressions) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *expr, [&](const BoundColumnRefExpression &col_ref) {
			    if (col_ref.binding.table_index == agg.group_index) {
				    referenced_groups.insert(col_ref.binding.column_index.GetIndexUnsafe());
			    }
		    });
	}
	if (referenced_groups.size() == agg.groups.size()) {
		return op;
	}

	column_binding_map_t<ColumnBinding> remap;
	vector<unique_ptr<Expression>> new_groups;
	new_groups.reserve(referenced_groups.size());
	idx_t new_idx = 0;
	for (idx_t i = 0; i < agg.groups.size(); i++) {
		if (referenced_groups.count(i) == 0) {
			continue;
		}
		remap[ColumnBinding(agg.group_index, ProjectionIndex(i))] =
		    ColumnBinding(agg.group_index, ProjectionIndex(new_idx));
		new_groups.push_back(std::move(agg.groups[i]));
		++new_idx;
	}
	agg.groups = std::move(new_groups);
	agg.grouping_sets.clear();
	if (!agg.groups.empty()) {
		GroupingSet grouping_set;
		for (idx_t i = 0; i < agg.groups.size(); i++) {
			grouping_set.insert(ProjectionIndex(i));
		}
		agg.grouping_sets.push_back(std::move(grouping_set));
	}

	std::function<void(Expression &)> rewrite_bindings = [&](Expression &expr) {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = expr.Cast<BoundColumnRefExpression>();
			auto it = remap.find(col_ref.binding);
			if (it != remap.end()) {
				col_ref.binding = it->second;
			}
			return;
		}
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { rewrite_bindings(child); });
	};
	for (auto &expr : top_proj.expressions) {
		rewrite_bindings(*expr);
	}
	agg.ResolveOperatorTypes();
	top_proj.ResolveOperatorTypes();
	return op;
}

// ===========================================================================
// AggregatePushdownOuter — orchestrator + decision refiner
// ===========================================================================

AggregatePushdownOuter::AggregatePushdownOuter(Optimizer &optimizer_p, OuterYanTree &tree_p)
    : optimizer(optimizer_p), tree(tree_p) {
}

void AggregatePushdownOuter::Prune(LogicalOperator &plan_copy) {
	// Walk `plan_copy` in post-order with a fresh counter (mirrors the
	// rewriter's `current_join_id` ordering), inspect each pushed
	// aggregate's surviving `groups.size()`, and tighten the decision
	// vector accordingly.
	idx_t walk_join_id = 0;
	std::function<void(LogicalOperator &)> walk = [&](LogicalOperator &op) {
		for (auto &child : op.children) {
			if (child) {
				walk(*child);
			}
		}
		if (!AvailableJoin(op.type)) {
			return;
		}
		auto &join = op.Cast<LogicalComparisonJoin>();
		D_ASSERT(join.join_type != JoinType::MARK);
		D_ASSERT(walk_join_id < tree.agg_pushdown_decisions.size());
		auto &decision = tree.agg_pushdown_decisions[walk_join_id];
		++walk_join_id;

		if (!decision.push_into_child) {
			return;
		}
		auto &right = *join.children[1];
		// SELECT_DISTINCT scaffold is a plain `LogicalDistinct` directly
		// on the right child — the threshold check is on
		// `distinct_targets.size()`.
		if (right.type == LogicalOperatorType::LOGICAL_DISTINCT) {
			auto &distinct = right.Cast<LogicalDistinct>();
			if (distinct.distinct_targets.size() > AggPushdown::kGroupByNum) {
				decision.push_into_child = false;
			}
			return;
		}
		// COUNT_STAR / MINMAX / SUM scaffold: `LogicalProjection ←
		// LogicalAggregate ← original_right_child`. If pruning removed
		// the scaffold entirely (rare; the join consumes nothing from
		// the right side), the decision is left at true — stage B will
		// rebuild and the standard prune will collapse it the same way.
		if (right.type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return;
		}
		if (right.children.empty() ||
		    right.children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return;
		}
		auto &agg = right.children[0]->Cast<LogicalAggregate>();
		if (agg.groups.size() > AggPushdown::kGroupByNum) {
			decision.push_into_child = false;
		}
	};
	walk(plan_copy);
	D_ASSERT(walk_join_id == tree.agg_pushdown_decisions.size());
}

unique_ptr<LogicalOperator>
AggregatePushdownOuter::ReplaceRootMinMax(unique_ptr<LogicalOperator> plan,
                                          const vector<AggregatePushdownRewriter::AggColumnInfo> &agg_columns) {
	if (tree.root_aggregation.type != OuterYanAggregationType::MINMAX) {
		return plan;
	}
	if (!plan || plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.empty()) {
		return plan;
	}
	auto &root_proj = plan->Cast<LogicalProjection>();
	if (root_proj.children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return plan;
	}
	auto &root_agg = root_proj.children[0]->Cast<LogicalAggregate>();
	if (root_agg.expressions.empty() || root_agg.children.empty()) {
		return plan;
	}

	auto &agg_child = *root_agg.children[0];
	agg_child.ResolveOperatorTypes();
	const auto child_bindings = agg_child.GetColumnBindings();
	const auto child_types = agg_child.types;

	// For every tracked min/max target, find its post-push binding in the
	// operator immediately below the root aggregate and emit a fresh
	// min/max on it. Preserves order: the reference rebuilds the root
	// aggregate's expression list in `agg_columns` order, so each new
	// expression slot ends up matching the original projection slot.
	vector<unique_ptr<Expression>> new_agg_expressions;
	for (const auto &agg_col : agg_columns) {
		if (agg_col.function_name != "min" && agg_col.function_name != "max") {
			continue;
		}
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			if (child_bindings[i] != agg_col.current_binding) {
				continue;
			}
			vector<unique_ptr<Expression>> args;
			args.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
			AggregateFunction agg_func = LookupAggregate(optimizer.context, agg_col.function_name, child_types[i]);
			FunctionBinder function_binder(optimizer.context);
			auto expr = function_binder.BindAggregateFunction(agg_func, std::move(args), nullptr,
			                                                 AggregateType::NON_DISTINCT);
			new_agg_expressions.push_back(std::move(expr));
			break;
		}
	}
	if (new_agg_expressions.empty()) {
		// No min/max push reached the root — leave unchanged.
		return plan;
	}
	root_agg.expressions = std::move(new_agg_expressions);

	// Rebuild the root projection to read the new aggregate outputs. The
	// reference replaces the whole expression list with just the aggregate
	// outputs (each aliased back to the original min/max alias is handled
	// by `LogicalProjection::ResolveOperatorTypes`).
	vector<unique_ptr<Expression>> new_proj_exprs;
	for (idx_t i = 0; i < root_agg.expressions.size(); i++) {
		const auto agg_binding = ColumnBinding(root_agg.aggregate_index, ProjectionIndex(i));
		new_proj_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(root_agg.expressions[i]->return_type, agg_binding));
	}
	root_proj.expressions = std::move(new_proj_exprs);
	root_agg.ResolveOperatorTypes();
	root_proj.ResolveOperatorTypes();
	return plan;
}

unique_ptr<LogicalOperator>
AggregatePushdownOuter::ReplaceRootSum(unique_ptr<LogicalOperator> plan,
                                       const vector<AggregatePushdownRewriter::AggColumnInfo> &agg_columns) {
	if (tree.root_aggregation.type != OuterYanAggregationType::SUM) {
		return plan;
	}
	if (!plan || plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.empty()) {
		return plan;
	}
	auto &root_proj = plan->Cast<LogicalProjection>();
	if (root_proj.children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return plan;
	}
	auto &root_agg = root_proj.children[0]->Cast<LogicalAggregate>();
	if (root_agg.expressions.empty() || root_agg.children.empty()) {
		return plan;
	}

	auto &agg_child = *root_agg.children[0];
	agg_child.ResolveOperatorTypes();
	const auto child_bindings = agg_child.GetColumnBindings();
	const auto child_types = agg_child.types;

	// For every sum target, find its post-push binding in the operator
	// immediately below the root aggregate and emit `sum(binding)`. Each
	// new expression slot lines up with the original projection slot.
	vector<unique_ptr<Expression>> new_agg_expressions;
	for (const auto &agg_col : agg_columns) {
		if (agg_col.function_name != "sum") {
			continue;
		}
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			if (child_bindings[i] != agg_col.current_binding) {
				continue;
			}
			vector<unique_ptr<Expression>> args;
			args.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
			auto sum_function = GetSumAggregate(child_types[i].InternalType());
			if (sum_function.name.empty()) {
				sum_function.name = "sum";
			}
			FunctionBinder function_binder(optimizer.context);
			auto sum_expr = function_binder.BindAggregateFunction(sum_function, std::move(args), nullptr,
			                                                     AggregateType::NON_DISTINCT);
			new_agg_expressions.push_back(std::move(sum_expr));
			break;
		}
	}
	if (new_agg_expressions.empty()) {
		// No sum target reached the root via the push — leave unchanged.
		return plan;
	}
	root_agg.expressions = std::move(new_agg_expressions);

	vector<unique_ptr<Expression>> new_proj_exprs;
	for (idx_t i = 0; i < root_agg.expressions.size(); i++) {
		const auto agg_binding = ColumnBinding(root_agg.aggregate_index, ProjectionIndex(i));
		new_proj_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(root_agg.expressions[i]->return_type, agg_binding));
	}
	root_proj.expressions = std::move(new_proj_exprs);
	root_agg.ResolveOperatorTypes();
	root_proj.ResolveOperatorTypes();
	return plan;
}

unique_ptr<LogicalOperator>
AggregatePushdownOuter::ReplaceRootCountWithSum(unique_ptr<LogicalOperator> plan) {
	if (tree.root_aggregation.type != OuterYanAggregationType::COUNT_STAR) {
		// MINMAX is handled by `ReplaceRootMinMax`. SUM and SELECT_DISTINCT
		// remain to be wired in a follow-on (Slice 2C-cont / Slice 2D-cont).
		return plan;
	}
	if (!plan || plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.empty()) {
		return plan;
	}
	auto &root_proj = plan->Cast<LogicalProjection>();
	if (root_proj.children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return plan;
	}
	auto &root_agg = root_proj.children[0]->Cast<LogicalAggregate>();
	if (root_agg.expressions.empty() || root_agg.children.empty()) {
		return plan;
	}

	// Search the operator immediately below the root aggregate for an
	// `annot` projection emitted by the rewriter. If none exists, no push
	// reached the root and `count(*)` is correct as-is.
	ColumnBinding annot_binding;
	LogicalType annot_type;
	auto &agg_child = *root_agg.children[0];
	if (agg_child.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return plan;
	}
	auto &agg_child_proj = agg_child.Cast<LogicalProjection>();
	bool found_annot = false;
	const auto child_bindings = agg_child.GetColumnBindings();
	agg_child.ResolveOperatorTypes();
	for (idx_t i = 0; i < agg_child_proj.expressions.size(); i++) {
		if (agg_child_proj.expressions[i]->alias == "annot") {
			annot_binding = child_bindings[i];
			annot_type = agg_child.types[i];
			found_annot = true;
			break;
		}
	}
	if (!found_annot) {
		return plan;
	}

	// Rebuild the root aggregate's first expression as `sum(annot)`.
	vector<unique_ptr<Expression>> sum_args;
	sum_args.push_back(make_uniq<BoundColumnRefExpression>(annot_type, annot_binding));
	auto sum_function = GetSumAggregate(annot_type.InternalType());
	if (sum_function.name.empty()) {
		sum_function.name = "sum";
	}
	FunctionBinder function_binder(optimizer.context);
	auto sum_expr = function_binder.BindAggregateFunction(sum_function, std::move(sum_args), nullptr,
	                                                     AggregateType::NON_DISTINCT);
	root_agg.expressions[0] = std::move(sum_expr);

	// Adjust the root projection's first expression to read the new
	// aggregate output. The reference replaces the whole expression list
	// with just the annot ref; we mirror that — `count(*)` queries return
	// a single column.
	const auto agg_binding = ColumnBinding(root_agg.aggregate_index, ProjectionIndex(0));
	vector<unique_ptr<Expression>> new_proj_exprs;
	new_proj_exprs.push_back(
	    make_uniq<BoundColumnRefExpression>(root_agg.expressions[0]->return_type, agg_binding));
	root_proj.expressions = std::move(new_proj_exprs);
	root_agg.ResolveOperatorTypes();
	root_proj.ResolveOperatorTypes();
	return plan;
}

unique_ptr<LogicalOperator> AggregatePushdownOuter::Optimize(unique_ptr<LogicalOperator> plan) {
	if (tree.root_aggregation.type == OuterYanAggregationType::NONE) {
		return plan;
	}
	if (tree.agg_pushdown_decisions.empty()) {
		return plan;
	}
	// COUNT_STAR / MINMAX / SUM / SELECT_DISTINCT supported. OTHER and
	// NONE pass through unchanged.
	if (tree.root_aggregation.type != OuterYanAggregationType::COUNT_STAR &&
	    tree.root_aggregation.type != OuterYanAggregationType::MINMAX &&
	    tree.root_aggregation.type != OuterYanAggregationType::SUM &&
	    tree.root_aggregation.type != OuterYanAggregationType::SELECT_DISTINCT) {
		return plan;
	}

	// Stage A — speculative refinement on a throwaway deep copy.
	auto plan_copy = plan->Copy(optimizer.context);
	{
		AggregatePushdownRewriter stage_a(optimizer.binder, optimizer.context, tree);
		plan_copy = stage_a.Rewrite(std::move(plan_copy));
		D_ASSERT(stage_a.JoinsVisited() == tree.agg_pushdown_decisions.size());
	}

	// Alternating prune cycle — fixed-N loop matching the reference's
	// `Optimizer::DetermineMaxHeight`-bounded iteration. Each round of
	// `RemoveUnusedColumns` exposes new prunable groups for
	// `PruneInsertedAggregates`, and vice versa.
	const idx_t max_height = DetermineMaxHeight(*plan_copy);
	for (idx_t i = 0; i < max_height; i++) {
		RemoveUnusedColumns unused(optimizer);
		unused.VisitOperator(plan_copy);
		plan_copy = PruneInsertedAggregates(std::move(plan_copy));
	}

	Prune(*plan_copy);
	plan_copy.reset();

	// Stage B — real insertion on the original plan using the refined
	// decisions. The rewriter's `agg_columns` (final state) is captured
	// after this stage so `ReplaceRootMinMax` can find each min/max
	// target's post-push binding.
	vector<AggregatePushdownRewriter::AggColumnInfo> final_agg_columns;
	{
		AggregatePushdownRewriter stage_b(optimizer.binder, optimizer.context, tree);
		plan = stage_b.Rewrite(std::move(plan));
		D_ASSERT(stage_b.JoinsVisited() == tree.agg_pushdown_decisions.size());
		final_agg_columns = stage_b.AggColumns();
	}

	// Final prune cycle on the real plan — mirrors the reference's
	// `RemoveUnusedColumns` + `PruneAggregationWithProjectionMap` call
	// after `ApplyAgg`. One pass each is sufficient at this point because
	// the bindings are already consistent.
	const idx_t final_height = DetermineMaxHeight(*plan);
	for (idx_t i = 0; i < final_height; i++) {
		RemoveUnusedColumns unused(optimizer);
		unused.VisitOperator(plan);
		plan = PruneInsertedAggregates(std::move(plan));
	}

	// Rewire the root aggregate to consume the pushed-down annot columns.
	// Each branch is a no-op when the type does not match.
	plan = ReplaceRootCountWithSum(std::move(plan));
	plan = ReplaceRootMinMax(std::move(plan), final_agg_columns);
	plan = ReplaceRootSum(std::move(plan), final_agg_columns);

	return plan;
}

} // namespace duckdb
