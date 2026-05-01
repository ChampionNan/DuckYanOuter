#include "duckdb/optimizer/outer_yan/aggregate_pushdown_outer.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! Internal LogicalOperatorVisitor that applies a single pushdown rule.
//!
//! Mirrors DuckDB's `AggregateFunctionRewriterInternal` pattern: the public
//! orchestrator (AggregatePushdownOuter) stays narrow; the traversal logic
//! lives here, hidden inside the translation unit.
class AggregatePushdownOuterInternal : public LogicalOperatorVisitor {
public:
	AggregatePushdownOuterInternal(Binder &binder, ClientContext &context) : binder(binder), context(context) {
	}

	void Optimize(unique_ptr<LogicalOperator> &plan) {
		VisitOperator(*plan);
	}

protected:
	void VisitOperator(LogicalOperator &op) override {
		// TODO: dispatch on op.type — LogicalAggregate, LogicalComparisonJoin
		// (inner + outer), LogicalProjection, LogicalFilter, LogicalGet —
		// applying the corresponding pushdown rewrite per the DuckYanPlus
		// semantics, extended for outer joins.
		VisitOperatorChildren(op);
	}

private:
	Binder &binder;
	ClientContext &context;
};

// ---------------------------------------------------------------------------
// Public orchestrator
// ---------------------------------------------------------------------------

AggregatePushdownOuter::AggregatePushdownOuter(Binder &binder_p, ClientContext &context_p)
    : binder(binder_p), context(context_p) {
}

unique_ptr<LogicalOperator> AggregatePushdownOuter::Optimize(unique_ptr<LogicalOperator> plan) {
	AggregatePushdownOuterInternal rewriter(binder, context);
	rewriter.Optimize(plan);
	return plan;
}

} // namespace duckdb
