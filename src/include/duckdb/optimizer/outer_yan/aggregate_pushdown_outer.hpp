//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/aggregate_pushdown_outer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class Optimizer;

//! AggregatePushdownOuter — separate optimizer pass running after OuterYanPost.
//!
//! Pushes group-by aggregations below joins, including outer joins, to feed
//! the Yannakakis-style reduction with already-aggregated build sides.
//!
//! Reference (semantics): `aggregation_pushdown.{hpp,cpp}` in
//! `ChampionNan/DuckDBYanPlus`. The reference implementation is structurally
//! tangled because rewriting and traversal are intertwined; this version
//! follows DuckDB's own `AggregateFunctionRewriter` pattern instead:
//!
//!   - Public class (this one) is a thin orchestrator with a list of
//!     pushdown *rules*, each rule a small strategy object.
//!   - The actual traversal lives in an internal
//!     `LogicalOperatorVisitor`-derived class defined inside the .cpp,
//!     which applies one rule per pass — exactly the structure used by
//!     `AggregateFunctionRewriterInternal` in DuckDB's tree.
//!
//! This keeps the public header narrow and reuses DuckDB's idioms instead
//! of inventing a new traversal strategy.
//!
//! Non-decomposable aggregates (e.g. `COUNT(DISTINCT)`) are left in place.
class AggregatePushdownOuter {
public:
	AggregatePushdownOuter(Binder &binder, ClientContext &context);

	//! Entry point — drives the visitor traversal and applies rewrites in
	//! place. Returns the (possibly replaced) root.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	Binder &binder;
	ClientContext &context;
};

} // namespace duckdb
