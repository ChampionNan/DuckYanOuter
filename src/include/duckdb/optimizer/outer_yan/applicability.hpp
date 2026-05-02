//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/applicability.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class LogicalComparisonJoin;

//! Identifier for a base relation in the OuterYan join graph.
using RelationId = idx_t;

//! One edge of the join graph: a single join condition between two relations.
struct JoinGraphEdge {
	RelationId left;
	RelationId right;
	const LogicalComparisonJoin *join;
	idx_t condition_index;
};

//! Result of the applicability check at the entrance of OuterYanPre.
struct ApplicabilityResult {
	bool applicable = false;
	//! Diagnostic string, exposed via EXPLAIN when not applicable.
	string reason;

	//! Populated only when applicable == true.
	vector<const LogicalOperator *> relations;
	unordered_map<idx_t, RelationId> table_index_to_relation;
	vector<JoinGraphEdge> edges;
	bool has_root_aggregate = false;
};

//! Applicability gate for OuterYan. Accepts iff:
//!   - all joins are inner or LEFT/RIGHT/FULL outer (and at least one outer),
//!   - join predicates may be equi or theta, but must be null-rejecting
//!     and reference exactly two base relations,
//!   - no semi/anti/mark joins, set ops, correlated subqueries, or window
//!     operators appear inside the skeleton,
//!   - aggregation, if present, sits only at the root above the joins,
//!   - the join graph (relations as nodes, conditions as edges) is acyclic.
//!
//! Implemented as a LogicalOperatorVisitor that walks the skeleton top-down,
//! collecting leaves and edges and short-circuiting on the first violation.
class OuterYanApplicability : public LogicalOperatorVisitor {
public:
	OuterYanApplicability();

	//! Entry point. Runs the visitor and returns the populated result.
	static ApplicabilityResult Check(LogicalOperator &plan);

	void VisitOperator(LogicalOperator &op) override;

private:
	void VisitComparisonJoin(LogicalComparisonJoin &join);
	void RegisterLeaf(LogicalOperator &leaf);
	void Reject(string reason);

	// Walk past projection/filter (and a single root aggregate) to find
	// the top of the join skeleton.
	LogicalOperator &SkeletonRoot(LogicalOperator &plan);

	// Union-find helpers for graph-cycle detection.
	RelationId Find(RelationId x);
	bool UnionRelations(RelationId a, RelationId b);

private:
	ApplicabilityResult result;
	vector<RelationId> uf_parent;
	unordered_set<uint64_t> seen_pairs;
	bool has_outer = false;
	bool rejected = false;
};

} // namespace duckdb
