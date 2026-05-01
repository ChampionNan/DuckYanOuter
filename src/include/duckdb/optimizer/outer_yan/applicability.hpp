//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/applicability.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Result of the applicability check at the entrance of OuterYanPre.
struct ApplicabilityResult {
	bool applicable = false;
	//! Diagnostic string, exposed via EXPLAIN when not applicable.
	string reason;
};

//! Applicability gate for OuterYan. Accepts iff:
//!   - all joins are inner or LEFT/RIGHT/FULL outer,
//!   - join predicates may be equi-join or theta-join, but must be
//!     null-rejecting and reference exactly two relations,
//!   - no semi/anti/mark joins, set ops, or correlated subqueries appear,
//!   - the join graph is acyclic (GYO ear-removal succeeds).
//!
//! When the check fails, the OuterYan path is bypassed and the existing
//! JOIN_ORDER pass runs as before.
class OuterYanApplicability {
public:
	static ApplicabilityResult Check(const LogicalOperator &plan);
};

} // namespace duckdb
