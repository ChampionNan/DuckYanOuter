//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/evaluation_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"

namespace duckdb {

//! OrderFix — verifies / enforces the required evaluation order on the OJT.
//!
//! Two regimes (per project plan §5):
//!
//!   - **Full query** (`SELECT *`): any post-order is correct; cost-driven
//!     ordering chosen by OuterYanDP is final. This pass is a no-op in this
//!     mode aside from sanity checks.
//!   - **Aggregation query**: evaluation order must exactly match the OJT
//!     post-order with the output relation as the root (forced by
//!     `OuterYanPre`'s root marker). When multiple post-orders are valid
//!     (multi-way joins), tie-break by cost.
class OrderFix {
public:
	void Apply(OrderedJoinTree &ojt, bool is_aggregation_query);
};

} // namespace duckdb
