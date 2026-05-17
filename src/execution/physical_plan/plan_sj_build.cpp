//===----------------------------------------------------------------------===//
// CreatePlan(LogicalSJBuild&) — OuterYan semi-join build lowering.
//
// IMPLEMENTATION REFERENCES — cross-check against these when editing.
//
// Primary references (raw-pointer-cache pattern + dual CreatePlan /
// CreatePlanFromRelated entry points):
//   - embryo-labs/dynamic-predicate-transfer:
//       src/execution/physical_plan/plan_create_bf.cpp
//     (Local /tmp/dpt_plan_create_bf.cpp during the design session.)
//     The structure of CreatePlanFromRelated and CreatePlan + the
//     `op.physical = static_cast<PhysicalCreateBF*>(&create_bf)` cache
//     pattern come from this file.
//   - embryo-labs/Robust-Predicate-Transfer:
//       src/execution/physical_plan/plan_create_bf.cpp
//     (Local /tmp/rpt_plan_create_bf.cpp.) Older variant using raw new/
//     unique_ptr management — we use DuckDB's new arena-allocated Make<>
//     instead.
//
// DuckDB-side patterns referenced:
//   - The `Make<PhysicalT>(args...)` arena allocation idiom — see
//     plan_filter.cpp (CreatePlan(LogicalFilter&)).
//   - `CreatePlan(*op.children[0])` recursion — DuckDB standard for any
//     pass-through-style physical operator.
//
// WHY THE CACHE (`op.physical`): a single LogicalSJBuild produces shared
// filters used by multiple LogicalSJProbes elsewhere in the plan. Each
// LogicalSJProbe's CreatePlan calls CreatePlanFromRelated on each
// related_sj_build entry to resolve the physical pointer. Without the
// cache we'd construct many physical SJBuilds for one logical — wrong
// (multiple materialisations of the same data flowing through different
// sinks) and wasteful. The cache makes the two entry points idempotent.
//
// ORIGINAL TO OuterYan (not in DPT):
//   - `ResolveBuildColIds` below. DPT's FilterPlan carries `bound_cols_*`
//     pre-resolved by the optimizer; we instead carry only ColumnBindings
//     on SemiJoinFilter and resolve to column indices here at lowering
//     time. Trade-off: simpler optimizer pass, slightly more work at
//     plan-gen time. The cost is negligible — both lists are tiny.
//===----------------------------------------------------------------------===//

#include "duckdb/execution/operator/join/physical_sj_build.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_sj_build.hpp"

namespace duckdb {

// Map each filter's BuildBindings to column indices within the child's
// output schema. Both lists are short (≤ a few entries each), so the
// quadratic-looking nested loop is acceptable. Original to OuterYan;
// see the file-header note above for why we don't carry pre-resolved
// column ids on SemiJoinFilter like DPT's FilterPlan does.
// Column-id resolution lives on PhysicalSJBuild as a static method (see
// physical_sj_build.cpp: ResolveBuildColIds) so all SJ-build plan-gen
// logic is co-located with the operator class.

// Entry point #1 — called from CreatePlan(LogicalSJProbe&) when wiring up
// related_sj_build. Reference: DPT plan_create_bf.cpp lines 8–17
// (`CreatePlanFromRelated`). On cache hit: return the cached pointer.
// On cache miss: build the child plan, construct PhysicalSJBuild via Make,
// cache the pointer, attach child as the build's only child.
PhysicalSJBuild *PhysicalPlanGenerator::CreatePlanFromRelated(LogicalSJBuild &op) {
	if (op.physical) {
		return op.physical;
	}
	auto &child_plan = CreatePlan(*op.children[0]);
	auto build_col_ids = ResolveBuildColIds(op);
	auto &sj_build = Make<PhysicalSJBuild>(child_plan.GetTypes(), std::move(op.sj_to_create),
	                                       std::move(build_col_ids), op.estimated_cardinality);
	sj_build.children.push_back(child_plan);
	op.physical = static_cast<PhysicalSJBuild *>(&sj_build);
	return op.physical;
}

// Entry point #2 — called from the main CreatePlan(LogicalOperator&)
// dispatch in physical_plan_generator.cpp when the recursion reaches a
// LogicalSJBuild in the plan tree. Reference: DPT plan_create_bf.cpp
// lines 19–29 (`CreatePlan(LogicalCreateBF&)`). Same structure as
// CreatePlanFromRelated above, but returns by reference instead of
// pointer (DuckDB CreatePlan convention).
PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalSJBuild &op) {
	if (op.physical) {
		return *op.physical;
	}
	auto &child_plan = CreatePlan(*op.children[0]);
	auto build_col_ids = ResolveBuildColIds(op);
	auto &sj_build = Make<PhysicalSJBuild>(child_plan.GetTypes(), std::move(op.sj_to_create),
	                                       std::move(build_col_ids), op.estimated_cardinality);
	op.physical = static_cast<PhysicalSJBuild *>(&sj_build);
	sj_build.children.push_back(child_plan);
	return sj_build;
}

} // namespace duckdb
