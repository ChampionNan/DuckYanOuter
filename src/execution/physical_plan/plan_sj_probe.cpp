//===----------------------------------------------------------------------===//
// CreatePlan(LogicalSJProbe&) — OuterYan semi-join probe lowering.
//
// IMPLEMENTATION REFERENCES — cross-check against these when editing.
//
// Primary references:
//   - embryo-labs/dynamic-predicate-transfer:
//       src/execution/physical_plan/plan_use_bf.cpp
//     (Local /tmp/dpt_plan_use_bf.cpp during the design session.)
//     The pattern of (1) recurse into child, (2) resolve each related
//     build via CreatePlanFromRelated, (3) construct PhysicalSJProbe via
//     Make, (4) push back the child, comes from this file.
//   - embryo-labs/Robust-Predicate-Transfer:
//       src/execution/physical_plan/plan_use_bf.cpp
//     (Local /tmp/rpt_plan_use_bf.cpp.) Older variant with the same shape.
//
// DuckDB-side patterns referenced:
//   - `Make<PhysicalT>` — arena allocation, see plan_filter.cpp.
//   - `CreatePlan(*op.children[0])` — DuckDB standard recursive lowering.
//
// ORIGINAL TO OuterYan (not in DPT):
//   - `ResolveProbeColIds`. Same rationale as in plan_sj_build.cpp —
//     OuterYan's SemiJoinFilter carries ColumnBindings, not pre-resolved
//     bound_cols_*. We resolve them here at lowering time using the
//     child operator's GetColumnBindings().
//   - Multi-filter per probe. DPT's PhysicalUseBF carries one
//     BloomFilterUsage; ours can carry many SemiJoinFilters and resolves
//     each one to its own probe_col_ids in the resulting probe op.
//===----------------------------------------------------------------------===//

#include "duckdb/execution/operator/join/physical_sj_build.hpp"
#include "duckdb/execution/operator/join/physical_sj_probe.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_sj_build.hpp"
#include "duckdb/planner/operator/logical_sj_probe.hpp"

namespace duckdb {

// Map each filter's ProbeBindings to column indices within the child's
// output schema. Mirrors ResolveBuildColIds in plan_sj_build.cpp.
static vector<vector<idx_t>> ResolveProbeColIds(LogicalSJProbe &op) {
	auto child_bindings = op.children[0]->GetColumnBindings();
	vector<vector<idx_t>> result;
	result.reserve(op.sj_to_use.size());
	for (auto &filter : op.sj_to_use) {
		vector<idx_t> ids;
		ids.reserve(filter->ProbeBindings().size());
		for (auto &binding : filter->ProbeBindings()) {
			bool found = false;
			for (idx_t i = 0; i < child_bindings.size(); i++) {
				if (child_bindings[i] == binding) {
					ids.push_back(i);
					found = true;
					break;
				}
			}
			if (!found) {
				throw InternalException(
				    "plan_sj_probe: probe binding (%llu, %llu) not found in child's column bindings",
				    static_cast<unsigned long long>(binding.table_index.index),
				    static_cast<unsigned long long>(binding.column_index.GetIndexUnsafe()));
			}
		}
		result.emplace_back(std::move(ids));
	}
	return result;
}

// Reference: DPT plan_use_bf.cpp `CreatePlan(LogicalUseBF&)`. Same shape:
//   1. CreatePlan(child) → child PhysicalOperator&.
//   2. ResolveProbeColIds from child's GetColumnBindings.
//   3. Construct PhysicalSJProbe via Make<>.
//   4. Wire related_sj_build via CreatePlanFromRelated for each entry.
//      (Order in vector matches op.sj_to_use, so probe[i] uses build[i]
//      to resolve its filter's payload at probe time.)
//   5. Push back child.
PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalSJProbe &op) {
	auto &child_plan = CreatePlan(*op.children[0]);
	auto probe_col_ids = ResolveProbeColIds(op);

	// Build the related_sj_build pointer list BEFORE constructing the
	// PhysicalSJProbe (it's a constructor argument-adjacent field). Each
	// CreatePlanFromRelated call may construct the related PhysicalSJBuild
	// on first encounter; subsequent encounters reuse the cached pointer.
	vector<PhysicalSJBuild *> related;
	related.reserve(op.related_sj_build.size());
	for (auto *logical_build : op.related_sj_build) {
		if (!logical_build) {
			throw InternalException("plan_sj_probe: null related_sj_build entry");
		}
		related.emplace_back(CreatePlanFromRelated(*logical_build));
	}

	auto &sj_probe = Make<PhysicalSJProbe>(child_plan.GetTypes(), std::move(op.sj_to_use), std::move(probe_col_ids),
	                                       op.estimated_cardinality);
	auto &probe_typed = sj_probe.Cast<PhysicalSJProbe>();
	probe_typed.related_sj_build = std::move(related);
	sj_probe.children.push_back(child_plan);
	return sj_probe;
}

} // namespace duckdb
