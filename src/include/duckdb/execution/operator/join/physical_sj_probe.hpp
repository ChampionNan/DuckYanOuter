//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_sj_probe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/execution/operator/join/physical_sj_build.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/outer_yan/semi_join_filter.hpp"

namespace duckdb {

//! PhysicalSJProbe — probe-side of an OuterYan semi-join reducer pair.
//!
//! Filters input chunks against the shared SemiJoinFilter instances in
//! `sj_to_use`, populated by the paired PhysicalSJBuild operators tracked
//! in `related_sj_build`. Each filter applies in order; the surviving
//! row set is narrowed step by step.
//!
//! Lifecycle: operator-in-the-middle. Pipeline membership is the probe
//! pipeline; the build pipeline is set up as a dependency via
//! `related_sj_build[i]->BuildPipelinesFromRelated(...)` in BuildPipelines.
class PhysicalSJProbe : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SJ_PROBE;

public:
	PhysicalSJProbe(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                vector<shared_ptr<SemiJoinFilter>> sj_to_use,
	                vector<vector<idx_t>> probe_col_ids_per_filter,
	                idx_t estimated_cardinality);

	//! Shared filters this operator probes against. Same shared_ptr instances
	//! are populated by the paired PhysicalSJBuild operators tracked in
	//! `related_sj_build` (index-aligned).
	vector<shared_ptr<SemiJoinFilter>> sj_to_use;

	//! For each filter in `sj_to_use`, the list of column indices within
	//! input chunks corresponding to that filter's ProbeBindings. Computed
	//! at plan-generation time.
	vector<vector<idx_t>> probe_col_ids_per_filter;

	//! Back-pointers to upstream PhysicalSJBuild operators (one per filter).
	//! Set during physical plan generation from LogicalSJProbe::related_sj_build.
	vector<PhysicalSJBuild *> related_sj_build;

public:
	// Operator interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::INSERTION_ORDER;
	}
	bool ParallelOperator() const override {
		return true;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
};

} // namespace duckdb
