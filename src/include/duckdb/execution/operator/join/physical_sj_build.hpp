//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_sj_build.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"

namespace duckdb {

//! PhysicalSJBuild — sink-side of an OuterYan semi-join reducer pair.
//!
//! Materialises the shared HashFilter instances in `sj_to_create`. The same
//! shared_ptr instances appear in PhysicalSJProbe::sj_to_use for the paired
//! probe operators — the shared object is the binding (RPT pattern).
//!
//! Pass-through on the data path; effect is build-side filter population.
//!
//! TODO: full Sink lifecycle (GetGlobalSinkState → GetLocalSinkState →
//! Sink → Combine → Finalize). Stub for scaffolding.
class PhysicalSJBuild : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SJ_BUILD;

public:
	PhysicalSJBuild(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                vector<shared_ptr<HashFilter>> sj_to_create, idx_t estimated_cardinality);

	vector<shared_ptr<HashFilter>> sj_to_create;

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
