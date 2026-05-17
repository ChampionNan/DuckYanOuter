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
#include "duckdb/optimizer/outer_yan/semi_join_filter.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//! PhysicalSJBuild — sink+source of an OuterYan semi-join reducer pair.
//!
//! Sink role: materialise incoming chunks into a global ColumnDataCollection.
//! At Finalize, build one filter per entry in `sj_to_create`. Each filter
//! tries the bitmap path first (KeyBitmap::TryBuild) and falls back to a
//! hash filter if the bitmap is rejected (out-of-range, type unsupported,
//! pragma budget exceeded). See plan 3.
//!
//! Source role: replay the materialised build data downstream, so the rest
//! of the plan continues to see the build relation. Without this, the build
//! subtree would be a dead end and downstream operators would need a
//! separate scan of the same data.
//!
//! Pipeline shape (mirrors RPT/DPT's PhysicalCreateBF):
//!   - The probe pipeline declares an AddDependency on this operator's
//!     build pipeline (set up via BuildPipelinesFromRelated). The probe
//!     therefore cannot run until Finalize has completed and all filters
//!     have transitioned out of UNDECIDED.
class PhysicalSJBuild : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SJ_BUILD;

public:
	PhysicalSJBuild(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                vector<shared_ptr<SemiJoinFilter>> sj_to_create,
	                vector<vector<idx_t>> build_col_ids_per_filter,
	                idx_t estimated_cardinality);

	//! Shared filters this operator populates. Same shared_ptr instances are
	//! held by the paired LogicalSJProbe / PhysicalSJProbe in `sj_to_use`.
	vector<shared_ptr<SemiJoinFilter>> sj_to_create;

	//! For each filter in `sj_to_create`, the list of column indices within
	//! the operator's input chunks that correspond to that filter's
	//! BuildBindings. Computed at plan-generation time from
	//! LogicalSJBuild::children[0]->GetColumnBindings().
	vector<vector<idx_t>> build_col_ids_per_filter;

	//! Build-side child meta-pipeline, populated by BuildPipelines /
	//! BuildPipelinesFromRelated. Multiple downstream PhysicalSJProbe ops
	//! reuse this single instance via AddDependency.
	shared_ptr<Pipeline> this_pipeline;

public:
	// Sink interface
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
	// Source interface — re-emits materialised build data so downstream ops
	// can continue without re-scanning the build subtree.
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
	                         OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

public:
	// Pipeline wiring.
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	//! Called by PhysicalSJProbe::BuildPipelines. Establishes the probe→build
	//! dependency. Idempotent across multiple probes sharing this build.
	void BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline);

public:
	//! 16 MiB default. Large enough for PK-FK joins on dense ID columns up
	//! to ~128M-wide ranges.
	static constexpr idx_t kDefaultBitmapBudgetBytes = 1ULL << 24;
	//! Per-session SET VARIABLE knob name. Read via ClientConfig::user_variables.
	static constexpr const char *kBitmapBudgetVar = "outer_yan_bitmap_max_bytes";

	//! Per-filter bitmap byte budget. Reads
	//!   SET VARIABLE outer_yan_bitmap_max_bytes = N;
	//! from ClientConfig::user_variables; falls back to kDefaultBitmapBudgetBytes
	//! when unset. Returning 0 forces the HashFilter fallback for all filters
	//! in this session.
	static idx_t GetBitmapBudget(ClientContext &context);

	//! Resolve each filter's BuildBindings to column indices within the
	//! child operator's output schema. Called by
	//! PhysicalPlanGenerator::CreatePlan(LogicalSJBuild&) and
	//! ::CreatePlanFromRelated when constructing this op. Public static
	//! so plan_sj_build.cpp can call it without touching internals.
	static vector<vector<idx_t>> ResolveBuildColIds(class LogicalSJBuild &op);
};

} // namespace duckdb
