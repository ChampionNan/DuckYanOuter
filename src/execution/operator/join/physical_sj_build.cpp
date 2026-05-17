//===----------------------------------------------------------------------===//
// PhysicalSJBuild — OuterYan semi-join build operator.
//
// IMPLEMENTATION REFERENCES — please cross-check against these when editing.
//
// Primary references (Sink + Source dual role, child meta-pipeline pattern):
//   - embryo-labs/dynamic-predicate-transfer:
//       src/execution/operator/persistent/physical_create_bf.cpp
//     The Sink/Combine/Finalize/Source layout + BuildPipelines +
//     BuildPipelinesFromRelated come directly from this file's structure.
//     (Local /tmp/dpt_physical_create_bf.cpp during the design session.)
//   - embryo-labs/Robust-Predicate-Transfer:
//       src/execution/operator/persistent/physical_create_bf.cpp
//     Older variant; same shape, fewer runtime-optimisation knobs.
//
// What we INTENTIONALLY DO NOT carry over from DPT (per plan 3 non-goals):
//   - GiveUpBFCreation runtime selectivity / OOM guards
//   - is_successful / can_stop mutable flags
//   - min_max_to_create / DynamicTableFilterSet pushdown
//   - TemporaryMemoryManager registration (deferred — open question 3)
//   - Parallel finalize event (single-threaded loop over filters is fine
//     for v1; the parallel work is in the Sink stage)
//
// What we ADD vs DPT:
//   - Bitmap-first / HashFilter-fallback decision inside Finalize
//     (KeyBitmap::TryBuild → if nullptr fall back to HashFilter::Build).
//   - SemiJoinFilter carrier (instead of BloomFilter / HashFilter).
//
// DuckDB-side patterns referenced:
//   - PhysicalRecursiveCTE / PhysicalColumnDataScan — Sink+Source operators
//     that buffer and re-emit (we follow the same ColumnDataCollection idiom).
//   - PhysicalHashJoin — for build-side data materialisation patterns.
//===----------------------------------------------------------------------===//

#include "duckdb/execution/operator/join/physical_sj_build.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"
#include "duckdb/optimizer/outer_yan/key_bitmap.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/operator/logical_sj_build.hpp"

#include <mutex>

namespace duckdb {

// Per-session budget knob. Set via:
//   SET VARIABLE outer_yan_bitmap_max_bytes = 16777216;
// Read from ClientConfig::user_variables (DuckDB's case-insensitive map
// populated by SET VARIABLE) rather than registering a global
// ConfigurationOption — the latter would require touching generated
// settings infrastructure (settings.hpp / settings.cpp / config.cpp's
// internal_options table) and is out of scope for plan 3. Setting to 0
// forces the HashFilter fallback for all SemiJoinFilters in the session
// (escape hatch for benchmarks).
idx_t PhysicalSJBuild::GetBitmapBudget(ClientContext &context) {
	auto &cfg = ClientConfig::GetConfig(context);
	auto it = cfg.user_variables.find(kBitmapBudgetVar);
	if (it == cfg.user_variables.end() || it->second.IsNull()) {
		return kDefaultBitmapBudgetBytes;
	}
	try {
		auto v = it->second.DefaultCastAs(LogicalType::UBIGINT);
		return NumericCast<idx_t>(v.GetValue<uint64_t>());
	} catch (...) {
		throw InvalidInputException("SET VARIABLE %s must be a non-negative integer (got %s)", kBitmapBudgetVar,
		                            it->second.ToString());
	}
}

// Map each filter's BuildBindings to column indices within the child's
// output schema. Both lists are short (≤ a few entries each), so the
// quadratic-looking nested loop is fine. Called from
// PhysicalPlanGenerator::CreatePlan(LogicalSJBuild&) and
// ::CreatePlanFromRelated at plan-gen time.
vector<vector<idx_t>> PhysicalSJBuild::ResolveBuildColIds(LogicalSJBuild &op) {
	auto child_bindings = op.children[0]->GetColumnBindings();
	vector<vector<idx_t>> result;
	result.reserve(op.sj_to_create.size());
	for (auto &filter : op.sj_to_create) {
		vector<idx_t> ids;
		ids.reserve(filter->BuildBindings().size());
		for (auto &binding : filter->BuildBindings()) {
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
				    "PhysicalSJBuild::ResolveBuildColIds: build binding (%llu, %llu) not found in child's column bindings",
				    static_cast<unsigned long long>(binding.table_index.index),
				    static_cast<unsigned long long>(binding.column_index.GetIndexUnsafe()));
			}
		}
		result.emplace_back(std::move(ids));
	}
	return result;
}

PhysicalSJBuild::PhysicalSJBuild(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                 vector<shared_ptr<SemiJoinFilter>> sj_to_create_p,
                                 vector<vector<idx_t>> build_col_ids_per_filter_p,
                                 idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::SJ_BUILD, std::move(types), estimated_cardinality),
      sj_to_create(std::move(sj_to_create_p)),
      build_col_ids_per_filter(std::move(build_col_ids_per_filter_p)) {
	if (sj_to_create.size() != build_col_ids_per_filter.size()) {
		throw InternalException("PhysicalSJBuild: filter / build_col_ids count mismatch");
	}
}

// ===========================================================================
// Sink — materialise build-side data into a global ColumnDataCollection.
//
// Reference: DPT physical_create_bf.cpp `CreateBFGlobalSinkState` +
// `CreateBFLocalSinkState`. Same pattern: per-thread local
// ColumnDataCollection, folded into a global at Combine/Finalize. Differs
// only in that we don't carry `temporary_memory_state`, `min_max` aggregates,
// or `is_successful` (all non-goals per plan 3).
// ===========================================================================

class SJBuildGlobalSinkState : public GlobalSinkState {
public:
	SJBuildGlobalSinkState(ClientContext &context, const PhysicalSJBuild &op) : context(context), op(op) {
		total_data = make_uniq<ColumnDataCollection>(context, op.types);
	}

	ClientContext &context;
	const PhysicalSJBuild &op;

	// Lock for fold-in at Combine time; nothing else contends on this.
	std::mutex glock;
	// Per-thread local collections. Moved into total_data at Finalize.
	vector<unique_ptr<ColumnDataCollection>> local_data_collections;
	// Final merged collection — both the Sink output and the Source input.
	unique_ptr<ColumnDataCollection> total_data;
};

class SJBuildLocalSinkState : public LocalSinkState {
public:
	SJBuildLocalSinkState(ClientContext &context, const PhysicalSJBuild &op) {
		local_data = make_uniq<ColumnDataCollection>(context, op.types);
	}

	unique_ptr<ColumnDataCollection> local_data;
};

unique_ptr<GlobalSinkState> PhysicalSJBuild::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SJBuildGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalSJBuild::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<SJBuildLocalSinkState>(context.client, *this);
}

// Reference: DPT physical_create_bf.cpp `PhysicalCreateBF::Sink` (line ~262).
// Simplified — no GiveUpBFCreation, no min-max aggregation.
SinkResultType PhysicalSJBuild::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	(void)context;
	auto &lstate = input.local_state.Cast<SJBuildLocalSinkState>();
	lstate.local_data->Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

// Reference: DPT physical_create_bf.cpp `PhysicalCreateBF::Combine` (line ~283).
SinkCombineResultType PhysicalSJBuild::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	(void)context;
	auto &gstate = input.global_state.Cast<SJBuildGlobalSinkState>();
	auto &lstate = input.local_state.Cast<SJBuildLocalSinkState>();
	std::lock_guard<std::mutex> lock(gstate.glock);
	gstate.local_data_collections.emplace_back(std::move(lstate.local_data));
	return SinkCombineResultType::FINISHED;
}

// Reference: DPT physical_create_bf.cpp `PhysicalCreateBF::Finalize` (line ~403).
// Differences from DPT:
//   - No is_successful / give-up logic (plan-3 non-goal).
//   - No FinalizeMinMax / dynamic table filter (plan-3 non-goal).
//   - Bitmap-first + hash fallback per filter (plan-3 §"Build flow").
//   - Single-threaded over filters; we don't schedule a CreateBFFinalizeEvent
//     because the bitmap path's heavy work (bit-set + popcount) is already
//     vectorised inside KeyBitmap and the per-filter cost is bounded by
//     `outer_yan_bitmap_max_bytes`. Parallelisation across filters is a
//     future optimisation.
SinkFinalizeType PhysicalSJBuild::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const {
	(void)pipeline;
	(void)event;
	auto &gstate = input.global_state.Cast<SJBuildGlobalSinkState>();

	// Fold per-thread collections into the global one.
	for (auto &local : gstate.local_data_collections) {
		gstate.total_data->Combine(*local);
	}
	gstate.local_data_collections.clear();

	const auto budget = GetBitmapBudget(context);

	for (idx_t f = 0; f < sj_to_create.size(); f++) {
		auto &filter = sj_to_create[f];
		const auto &build_col_ids = build_col_ids_per_filter[f];

		// Resolve build column types from this operator's output schema.
		vector<LogicalType> build_col_types;
		build_col_types.reserve(build_col_ids.size());
		for (auto cid : build_col_ids) {
			if (cid >= types.size()) {
				throw InternalException("PhysicalSJBuild::Finalize: build_col_id out of range");
			}
			build_col_types.emplace_back(types[cid]);
		}

		// Try the bitmap path first. nullptr → fall back to HashFilter.
		auto bitmap = KeyBitmap::TryBuild(context, *gstate.total_data, build_col_ids, build_col_types, budget);
		if (bitmap) {
			filter->FinalizeAsBitmap(std::move(bitmap));
		} else {
			auto hf = make_shared_ptr<HashFilter>();
			// Stream materialised data through HashFilter::Build. We re-scan
			// total_data once per filter that falls back; for the common
			// case where most filters take the bitmap path, this is rare.
			DataChunk scan_chunk;
			gstate.total_data->InitializeScanChunk(scan_chunk);
			ColumnDataParallelScanState scan_state;
			ColumnDataLocalScanState local_scan_state;
			gstate.total_data->InitializeScan(scan_state);
			while (gstate.total_data->Scan(scan_state, local_scan_state, scan_chunk)) {
				hf->Build(scan_chunk, build_col_ids);
				scan_chunk.Reset();
			}
			hf->Finalize();
			filter->FinalizeAsHash(std::move(hf));
		}
	}
	return SinkFinalizeType::READY;
}

// ===========================================================================
// Source — re-emit materialised data so downstream operators read through
//           this operator without a second scan of the build subtree.
//
// Reference: DPT physical_create_bf.cpp `CreateBFGlobalSourceState` /
// `CreateBFLocalSourceState` / `GetData` (lines ~443–497). Same idiom: scan
// `gstate.total_data` (the post-Combine ColumnDataCollection). We don't
// implement DPT's external/partitioned scan paths since plan-3 doesn't
// register with TemporaryMemoryManager.
// ===========================================================================

class SJBuildGlobalSourceState : public GlobalSourceState {
public:
	explicit SJBuildGlobalSourceState(const ColumnDataCollection &collection) : data_collection(collection) {
		collection.InitializeScan(scan_state);
	}

	idx_t MaxThreads() override {
		return MaxValue<idx_t>(data_collection.ChunkCount(), 1);
	}

	const ColumnDataCollection &data_collection;
	ColumnDataParallelScanState scan_state;
};

class SJBuildLocalSourceState : public LocalSourceState {
public:
	ColumnDataLocalScanState local_scan_state;
};

unique_ptr<GlobalSourceState> PhysicalSJBuild::GetGlobalSourceState(ClientContext &context) const {
	(void)context;
	if (!sink_state) {
		throw InternalException("PhysicalSJBuild::GetGlobalSourceState: sink_state not set");
	}
	auto &gstate = sink_state->Cast<SJBuildGlobalSinkState>();
	return make_uniq<SJBuildGlobalSourceState>(*gstate.total_data);
}

unique_ptr<LocalSourceState> PhysicalSJBuild::GetLocalSourceState(ExecutionContext &context,
                                                                  GlobalSourceState &gstate) const {
	(void)context;
	(void)gstate;
	return make_uniq<SJBuildLocalSourceState>();
}

SourceResultType PhysicalSJBuild::GetData(ExecutionContext &context, DataChunk &chunk,
                                          OperatorSourceInput &input) const {
	(void)context;
	auto &gstate = input.global_state.Cast<SJBuildGlobalSourceState>();
	auto &lstate = input.local_state.Cast<SJBuildLocalSourceState>();
	gstate.data_collection.Scan(gstate.scan_state, lstate.local_scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

// ===========================================================================
// Pipeline wiring — mirrors RPT / DPT's PhysicalCreateBF.
//
// References:
//   - DPT physical_create_bf.cpp `BuildPipelines` (line ~530) — sink-of-its-
//     own-child-meta-pipeline + source-of-current pattern.
//   - DPT physical_create_bf.cpp `BuildPipelinesFromRelated` (line ~499) —
//     idempotent dependency wiring across multiple probes sharing one build.
//
// Behaviour:
//   - First BuildPipelines call: creates `this_pipeline` (the build pipeline)
//     as a child meta-pipeline of the current. Builds children[0] into it.
//   - First BuildPipelinesFromRelated call (when no Source-side BuildPipelines
//     ran first): same — creates the build pipeline.
//   - Subsequent BuildPipelinesFromRelated calls: idempotent — just adds a
//     dependency from `current` to the existing `this_pipeline`. Probes thus
//     wait on the build's Finalize.
// ===========================================================================

void PhysicalSJBuild::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);
	if (!this_pipeline) {
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(children[0].get());
	} else {
		current.AddDependency(this_pipeline);
	}
}

void PhysicalSJBuild::BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	D_ASSERT(children.size() == 1);

	if (!this_pipeline) {
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(children[0].get());
	} else {
		current.AddDependency(this_pipeline);
	}
}

} // namespace duckdb
