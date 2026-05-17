//===----------------------------------------------------------------------===//
// PhysicalSJProbe — OuterYan semi-join probe operator.
//
// IMPLEMENTATION REFERENCES — please cross-check against these when editing.
//
// Primary references (operator-in-the-middle, BuildPipelines wiring,
// related-build pipeline dependency):
//   - embryo-labs/dynamic-predicate-transfer:
//       src/execution/operator/filter/physical_use_bf.cpp
//     The ExecuteInternal layout + BuildPipelines come from this file.
//     (Local /tmp/dpt_physical_use_bf.cpp during the design session.)
//   - embryo-labs/Robust-Predicate-Transfer:
//       src/execution/operator/filter/physical_use_bf.cpp
//     Older variant; same shape.
//
// What we INTENTIONALLY DO NOT carry over (per plan 3 non-goals):
//   - UseBFState::CheckBFSelectivity / adaptive disable (0.9 threshold).
//   - COMPACTION_THRESHOLD / below_join chunk caching.
//   - Per-state selectivity tracking counters.
//   PhysicalSJProbe is a stateless filter — just a per-chunk switch on the
//   shared filter's kind, run the kernel, slice the input.
//
// What we ADD vs DPT:
//   - Dispatch on SemiJoinFilter::kind {BITMAP, HASH}. DPT only had bloom.
// ===========================================================================

#include "duckdb/execution/operator/join/physical_sj_probe.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"
#include "duckdb/optimizer/outer_yan/key_bitmap.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalSJProbe::PhysicalSJProbe(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                 vector<shared_ptr<SemiJoinFilter>> sj_to_use_p,
                                 vector<vector<idx_t>> probe_col_ids_per_filter_p,
                                 idx_t estimated_cardinality)
    : CachingPhysicalOperator(physical_plan, PhysicalOperatorType::SJ_PROBE, std::move(types), estimated_cardinality),
      sj_to_use(std::move(sj_to_use_p)),
      probe_col_ids_per_filter(std::move(probe_col_ids_per_filter_p)) {
	if (sj_to_use.size() != probe_col_ids_per_filter.size()) {
		throw InternalException("PhysicalSJProbe: filter / probe_col_ids count mismatch");
	}
}

unique_ptr<OperatorState> PhysicalSJProbe::GetOperatorState(ExecutionContext &context) const {
	(void)context;
	return make_uniq<CachingOperatorState>();
}

// ===========================================================================
// ExecuteInternal — apply each filter in `sj_to_use` to narrow the surviving
// row set. Filters compose by intersection (a row passes iff it passes every
// filter), so we can iterate and tighten the SelectionVector in place.
//
// Per plan 3 §"Probe flow":
//   - Branch ONCE per filter on filter->kind() — outside the per-tuple loop.
//   - kernel call is straight-line vectorisable code.
//   - Filter->kind() returns the same value for the operator's lifetime
//     (set once at PhysicalSJBuild::Finalize), so branch prediction is
//     trivial.
//
// Multi-filter handling: each filter narrows independently against the
// CURRENT surviving rows. We track a working DataChunk (`current`) that is
// either a reference to input (no filtering yet) or a Slice of input. After
// all filters run, we either pass `current` through or build a final
// SelectionVector over the original input rows.
// ===========================================================================

OperatorResultType PhysicalSJProbe::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state) const {
	(void)context;
	(void)gstate;
	(void)state;

	const idx_t row_count = input.size();
	if (row_count == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	if (sj_to_use.empty()) {
		// Degenerate: nothing to filter. Pass through.
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Iteratively narrow `surviving` (a selection vector over the ORIGINAL
	// input row indices). Start with the identity mapping [0..row_count).
	SelectionVector surviving(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < row_count; i++) {
		surviving.set_index(i, i);
	}
	idx_t surviving_count = row_count;

	// Scratch chunk reused across filters when slicing is needed. Initialised
	// lazily.
	DataChunk working;
	bool working_initialised = false;
	auto active_chunk = [&]() -> DataChunk & {
		if (surviving_count == row_count) {
			return input;
		}
		if (!working_initialised) {
			working.Initialize(Allocator::DefaultAllocator(), input.GetTypes());
			working_initialised = true;
		}
		working.Slice(input, surviving, surviving_count);
		return working;
	};

	for (idx_t f = 0; f < sj_to_use.size(); f++) {
		auto &filter = sj_to_use[f];
		const auto &probe_col_ids = probe_col_ids_per_filter[f];

		// Branch on kind once per filter — invariant for the lifetime of
		// this operator.
		SelectionVector pass_sel(STANDARD_VECTOR_SIZE);
		idx_t pass_count = 0;
		DataChunk &chunk_in = active_chunk();

		switch (filter->kind()) {
		case SemiJoinFilter::Kind::BITMAP:
			filter->bitmap().ProbeBatch(chunk_in, probe_col_ids, pass_sel, pass_count);
			break;
		case SemiJoinFilter::Kind::HASH:
			HashFilterUseKernel::filter(chunk_in, probe_col_ids, filter->hash(), pass_sel, pass_count);
			break;
		case SemiJoinFilter::Kind::UNDECIDED:
			throw InternalException("PhysicalSJProbe: SJBuild::Finalize did not run before probe");
		}

		if (pass_count == 0) {
			chunk.SetCardinality(0);
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// Compose `surviving` ← `surviving ∘ pass_sel`. `pass_sel[i]`
		// indexes into chunk_in, which itself was a Slice over `surviving`
		// (or is identity-mapped input if no prior filter narrowed).
		if (surviving_count == row_count) {
			// pass_sel indexes input directly.
			for (idx_t i = 0; i < pass_count; i++) {
				surviving.set_index(i, pass_sel.get_index(i));
			}
		} else {
			// pass_sel indexes the slice; remap to original input rows.
			for (idx_t i = 0; i < pass_count; i++) {
				surviving.set_index(i, surviving.get_index(pass_sel.get_index(i)));
			}
		}
		surviving_count = pass_count;
	}

	if (surviving_count == row_count) {
		// No filter narrowed anything — pass through.
		chunk.Reference(input);
	} else {
		chunk.Slice(input, surviving, surviving_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

// ===========================================================================
// BuildPipelines — operator-in-the-middle that establishes a dependency on
// each related PhysicalSJBuild's build pipeline. Reference: DPT
// physical_use_bf.cpp BuildPipelines (line ~67).
//
// Idempotent across multiple probes sharing one build: each call to the
// build's BuildPipelinesFromRelated either creates `this_pipeline` (first
// time) or AddDependencies it (subsequent times). DuckDB's Pipeline
// dependency edges deduplicate identical adds.
// ===========================================================================

void PhysicalSJProbe::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);
	for (auto *builder : related_sj_build) {
		builder->BuildPipelinesFromRelated(current, meta_pipeline);
	}
	D_ASSERT(children.size() == 1);
	children[0].get().BuildPipelines(current, meta_pipeline);
}

} // namespace duckdb
