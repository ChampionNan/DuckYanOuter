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
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"

namespace duckdb {

//! PhysicalSJProbe — probe-side of an OuterYan semi-join reducer pair.
//!
//! Filters its input chunks against the shared HashFilter instances in
//! `sj_to_use`, populated by the paired PhysicalSJBuild operators tracked
//! in `related_create_bf` (mirrors RPT's PhysicalUseBF::related_create_bf).
//!
//! TODO: full Operator-in-the-middle lifecycle. Stub for scaffolding.
class PhysicalSJProbe : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SJ_PROBE;

public:
	PhysicalSJProbe(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                vector<shared_ptr<HashFilter>> sj_to_use, idx_t estimated_cardinality);

	vector<shared_ptr<HashFilter>> sj_to_use;

	//! Back-pointers to upstream PhysicalSJBuild operators, set during
	//! physical plan generation from LogicalSJProbe::related_sj_build.
	vector<PhysicalSJBuild *> related_sj_build;

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::INSERTION_ORDER;
	}
	bool ParallelOperator() const override {
		return true;
	}

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
};

} // namespace duckdb
