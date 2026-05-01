#include "duckdb/execution/operator/join/physical_sj_probe.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

PhysicalSJProbe::PhysicalSJProbe(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                 vector<shared_ptr<HashFilter>> sj_to_use_p, idx_t estimated_cardinality)
    : CachingPhysicalOperator(physical_plan, PhysicalOperatorType::SJ_PROBE, std::move(types), estimated_cardinality),
      sj_to_use(std::move(sj_to_use_p)) {
}

unique_ptr<OperatorState> PhysicalSJProbe::GetOperatorState(ExecutionContext &context) const {
	throw NotImplementedException("PhysicalSJProbe::GetOperatorState");
}

OperatorResultType PhysicalSJProbe::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state) const {
	throw NotImplementedException("PhysicalSJProbe::ExecuteInternal");
}

} // namespace duckdb
