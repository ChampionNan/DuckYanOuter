#include "duckdb/execution/operator/join/physical_sj_build.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

PhysicalSJBuild::PhysicalSJBuild(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                 vector<shared_ptr<HashFilter>> sj_to_create_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::SJ_BUILD, std::move(types), estimated_cardinality),
      sj_to_create(std::move(sj_to_create_p)) {
}

unique_ptr<GlobalSinkState> PhysicalSJBuild::GetGlobalSinkState(ClientContext &context) const {
	throw NotImplementedException("PhysicalSJBuild::GetGlobalSinkState");
}

unique_ptr<LocalSinkState> PhysicalSJBuild::GetLocalSinkState(ExecutionContext &context) const {
	throw NotImplementedException("PhysicalSJBuild::GetLocalSinkState");
}

SinkResultType PhysicalSJBuild::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	throw NotImplementedException("PhysicalSJBuild::Sink");
}

SinkCombineResultType PhysicalSJBuild::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	throw NotImplementedException("PhysicalSJBuild::Combine");
}

SinkFinalizeType PhysicalSJBuild::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const {
	throw NotImplementedException("PhysicalSJBuild::Finalize");
}

void PhysicalSJBuild::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	throw NotImplementedException("PhysicalSJBuild::BuildPipelines");
}

} // namespace duckdb
