#include "duckdb/planner/operator/logical_sj_probe.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

LogicalSJProbe::LogicalSJProbe(vector<shared_ptr<HashFilter>> sj_to_use_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SJ_PROBE), sj_to_use(std::move(sj_to_use_p)) {
}

void LogicalSJProbe::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	throw InternalException("Shouldn't go here: LogicalSJProbe::Serialize");
}

unique_ptr<LogicalOperator> LogicalSJProbe::Deserialize(Deserializer &deserializer) {
	throw InternalException("Shouldn't go here: LogicalSJProbe::Deserialize");
}

idx_t LogicalSJProbe::EstimateCardinality(ClientContext &context) {
	return children[0]->EstimateCardinality(context);
}

vector<ColumnBinding> LogicalSJProbe::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalSJProbe::ResolveTypes() {
	types = children[0]->types;
}

void LogicalSJProbe::AddDownStreamOperator(LogicalSJBuild *op) {
	related_sj_build.emplace_back(op);
}

} // namespace duckdb
