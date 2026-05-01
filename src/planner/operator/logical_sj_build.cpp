#include "duckdb/planner/operator/logical_sj_build.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

LogicalSJBuild::LogicalSJBuild(vector<shared_ptr<HashFilter>> sj_to_create_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SJ_BUILD), sj_to_create(std::move(sj_to_create_p)) {
}

void LogicalSJBuild::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	throw InternalException("Shouldn't go here: LogicalSJBuild::Serialize");
}

unique_ptr<LogicalOperator> LogicalSJBuild::Deserialize(Deserializer &deserializer) {
	throw InternalException("Shouldn't go here: LogicalSJBuild::Deserialize");
}

idx_t LogicalSJBuild::EstimateCardinality(ClientContext &context) {
	return children[0]->EstimateCardinality(context);
}

vector<ColumnBinding> LogicalSJBuild::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalSJBuild::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
