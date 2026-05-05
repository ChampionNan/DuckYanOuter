#include "duckdb/optimizer/outer_yan/simplification.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void Simplification::Apply(LogicalOperator &plan) {
	throw NotImplementedException("Simplification::Apply");
}

} // namespace duckdb
