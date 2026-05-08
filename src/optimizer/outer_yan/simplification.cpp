#include "duckdb/optimizer/outer_yan/simplification.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void Simplification::Apply(OuterYanTree &tree) {
	throw NotImplementedException("Simplification::Apply");
}

} // namespace duckdb
