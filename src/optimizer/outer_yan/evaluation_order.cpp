#include "duckdb/optimizer/outer_yan/evaluation_order.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void OrderFix::Apply(OrderedJoinTree &ojt, bool is_aggregation_query) {
	throw NotImplementedException("OrderFix::Apply");
}

} // namespace duckdb
