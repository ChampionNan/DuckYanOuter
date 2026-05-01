#include "duckdb/optimizer/outer_yan/cost_model_outer.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OuterYanCostModel::OuterYanCostModel() = default;

double OuterYanCostModel::SubtreeCost(const OJTNode &node) {
	throw NotImplementedException("OuterYanCostModel::SubtreeCost");
}

} // namespace duckdb
