#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OrderedJoinTree::OrderedJoinTree() = default;

OrderedJoinTree::OrderedJoinTree(unique_ptr<OJTNode> root_p) : root(std::move(root_p)) {
}

void OrderedJoinTree::PostOrder(std::function<void(OJTNode &, optional_ptr<OJTNode>, OJTEdgeKind)> callback) {
	throw NotImplementedException("OrderedJoinTree::PostOrder");
}

} // namespace duckdb
