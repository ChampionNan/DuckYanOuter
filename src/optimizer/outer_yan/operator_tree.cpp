#include "duckdb/optimizer/outer_yan/operator_tree.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OperatorTree::OperatorTree() = default;

OperatorTree::OperatorTree(unique_ptr<OTNode> root_p) : root(std::move(root_p)) {
}

bool OperatorTree::IsCanonical() const {
	throw NotImplementedException("OperatorTree::IsCanonical");
}

void OperatorTree::Canonicalize() {
	throw NotImplementedException("OperatorTree::Canonicalize");
}

void OperatorTree::RotateLeft(OTNode &pivot) {
	throw NotImplementedException("OperatorTree::RotateLeft");
}

void OperatorTree::RotateRight(OTNode &pivot) {
	throw NotImplementedException("OperatorTree::RotateRight");
}

} // namespace duckdb
