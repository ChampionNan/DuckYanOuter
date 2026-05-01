//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/ordered_join_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/planner/expression.hpp"

#include <functional>

namespace duckdb {

//! Edge classification in the Ordered Join Tree (OJT).
enum class OJTEdgeKind : uint8_t {
	INNER,
	LEFT_OUTER,   //!< parent preserved, child null-supplying
	RIGHT_OUTER,  //!< child preserved, parent null-supplying
	FULL_OUTER,
};

struct OJTNode;

//! Edge from an OJTNode to a child subtree.
struct OJTEdge {
	//! Current edge classification (may have been transformed by Desimplification).
	OJTEdgeKind kind = OJTEdgeKind::INNER;
	//! Original edge classification, recorded before Desimplification rules were
	//! applied. Resimplification uses this to revert back to the original join
	//! type when the chosen ordering makes the rewrite redundant.
	OJTEdgeKind original_kind = OJTEdgeKind::INNER;
	//! Reference to the OT node corresponding to the join operator on this
	//! edge. The actual join predicate lives inside that OT node's underlying
	//! `LogicalOperator` — the OJT does not duplicate it.
	OTNode *join_node = nullptr;
	unique_ptr<OJTNode> child;
	//! Post-DP only: whether this edge is implemented as a full join or as an SJ reduction pair.
	bool implement_as_reduction = false;
};

//! Node in the Ordered Join Tree.
struct OJTNode {
	idx_t relation_id = 0;
	//! Single-relation predicates pushed down to this node.
	vector<unique_ptr<Expression>> filters;
	//! Ordered children — order is meaningful for evaluation-order regimes.
	vector<OJTEdge> children;
	RelationStats stats;
};

//! Ordered Join Tree — internal IR used inside OuterYanDP and OuterYanPost.
//! Mapping to/from OT focuses only on join + base-relation nodes; non-join
//! operators ride along inside the underlying LogicalOperator referenced
//! through `OJTEdge::join_node` (and the OT structure that points to them).
class OrderedJoinTree {
public:
	OrderedJoinTree();
	explicit OrderedJoinTree(unique_ptr<OJTNode> root);

	OJTNode &Root() {
		return *root;
	}
	const OJTNode &Root() const {
		return *root;
	}

	static unique_ptr<OrderedJoinTree> FromOT(const OperatorTree &ot);
	unique_ptr<OperatorTree> ToOT() const;

	//! Visit every node in post-order. Callback receives node, its parent (or
	//! null for root), and the edge kind from parent to node.
	void PostOrder(std::function<void(OJTNode &, optional_ptr<OJTNode>, OJTEdgeKind)> callback);

private:
	unique_ptr<OJTNode> root;
};

} // namespace duckdb
