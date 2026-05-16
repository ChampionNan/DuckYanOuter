#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

OrderedJoinTree::OrderedJoinTree() = default;

OrderedJoinTree::OrderedJoinTree(unique_ptr<OJTNode> root_p) : root(std::move(root_p)) {
}

void OrderedJoinTree::PostOrder(std::function<void(OJTNode &, optional_ptr<OJTNode>, OuterYanJoinKind)> callback) {
	throw NotImplementedException("OrderedJoinTree::PostOrder");
}

// ============================================================================
// Printer helpers
// ============================================================================

string OrderedJoinTree::RelationLabel(const OJTNode &node) {
	const auto *base = FindBaseOp(node.base_op);
	if (!base) {
		return "<no base_op>";
	}
	if (base->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = base->Cast<LogicalGet>();
		auto table_entry = get.GetTable();
		string name = table_entry ? table_entry->name : "<no_table>";
		return StringUtil::Format("%s, table_index=%llu", name.c_str(), get.table_index.index);
	}
	return EnumUtil::ToString(base->type);
}

void OrderedJoinTree::CollectNodes(const OJTNode &node, vector<const OJTNode *> &out) {
	out.push_back(&node);
	for (const auto &edge : node.children) {
		if (edge.child) {
			CollectNodes(*edge.child, out);
		}
	}
}

void OrderedJoinTree::CollectEdges(const OJTNode &node, vector<const OJTEdge *> &out) {
	for (const auto &edge : node.children) {
		if (!edge.child) {
			continue;
		}
		out.push_back(&edge);
		CollectEdges(*edge.child, out);
	}
}

void OrderedJoinTree::PrintNodeRecursive(const OJTNode &node, std::ostringstream &os,
                                         const string &indent, bool is_last_child, bool is_root,
                                         OuterYanJoinKind incoming_kind, idx_t incoming_order) {
	os << indent;
	if (!is_root) {
		os << (is_last_child ? "└──" : "├──");
		os << "[order=" << incoming_order << ", " << EnumUtil::ToString(incoming_kind) << "]── ";
	}
	os << "R" << node.relation_id << " [" << RelationLabel(node) << "]";
	if (!node.filters.empty()) {
		os << " filters=" << node.filters.size();
	}
	os << "\n";

	string child_indent = indent;
	if (!is_root) {
		child_indent += (is_last_child ? "    " : "│   ");
	}
	for (idx_t i = 0; i < node.children.size(); i++) {
		const auto &edge = node.children[i];
		if (!edge.child) {
			continue;
		}
		bool last = (i + 1 == node.children.size());
		PrintNodeRecursive(*edge.child, os, child_indent, last, false, edge.kind, edge.order);
	}
}

// ============================================================================
// Printers
// ============================================================================

string OrderedJoinTree::Print() const {
	std::ostringstream os;
	const auto &r = Root();

	vector<const OJTNode *> nodes;
	CollectNodes(r, nodes);
	std::sort(nodes.begin(), nodes.end(),
	          [](const OJTNode *a, const OJTNode *b) { return a->relation_id < b->relation_id; });

	vector<const OJTEdge *> edges;
	CollectEdges(r, edges);
	std::sort(edges.begin(), edges.end(),
	          [](const OJTEdge *a, const OJTEdge *b) { return a->order < b->order; });

	os << "OJT: " << nodes.size() << " relations, " << edges.size() << " joins\n";
	os << "root: R" << r.relation_id << "\n";
	os << "relations:\n";
	for (auto *n : nodes) {
		os << "  R" << n->relation_id << " = " << RelationLabel(*n);
		if (!n->filters.empty()) {
			os << " (filters=" << n->filters.size() << ")";
		}
		os << "\n";
	}
	os << "edges (parent --KIND/order--> child):\n";
	for (auto *e : edges) {
		os << "  R" << e->parent_relation_id << " --" << EnumUtil::ToString(e->kind) << "/" << e->order
		   << "--> R" << e->child->relation_id << "\n";
	}
	return os.str();
}

string OrderedJoinTree::PrintAsTree() const {
	std::ostringstream os;
	const auto &r = Root();

	vector<const OJTNode *> nodes;
	CollectNodes(r, nodes);
	vector<const OJTEdge *> edges;
	CollectEdges(r, edges);
	os << "OJT (" << nodes.size() << " relations, " << edges.size() << " joins)\n";
	PrintNodeRecursive(r, os, /*indent*/ "", /*is_last_child*/ true, /*is_root*/ true,
	                   JoinType::INNER, 0);
	return os.str();
}

} // namespace duckdb
