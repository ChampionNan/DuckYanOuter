#include "duckdb/optimizer/outer_yan/resimplification.hpp"

#include "duckdb/common/exception.hpp"

#include <algorithm>

namespace duckdb {

void Resimplification::IndexOJT(OJTNode &node) {
	node_by_rel[node.relation_id] = &node;
	for (auto &edge : node.children) {
		if (!edge.child) {
			throw InternalException("Resimplification: OJT edge missing child");
		}
		all_edges.push_back(&edge);
		parent_edge_by_rel[edge.child->relation_id] = &edge;
		IndexOJT(*edge.child);
	}
}

bool Resimplification::ArrowTargetsSharedNode(const OJTEdge &k_edge, idx_t shared_rel) {
	D_ASSERT(k_edge.child);
	const bool shared_is_child = (shared_rel == k_edge.child->relation_id);
	D_ASSERT(shared_is_child || shared_rel == k_edge.parent_relation_id);
	switch (k_edge.kind) {
	case JoinType::INNER:
		return false;
	case JoinType::LEFT:
		// OJT places child on the right; arrow on right side.
		return shared_is_child;
	case JoinType::RIGHT:
		// Arrow on left = parent side.
		return !shared_is_child;
	case JoinType::OUTER:
		return true;
	default:
		throw InternalException(
		    "Resimplification: unsupported OuterYanJoinKind on OJT edge");
	}
}

bool Resimplification::TryRevert(OJTEdge &i_edge) {
	if (i_edge.kind == i_edge.original_kind) {
		return false; // not de-simplified.
	}
	if (!i_edge.child) {
		throw InternalException("Resimplification: OJT edge missing child");
	}
	const idx_t i_p = i_edge.parent_relation_id;
	const idx_t i_c = i_edge.child->relation_id;

	auto try_with = [&](OJTEdge *k, idx_t shared) -> bool {
		if (!k || k == &i_edge) {
			return false;
		}
		if (k->order <= i_edge.order) {
			return false;
		}
		if (!ArrowTargetsSharedNode(*k, shared)) {
			return false;
		}
		i_edge.kind = i_edge.original_kind;
		return true;
	};

	// Adjacent at i_p: the parent edge into OJTNode(i_p), plus i_edge's siblings.
	auto pe_it = parent_edge_by_rel.find(i_p);
	if (pe_it != parent_edge_by_rel.end() && try_with(pe_it->second, i_p)) {
		return true;
	}
	auto p_node_it = node_by_rel.find(i_p);
	if (p_node_it != node_by_rel.end()) {
		for (auto &sib : p_node_it->second->children) {
			if (try_with(&sib, i_p)) {
				return true;
			}
		}
	}

	// Adjacent at i_c: child edges of OJTNode(i_c). Monotonic order keeps
	// their order < i_edge.order, so they are filtered out; kept for
	// completeness in case the invariant ever weakens.
	for (auto &ce : i_edge.child->children) {
		if (try_with(&ce, i_c)) {
			return true;
		}
	}

	return false;
}

void Resimplification::Apply(OrderedJoinTree &ojt) {
	all_edges.clear();
	node_by_rel.clear();
	parent_edge_by_rel.clear();

	IndexOJT(ojt.Root());
	if (all_edges.size() < 2) {
		return;
	}

	// Ascending order keeps later $\Diamond_k$'s (possibly de-simplified)
	// arrow sides intact for as long as possible.
	std::sort(all_edges.begin(), all_edges.end(),
	          [](const OJTEdge *a, const OJTEdge *b) { return a->order < b->order; });

	bool changed;
	do {
		changed = false;
		for (auto *i_edge : all_edges) {
			if (TryRevert(*i_edge)) {
				changed = true;
			}
		}
	} while (changed);
}

} // namespace duckdb
