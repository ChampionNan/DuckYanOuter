#include "duckdb/optimizer/outer_yan/desimplification.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

// ============================================================================
// Static lookup tables
// ============================================================================

namespace {

constexpr idx_t kJoinKindIndex(OuterYanJoinKind k) {
	switch (k) {
	case JoinType::INNER:
		return 0;
	case JoinType::LEFT:
		return 1;
	case JoinType::RIGHT:
		return 2;
	case JoinType::OUTER:
		return 3;
	default:
		throw InternalException("Desimplification: OJT edge carries unsupported JoinType");
	}
}

} // namespace

using CellTag = Desimplification::CellTag;

//! Row/column order: INNER, LEFT, RIGHT, FULL(OUTER).
const CellTag Desimplification::kRightAssoc[4][4] = {
    /* D12=INNER */ {CellTag::OK, CellTag::OK, CellTag::R1, CellTag::R2},
    /* D12=LEFT  */ {CellTag::INVALID, CellTag::OK, CellTag::INVALID, CellTag::R3},
    /* D12=RIGHT */ {CellTag::OK, CellTag::OK, CellTag::OK, CellTag::OK},
    /* D12=FULL  */ {CellTag::INVALID, CellTag::OK, CellTag::INVALID, CellTag::OK},
};

const CellTag Desimplification::kLeftAssoc[4][4] = {
    /* D12=INNER */ {CellTag::OK, CellTag::OK, CellTag::INVALID, CellTag::INVALID},
    /* D12=LEFT  */ {CellTag::R4, CellTag::OK, CellTag::INVALID, CellTag::INVALID},
    /* D12=RIGHT */ {CellTag::OK, CellTag::OK, CellTag::OK, CellTag::OK},
    /* D12=FULL  */ {CellTag::R5, CellTag::OK, CellTag::R6, CellTag::OK},
};

CellTag Desimplification::LookupRightAssoc(OuterYanJoinKind k_lower, OuterYanJoinKind k_upper) {
	return kRightAssoc[kJoinKindIndex(k_lower)][kJoinKindIndex(k_upper)];
}

CellTag Desimplification::LookupLeftAssoc(OuterYanJoinKind k_lower, OuterYanJoinKind k_upper) {
	// Indexed by (Diamond_{1,2} = Diamond_up, Diamond_{2,3} = Diamond_low).
	return kLeftAssoc[kJoinKindIndex(k_upper)][kJoinKindIndex(k_lower)];
}

// ============================================================================
// Cell application
// ============================================================================

bool Desimplification::ApplyCell(CellTag tag, OJTEdge &e_lower, bool dry_run) {
	OuterYanJoinKind new_kind = e_lower.kind;
	switch (tag) {
	case CellTag::OK:
		return false;
	case CellTag::R1:
	case CellTag::R2:
		new_kind = JoinType::RIGHT; // Diamond_low Inner -> Right
		break;
	case CellTag::R3:
		new_kind = JoinType::OUTER; // Diamond_low Left  -> Full
		break;
	case CellTag::R4:
	case CellTag::R5:
		new_kind = JoinType::LEFT; // Diamond_low Inner -> Left
		break;
	case CellTag::R6:
		new_kind = JoinType::OUTER; // Diamond_low Right -> Full
		break;
	case CellTag::INVALID:
		if (dry_run) {
			return true; // signal: predicate fails for this pair
		}
		throw InternalException("Desimplification: pair is not a simple query "
		                        "(no associativity rule applies)");
	}
	if (new_kind == e_lower.kind) {
		return false;
	}
	if (!dry_run) {
		e_lower.kind = new_kind;
	}
	return true;
}

// ============================================================================
// Index construction
// ============================================================================

void Desimplification::BuildIndex(OJTNode &root) {
	edge_by_order.clear();
	parent_of.clear();
	n_joins = 0;
	BuildIndexDFS(root, nullptr);
}

void Desimplification::BuildIndexDFS(OJTNode &node, OJTNode *parent_node) {
	(void)parent_node; // parent linkage is recorded via `parent_of` below.
	for (auto &edge : node.children) {
		const idx_t ord = edge.order;
		if (ord == 0) {
			throw InternalException("Desimplification: encountered OJT edge with order 0");
		}
		if (ord >= edge_by_order.size()) {
			edge_by_order.resize(ord + 1);
		}
		edge_by_order[ord] = {&node, &edge};
		if (n_joins < ord) {
			n_joins = ord;
		}
		if (edge.child) {
			parent_of[edge.child.get()] = {&node, &edge};
			BuildIndexDFS(*edge.child, &node);
		}
	}
}

// ============================================================================
// Topology queries
// ============================================================================

OJTNode *Desimplification::SharedNode(const EdgeRef &lower, const EdgeRef &upper) {
	OJTNode *lp = lower.parent;
	OJTNode *lc = lower.edge->child.get();
	OJTNode *up = upper.parent;
	OJTNode *uc = upper.edge->child.get();
	if (lp == up || lp == uc) {
		return lp;
	}
	if (lc == up || lc == uc) {
		return lc;
	}
	return nullptr;
}

Desimplification::PathKind Desimplification::ClassifyPath(const EdgeRef &lower, const EdgeRef &upper, idx_t k) {
	if (SharedNode(lower, upper) != nullptr) {
		return PathKind::kSameNode;
	}

	OJTNode *lc = lower.edge->child.get();
	OJTNode *uc = upper.edge->child.get();

	// Build the set of OJT ancestors of `lc` (inclusive).
	unordered_set<OJTNode *> lc_ancestors;
	for (OJTNode *cur = lc; cur != nullptr;) {
		lc_ancestors.insert(cur);
		auto it = parent_of.find(cur);
		cur = (it != parent_of.end()) ? it->second.parent : nullptr;
	}

	// LCA(lc, uc): first node along uc -> root that is also in lc_ancestors.
	OJTNode *lca = nullptr;
	for (OJTNode *cur = uc; cur != nullptr;) {
		if (lc_ancestors.count(cur)) {
			lca = cur;
			break;
		}
		auto it = parent_of.find(cur);
		cur = (it != parent_of.end()) ? it->second.parent : nullptr;
	}
	if (!lca) {
		throw InternalException("Desimplification: no LCA between adjacent edges");
	}

	// Walk lc -> lca and uc -> lca, skipping the first edge from each direction
	// (which is e_low / e_up respectively).
	bool any_lower_order = false;
	bool any_higher_order = false;
	const idx_t k_plus_1 = k + 1;

	auto walk = [&](OJTNode *start) {
		OJTNode *node = start;
		bool first = true;
		while (node != lca) {
			auto it = parent_of.find(node);
			if (it == parent_of.end()) {
				throw InternalException("Desimplification: walked above OJT root during LCA traversal");
			}
			if (!first) {
				const idx_t ord = it->second.edge->order;
				if (ord < k) {
					any_lower_order = true;
				} else if (ord > k_plus_1) {
					any_higher_order = true;
				}
				// ord == k or k+1 cannot occur: e_low / e_up are excluded.
			}
			first = false;
			node = it->second.parent;
		}
	};
	walk(lc);
	walk(uc);

	if (any_lower_order && any_higher_order) {
		return PathKind::kMixed;
	}
	if (any_lower_order) {
		return PathKind::kLowerOnly;
	}
	if (any_higher_order) {
		return PathKind::kHigherOnly;
	}
	throw InternalException("Desimplification: distinct non-adjacent edges yielded an empty connecting path");
}

// ============================================================================
// Pair processing and fixpoint driver
// ============================================================================

bool Desimplification::ProcessPair(idx_t k, bool dry_run) {
	if (k + 1 > n_joins) {
		return false;
	}
	EdgeRef &lower = edge_by_order[k];
	EdgeRef &upper = edge_by_order[k + 1];
	if (!lower.edge || !upper.edge) {
		throw InternalException("Desimplification: missing entry in edge_by_order");
	}

	const PathKind path_kind = ClassifyPath(lower, upper, k);
	switch (path_kind) {
	case PathKind::kSameNode:
	case PathKind::kLowerOnly: {
		// Case (a) direct, or case (c) collapsed via lower-order subtree.
		// Direction selection under the right-assoc traversal convention
		// (R_a = non-shared end of e_low) maps every pair to right-assoc
		// form; the left-assoc table is retained as the dual lookup for the
		// flipped convention -- flip via `LookupLeftAssoc` here when needed.
		const CellTag tag = LookupRightAssoc(lower.edge->kind, upper.edge->kind);
		return ApplyCell(tag, *lower.edge, dry_run);
	}
	case PathKind::kHigherOnly:
		// Case (b): pair is remote in eval order -- already associative.
		return false;
	case PathKind::kMixed:
		// Case (d): recursive reduction over higher-/lower-order subpaths is
		// not yet implemented. Refine via `Swap` / `AdjacentSwap` once the
		// reordering machinery is in place.
		throw NotImplementedException("Desimplification: mixed-order path "
		                              "between adjacent edges -- recursive "
		                              "case (d) reduction not yet implemented");
	}
	return false;
}

bool Desimplification::RunPass(bool dry_run) {
	bool changed = false;
	for (idx_t k = 1; k + 1 <= n_joins; k++) {
		if (ProcessPair(k, dry_run)) {
			changed = true;
			if (dry_run) {
				return true;
			}
		}
	}
	return changed;
}

void Desimplification::Apply(OuterYanTree &tree) {
	if (!tree.HasOJT()) {
		return;
	}
	BuildIndex(tree.OJT().Root());
	while (RunPass(/*dry_run=*/false)) {
		// fixpoint
	}
}

bool Desimplification::AllPairsSatisfy(OuterYanTree &tree) {
	if (!tree.HasOJT()) {
		return true;
	}
	BuildIndex(tree.OJT().Root());
	return !RunPass(/*dry_run=*/true);
}

} // namespace duckdb
