#include "duckdb/optimizer/outer_yan/desimplification.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"

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
		throw InternalException("Desimplification: OTNode carries unsupported JoinType");
	}
}

} // namespace

using CellTag = Desimplification::CellTag;

//! Right-assoc, indexed by [D_low.kind][D_up.kind].
const CellTag Desimplification::kRightAssoc[4][4] = {
    /* D_low=INNER */ {CellTag::OK, CellTag::OK, CellTag::R1, CellTag::R2},
    /* D_low=LEFT  */ {CellTag::INVALID, CellTag::OK, CellTag::INVALID, CellTag::R3},
    /* D_low=RIGHT */ {CellTag::OK, CellTag::OK, CellTag::OK, CellTag::OK},
    /* D_low=FULL  */ {CellTag::INVALID, CellTag::OK, CellTag::INVALID, CellTag::OK},
};

//! Left-assoc, indexed by [D_up.kind][D_low.kind].
const CellTag Desimplification::kLeftAssoc[4][4] = {
    /* D_up=INNER */ {CellTag::OK, CellTag::OK, CellTag::INVALID, CellTag::INVALID},
    /* D_up=LEFT  */ {CellTag::R4, CellTag::OK, CellTag::INVALID, CellTag::INVALID},
    /* D_up=RIGHT */ {CellTag::OK, CellTag::OK, CellTag::OK, CellTag::OK},
    /* D_up=FULL  */ {CellTag::R5, CellTag::OK, CellTag::R6, CellTag::OK},
};

CellTag Desimplification::LookupRightAssoc(OuterYanJoinKind d_low, OuterYanJoinKind d_up) {
	return kRightAssoc[kJoinKindIndex(d_low)][kJoinKindIndex(d_up)];
}

CellTag Desimplification::LookupLeftAssoc(OuterYanJoinKind d_up, OuterYanJoinKind d_low) {
	return kLeftAssoc[kJoinKindIndex(d_up)][kJoinKindIndex(d_low)];
}

OuterYanJoinKind Desimplification::NewLowerKind(CellTag tag) {
	switch (tag) {
	case CellTag::R1:
	case CellTag::R2:
		return JoinType::RIGHT;
	case CellTag::R3:
		return JoinType::OUTER;
	case CellTag::R4:
	case CellTag::R5:
		return JoinType::LEFT;
	case CellTag::R6:
		return JoinType::OUTER;
	default:
		throw InternalException("Desimplification::NewLowerKind: tag has no rewrite kind");
	}
}

// ============================================================================
// Pair matching
// ============================================================================

bool Desimplification::TryMatch(OTNode &P, OTNode &D, bool right_assoc, bool dry_run) {
	D_ASSERT(P.info && D.info);
	auto &P_info = *P.info;
	auto &D_info = *D.info;

	// The shared middle relation between P and D flows through P's condition:
	//   right_assoc -> the LHS relation of P (which lives in P.children[0]).
	//   left_assoc  -> the RHS relation of P (which lives in P.children[1]).
	const idx_t mid_rel =
	    right_assoc ? P_info.cond_left_relation_id : P_info.cond_right_relation_id;
	if (!D.subtree_relations.count(mid_rel)) {
		return false; // P and D are not adjacent through P's condition.
	}

	// Locate `mid_rel` within D's two subtrees. By OT canonicalisation
	// (`OperatorTree::Finalize`) every relation lives in exactly one of
	// children[0] / children[1], so exactly one side will contain it.
	const bool in_D_left = D.children[0]->subtree_relations.count(mid_rel) > 0;
	const bool in_D_right = D.children[1]->subtree_relations.count(mid_rel) > 0;
	if (in_D_left == in_D_right) {
		// Either both (would violate relation_id uniqueness) or neither
		// (contradicts the `D.subtree_relations.count` guard above).
		throw InternalException(
		    "Desimplification: middle relation %llu not uniquely placed in D's subtrees",
		    mid_rel);
	}

	// Canonical shape places the middle relation on the side of D away from
	// the recursion direction:
	//   right-assoc -> middle relation should be in D's RIGHT subtree;
	//   left-assoc  -> middle relation should be in D's LEFT subtree.
	// When the OT shape inverts that, view D as flipped for the table lookup;
	// any rewrite kind produced by the table is flipped back before commit.
	const bool invert_D = right_assoc ? in_D_left : in_D_right;
	const OuterYanJoinKind eff_D_kind =
	    invert_D ? FlipOuterYanJoinKind(D_info.join_kind) : D_info.join_kind;

	const CellTag tag = right_assoc ? LookupRightAssoc(eff_D_kind, P_info.join_kind)
	                                : LookupLeftAssoc(P_info.join_kind, eff_D_kind);
	if (tag == CellTag::INVALID) {
		throw InternalException(
		    "Desimplification: non-simple query at (P=%s, D=%s) under %s-assoc -- "
		    "simplification was incomplete",
		    EnumUtil::ToString(P_info.join_kind), EnumUtil::ToString(D_info.join_kind),
		    right_assoc ? "right" : "left");
	}
	if (tag == CellTag::OK) {
		return false;
	}
	if (dry_run) {
		return true; // would mutate -- pair not yet satisfied.
	}

	const OuterYanJoinKind new_kind = NewLowerKind(tag);
	const OuterYanJoinKind committed = invert_D ? FlipOuterYanJoinKind(new_kind) : new_kind;
	if (committed == D_info.join_kind) {
		return false;
	}
	D_info.join_kind = committed;
	return true;
}

// ============================================================================
// Recursion drivers
// ============================================================================

bool Desimplification::MatchDescendants(OTNode &P, OTNode &node, bool right_assoc, bool dry_run) {
	if (node.kind != OTNode::Kind::JOIN) {
		return false;
	}
	bool changed = TryMatch(P, node, right_assoc, dry_run);
	if (dry_run && changed) {
		return true;
	}
	changed |= MatchDescendants(P, *node.children[0], right_assoc, dry_run);
	if (dry_run && changed) {
		return true;
	}
	changed |= MatchDescendants(P, *node.children[1], right_assoc, dry_run);
	return changed;
}

bool Desimplification::VisitAsParent(OTNode &P, bool dry_run) {
	if (P.kind != OTNode::Kind::JOIN) {
		return false;
	}
	bool changed = MatchDescendants(P, *P.children[0], /*right_assoc=*/true, dry_run);
	if (dry_run && changed) {
		return true;
	}
	changed |= MatchDescendants(P, *P.children[1], /*right_assoc=*/false, dry_run);
	if (dry_run && changed) {
		return true;
	}
	changed |= VisitAsParent(*P.children[0], dry_run);
	if (dry_run && changed) {
		return true;
	}
	changed |= VisitAsParent(*P.children[1], dry_run);
	return changed;
}

// ============================================================================
// Public entry points
// ============================================================================

void Desimplification::Apply(OuterYanTree &tree) {
	if (!tree.HasOT()) {
		return;
	}
	while (VisitAsParent(tree.OT().Root(), /*dry_run=*/false)) {
		// fixpoint
	}
}

bool Desimplification::AllPairsSatisfy(OuterYanTree &tree) {
	if (!tree.HasOT()) {
		return true;
	}
	return !VisitAsParent(tree.OT().Root(), /*dry_run=*/true);
}

} // namespace duckdb
