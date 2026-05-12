//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/desimplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/outer_yan/operator_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

namespace duckdb {

//! Iteratively applies the six Galindo-Legaria/Rosenthal outer-join
//! associativity rules over the `OperatorTree` until every (P, D) JOIN pair
//! is in the *associative-query state* (an OK cell after operator-inverse
//! normalisation).
//!
//! For each ordered JOIN pair (P, D) with D a JOIN descendant of P:
//!   - D in P.children[0]  -> right-assoc table (D = D_low, P = D_up).
//!   - D in P.children[1]  -> left-assoc  table (D = D_low, P = D_up).
//! The shared "middle" relation flows through P's condition: it is
//! P.left_child_relation_id when D sits on the left, P.right_child_relation_id
//! when D sits on the right. If that relation is absent from D's subtree the
//! pair is skipped (P and D are not adjacent through P's condition). When the
//! middle relation falls on the "wrong" side of D for the canonical rule
//! shape, `FlipOuterYanJoinKind` is applied to D's kind *at lookup time only*
//! (no OT mutation), and any mutation that results is flipped back before
//! committing to `D.join_kind`.
//!
//! Right-assoc table (D_low = D_{1,2}, D_up = D_{2,3}; cell value = new D_low.kind):
//!   D12 \ D23   INNER     LEFT   RIGHT          FULL
//!   INNER       OK        OK     R1:=RIGHT      R2:=RIGHT
//!   LEFT        INVAL     OK     INVAL          R3:=FULL
//!   RIGHT       OK        OK     OK             OK
//!   FULL        INVAL     OK     INVAL          OK
//!
//! Left-assoc table (D_up = D_{1,2}, D_low = D_{2,3}; cell value = new D_low.kind):
//!   D12 \ D23   INNER         LEFT   RIGHT          FULL
//!   INNER       OK            OK     INVAL          INVAL
//!   LEFT        R4:=LEFT      OK     INVAL          INVAL
//!   RIGHT       OK            OK     OK             OK
//!   FULL        R5:=LEFT      OK     R6:=FULL       OK
//!
//! Each post-rewrite (D_low.kind, D_up.kind) combination lands in an OK cell.
//! INVALID cells flag a non-simple query; `Apply` and `AllPairsSatisfy` both
//! throw in that case -- simplification was incomplete.
class Desimplification {
public:
	//! Drive `tree.OT()` to fixpoint. No-op if OT is absent.
	void Apply(OuterYanTree &tree);

	//! Predicate: every (P, D) pair already sits in an OK cell. Throws on
	//! INVALID. Does not mutate the tree.
	bool AllPairsSatisfy(OuterYanTree &tree);

	enum class CellTag : uint8_t {
		OK,         //!< already associative; no rewrite
		R1, R2, R3, //!< right-assoc rules
		R4, R5, R6, //!< left-assoc rules
		INVALID     //!< not a simple query
	};

private:
	//! Row/column order across both tables: INNER, LEFT, RIGHT, FULL(OUTER).
	static const CellTag kRightAssoc[4][4];
	static const CellTag kLeftAssoc[4][4];

	//! Right-assoc lookup: indexed by (D_low.kind, D_up.kind).
	static CellTag LookupRightAssoc(OuterYanJoinKind d_low, OuterYanJoinKind d_up);
	//! Left-assoc  lookup: indexed by (D_up.kind, D_low.kind).
	static CellTag LookupLeftAssoc(OuterYanJoinKind d_up, OuterYanJoinKind d_low);

	//! For a non-OK, non-INVALID rule: the canonical-frame new D_low kind.
	//!   R1, R2 -> RIGHT;  R3 -> FULL;  R4, R5 -> LEFT;  R6 -> FULL.
	static OuterYanJoinKind NewLowerKind(CellTag tag);

	//! Recursion #2 (outer): visit every OTNode as the parent P top-down. At
	//! each P, drive recursion #1 across both subtrees, then descend into the
	//! children as new Ps. Returns true if any (P, D) pair fired.
	bool VisitAsParent(OTNode &P, bool dry_run);

	//! Recursion #1 (inner): with P fixed, walk every JOIN descendant D in
	//! `subtree_root` and call `TryMatch`.
	bool MatchDescendants(OTNode &P, OTNode &subtree_root, bool right_assoc, bool dry_run);

	//! (P, D) pair handling: middle-relation locate via P's condition,
	//! at-lookup operator inversion via `FlipOuterYanJoinKind`, table lookup,
	//! optional mutation of `D.join_kind`. Returns true iff a non-OK cell
	//! fired (rewrite committed when `!dry_run`, signalled when `dry_run`).
	//! Throws on INVALID in either mode.
	bool TryMatch(OTNode &P, OTNode &D, bool right_assoc, bool dry_run);
};

} // namespace duckdb
