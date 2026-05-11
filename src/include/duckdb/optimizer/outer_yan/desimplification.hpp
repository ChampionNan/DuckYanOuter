//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/desimplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

namespace duckdb {

//! Iteratively applies the six Galindo-Legaria/Rosenthal outer-join
//! associativity rules to drive the OJT into the *associative-query state*:
//! every adjacent order pair (k, k+1) is either already associative, made so
//! by a rewrite of the lower-order kind, or skippable per case (b).
//!
//! The 4x4 lookup is split into two direction-keyed tables (`kRightAssoc`,
//! `kLeftAssoc`), both indexed by (Diamond_{1,2}, Diamond_{2,3}) -- i.e.,
//! kinds read in chain order R_1--R_2--R_3, not eval order. For a pair
//! sharing middle relation R_mid with non-shared ends R_a (of e_low) and
//! R_b (of e_up):
//!   ord(edge(R_a, R_mid)) < ord(edge(R_mid, R_b))  =>  right-assoc form (LHS)
//!   ord(edge(R_a, R_mid)) > ord(edge(R_mid, R_b))  =>  left-assoc  form (RHS)
//!
//! Right-assoc table (Diamond_{1,2} = Diamond_low, Diamond_{2,3} = Diamond_up):
//!   D12 \ D23   INNER     LEFT   RIGHT      FULL
//!   INNER       o         o      R1         R2
//!   LEFT        INVAL     o      INVAL      R3
//!   RIGHT       o         o      o          o
//!   FULL        INVAL     o      INVAL      o
//!
//! Left-assoc table (Diamond_{1,2} = Diamond_up, Diamond_{2,3} = Diamond_low):
//!   D12 \ D23   INNER     LEFT   RIGHT      FULL
//!   INNER       o         o      INVAL      INVAL
//!   LEFT        R4        o      INVAL      INVAL
//!   RIGHT       o         o      o          o
//!   FULL        R5        o      R6         o
//!
//! Each rule rewrites Diamond_low (the lower-order edge's kind):
//!   R1 (Inner, Right)  Diamond_low Inner -> Right    [right-assoc]
//!   R2 (Inner, Full)   Diamond_low Inner -> Right    [right-assoc]
//!   R3 (Left,  Full)   Diamond_low Left  -> Full     [right-assoc]
//!   R4 (Left,  Inner)  Diamond_low Inner -> Left     [left-assoc]
//!   R5 (Full,  Inner)  Diamond_low Inner -> Left     [left-assoc]
//!   R6 (Full,  Right)  Diamond_low Right -> Full     [left-assoc]
//!
//! INVALID cells indicate a non-simple query; `Apply` throws.
//!
//! Path classification at a pair (e_k, e_{k+1}):
//!   (a) shared OJT node                  -- direct table lookup.
//!   (b) path edges all have order > k+1  -- pair is remote in eval order;
//!                                           already associative; skip.
//!   (c) path edges all have order < k    -- collapsed via a virtual middle
//!                                           node; reduces to case (a).
//!   (d) mixed path orders                -- recursive reduction (TBD).
class Desimplification {
public:
	//! Drive `tree.OJT()` to fixpoint. No-op if OJT is absent.
	void Apply(OuterYanTree &tree);

	//! Predicate: every adjacent order pair is associative (or skippable per
	//! case (b)). Does not mutate the tree.
	bool AllPairsSatisfy(OuterYanTree &tree);

	enum class CellTag : uint8_t {
		OK,         //!< already associative; no rewrite
		R1, R2, R3, //!< right-assoc rules
		R4, R5, R6, //!< left-assoc rules
		INVALID     //!< not a simple query
	};
	enum class PathKind : uint8_t { kSameNode, kHigherOnly, kLowerOnly, kMixed };

private:
	struct EdgeRef {
		OJTNode *parent = nullptr; //!< OJT node owning the edge (parent endpoint)
		OJTEdge *edge = nullptr;
	};

	void BuildIndex(OJTNode &root);
	void BuildIndexDFS(OJTNode &node, OJTNode *parent_node);

	OJTNode *SharedNode(const EdgeRef &lower, const EdgeRef &upper);

	PathKind ClassifyPath(const EdgeRef &lower, const EdgeRef &upper, idx_t k);

	//! Right-assoc lookup table -- indexed by [Diamond_{1,2}][Diamond_{2,3}] where
	//! Diamond_{1,2} = Diamond_low, Diamond_{2,3} = Diamond_up.
	static const CellTag kRightAssoc[4][4];
	//! Left-assoc lookup table -- indexed by [Diamond_{1,2}][Diamond_{2,3}] where
	//! Diamond_{1,2} = Diamond_up, Diamond_{2,3} = Diamond_low.
	static const CellTag kLeftAssoc[4][4];

	//! Right-assoc lookup: Diamond_{1,2} = Diamond_low, Diamond_{2,3} = Diamond_up.
	static CellTag LookupRightAssoc(OuterYanJoinKind k_lower, OuterYanJoinKind k_upper);
	//! Left-assoc lookup: Diamond_{1,2} = Diamond_up, Diamond_{2,3} = Diamond_low.
	static CellTag LookupLeftAssoc(OuterYanJoinKind k_lower, OuterYanJoinKind k_upper);

	//! Mutate `e_lower.kind` per `tag`. For INVALID, throws in non-`dry_run`
	//! mode and reports a violation otherwise. Returns true iff the pair
	//! triggered any action (kind change or violation).
	static bool ApplyCell(CellTag tag, OJTEdge &e_lower, bool dry_run);

	bool ProcessPair(idx_t k, bool dry_run);
	bool RunPass(bool dry_run);

	vector<EdgeRef> edge_by_order;               //!< 1-based; index 0 unused
	unordered_map<OJTNode *, EdgeRef> parent_of; //!< child node -> parent edge
	idx_t n_joins = 0;
};

} // namespace duckdb
