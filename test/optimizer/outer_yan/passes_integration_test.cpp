#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_common.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

#include <functional>

using namespace duckdb;

// =============================================================================
// Integration tests for OuterYanPre / OuterYanDP / OuterYanPost — exercised
// through `OuterYanTree::BuildOT`, `BuildOJT`, `OJTToLogicalPlan`. We don't
// attempt to mock OT/OJT directly; the public construction path is the same
// one optimizer.cpp uses and observably covers Phase 0..3 of the plan.
// =============================================================================

namespace {

void Seed(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(id INTEGER, ax INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b(id INTEGER, bx INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE c(id INTEGER, cx INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE d(id INTEGER, dx INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO b VALUES (1, 100), (2, 200)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO c VALUES (1, 1000), (3, 3000)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO d VALUES (2, 20000), (3, 30000)"));
}

//! Build the full IR pipeline up to OJT for `sql`. Returns the populated
//! OuterYanTree so callers can inspect OT, OJT, and applicability.
unique_ptr<OuterYanTree> BuildIRFor(Connection &con, const string &sql) {
	auto plan = con.ExtractPlan(sql);
	REQUIRE(plan);
	auto tree = make_uniq<OuterYanTree>();
	if (!tree->CheckApplicability(*plan)) {
		return tree;
	}
	tree->BuildOT(std::move(plan));
	tree->BuildOJT();
	return tree;
}

//! Count OTNode JOIN nodes via a recursive walk.
idx_t CountJoinNodes(const OTNode &node) {
	if (node.kind == OTNode::Kind::RELATION) {
		return 0;
	}
	return 1 + CountJoinNodes(*node.children[0]) + CountJoinNodes(*node.children[1]);
}

idx_t CountRelationNodes(const OTNode &node) {
	if (node.kind == OTNode::Kind::RELATION) {
		return 1;
	}
	return CountRelationNodes(*node.children[0]) + CountRelationNodes(*node.children[1]);
}

} // namespace

// ============================================================================
// OT structural invariants (post-Finalize).
// ============================================================================

TEST_CASE("outer_yan passes: OT::IsValid holds on 3-relation LEFT chain",
          "[outer_yan][passes][ot]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(tree->HasOT());
	string reason;
	REQUIRE(tree->OT().IsValid(&reason));
	REQUIRE(reason.empty());
}

TEST_CASE("outer_yan passes: OT shape — 3-relation chain has 2 JOINs + 3 RELATIONs",
          "[outer_yan][passes][ot]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(tree->HasOT());
	REQUIRE(CountJoinNodes(tree->OT().Root()) == 2);
	REQUIRE(CountRelationNodes(tree->OT().Root()) == 3);
}

TEST_CASE("outer_yan passes: OT shape — 4-relation chain has 3 JOINs + 4 RELATIONs",
          "[outer_yan][passes][ot]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx, d.dx "
	    "FROM a LEFT JOIN b ON a.id = b.id "
	    "       LEFT JOIN c ON a.id = c.id "
	    "       LEFT JOIN d ON a.id = d.id");
	REQUIRE(tree->HasOT());
	REQUIRE(CountJoinNodes(tree->OT().Root()) == 3);
	REQUIRE(CountRelationNodes(tree->OT().Root()) == 4);
}

TEST_CASE("outer_yan passes: OT relation_id uniqueness — each base relation distinct",
          "[outer_yan][passes][ot]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(tree->HasOT());
	REQUIRE(tree->ot_relations_by_id.size() == 3);
	for (idx_t i = 0; i < tree->ot_relations_by_id.size(); i++) {
		REQUIRE(tree->ot_relations_by_id[i] != nullptr);
		REQUIRE(tree->ot_relations_by_id[i]->kind == OTNode::Kind::RELATION);
		REQUIRE(tree->ot_relations_by_id[i]->relation_id == i);
	}
}

TEST_CASE("outer_yan passes: ot_joins_by_order is sorted ascending by `order`",
          "[outer_yan][passes][ot]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx, d.dx "
	    "FROM a LEFT JOIN b ON a.id = b.id "
	    "       LEFT JOIN c ON a.id = c.id "
	    "       LEFT JOIN d ON a.id = d.id");
	REQUIRE(tree->HasOT());
	REQUIRE(tree->ot_joins_by_order.size() == 3);
	for (idx_t i = 1; i < tree->ot_joins_by_order.size(); i++) {
		REQUIRE(tree->ot_joins_by_order[i - 1]->order <
		        tree->ot_joins_by_order[i]->order);
	}
}

// ============================================================================
// Simplification — null-rejecting predicate converts LEFT/RIGHT/FULL to INNER.
// Tested through observable behavior: post-Simplify, the OJT's edge `kind`
// (which holds the *simplified* join type) is INNER, while
// `original_kind` retains the source LEFT/RIGHT/FULL.
// ============================================================================

TEST_CASE("outer_yan passes: null-rejecting WHERE simplifies LEFT to INNER",
          "[outer_yan][passes][simplify]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// `b.bx > 0` rejects rows where b.bx is NULL, so the LEFT JOIN simplifies
	// to INNER on the (a,b) edge.
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id WHERE b.bx > 0");
	REQUIRE(tree->HasOJT());
	// At least one OJT edge should have its `kind` lowered to INNER while
	// `original_kind` is still LEFT_OUTER. The edge fields are mirrored from
	// `info->join_kind` / `info->original_join_kind`.
	bool found_simplified = false;
	std::function<void(const OJTNode &)> walk = [&](const OJTNode &node) {
		for (const auto &edge : node.children) {
			if (edge.kind == JoinType::INNER && edge.original_kind != JoinType::INNER) {
				found_simplified = true;
			}
			walk(*edge.child);
		}
	};
	walk(tree->OJT().Root());
	REQUIRE(found_simplified);
}

TEST_CASE("outer_yan passes: no simplification when WHERE preserves NULLs",
          "[outer_yan][passes][simplify]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// `b.bx IS NULL OR b.bx > 0` does NOT reject NULL → LEFT stays LEFT.
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id WHERE b.bx IS NULL OR b.bx > 0");
	REQUIRE(tree->HasOJT());
	// At least one edge retains its outer kind unchanged.
	bool found_outer = false;
	std::function<void(const OJTNode &)> walk = [&](const OJTNode &node) {
		for (const auto &edge : node.children) {
			if (edge.kind != JoinType::INNER && edge.kind == edge.original_kind) {
				found_outer = true;
			}
			walk(*edge.child);
		}
	};
	walk(tree->OJT().Root());
	REQUIRE(found_outer);
}

// ============================================================================
// Semi-join pair recording — bottom-up + top-down passes write into
// `tree.bottom_up_pairs` / `tree.top_down_pairs`. Each pass must produce at
// least one pair for an accepted query, and pairs must be self-consistent.
// ============================================================================

TEST_CASE("outer_yan passes: semi-join pair recording populates both phases",
          "[outer_yan][passes][semi]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// We need to drive the FULL `optimizer.cpp` pipeline (not just BuildOJT)
	// because OuterYanPre is what records pairs. Easiest: run a query and
	// confirm via EXPLAIN that SJ_BUILD + SJ_PROBE are inserted (downstream
	// observable signal of recorded pairs).
	REQUIRE_NO_FAIL(con.Query("PRAGMA explain_output='optimized_only'"));
	auto result = con.Query("EXPLAIN SELECT a.id, b.bx, c.cx "
	                        "FROM a LEFT JOIN b ON a.id = b.id "
	                        "LEFT JOIN c ON a.id = c.id");
	REQUIRE_FALSE(result->HasError());
	auto plan_text = result->ToString();
	REQUIRE(plan_text.find("SJ_BUILD") != string::npos);
	REQUIRE(plan_text.find("SJ_PROBE") != string::npos);
}

// ============================================================================
// OJT structure — verify OJT has a single root and the relation count matches.
// ============================================================================

TEST_CASE("outer_yan passes: OJT has the same relation count as OT",
          "[outer_yan][passes][ojt]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(tree->HasOJT());

	// Walk OJT and count nodes.
	idx_t ojt_nodes = 0;
	std::function<void(const OJTNode &)> walk = [&](const OJTNode &node) {
		ojt_nodes++;
		for (const auto &edge : node.children) {
			walk(*edge.child);
		}
	};
	walk(tree->OJT().Root());
	REQUIRE(ojt_nodes == 3);
}

TEST_CASE("outer_yan passes: OJT root has subtree containing all relations",
          "[outer_yan][passes][ojt]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con,
	    "SELECT a.id, b.bx, c.cx, d.dx "
	    "FROM a LEFT JOIN b ON a.id = b.id "
	    "       LEFT JOIN c ON a.id = c.id "
	    "       LEFT JOIN d ON a.id = d.id");
	REQUIRE(tree->HasOJT());

	// Collect every relation_id reachable from OJT root.
	unordered_set<idx_t> reachable;
	std::function<void(const OJTNode &)> walk = [&](const OJTNode &node) {
		reachable.insert(node.relation_id);
		for (const auto &edge : node.children) {
			walk(*edge.child);
		}
	};
	walk(tree->OJT().Root());
	REQUIRE(reachable.size() == 4);
}

// ============================================================================
// DP / OJTToLogicalPlan round-trip across many shapes — the rebuilt plan must
// be executable and produce the same result as the source plan.
// ============================================================================

TEST_CASE("outer_yan passes: DP+rebuild preserves results — 3-way LEFT chain",
          "[outer_yan][passes][dp]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);

	const string sql = "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	                   "LEFT JOIN c ON a.id = c.id ORDER BY a.id, b.bx NULLS FIRST";
	auto baseline = con.Query(sql);
	REQUIRE_FALSE(baseline->HasError());

	// Disable + re-enable OuterYan: results identical.
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers TO ''"));
	auto with_oy = con.Query(sql);
	REQUIRE(baseline->ToString() == with_oy->ToString());

	REQUIRE_NO_FAIL(con.Query(
	    "SET disabled_optimizers TO 'outer_yan_pre,outer_yan_dp,outer_yan_post'"));
	auto without_oy = con.Query(sql);
	REQUIRE(baseline->ToString() == without_oy->ToString());
}

TEST_CASE("outer_yan passes: DP+rebuild preserves results — 4-way LEFT chain",
          "[outer_yan][passes][dp]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);

	const string sql = "SELECT a.id, b.bx, c.cx, d.dx "
	                   "FROM a LEFT JOIN b ON a.id = b.id "
	                   "       LEFT JOIN c ON a.id = c.id "
	                   "       LEFT JOIN d ON a.id = d.id "
	                   "ORDER BY a.id, b.bx NULLS FIRST, c.cx NULLS FIRST, d.dx NULLS FIRST";

	auto baseline = con.Query(sql);
	REQUIRE_FALSE(baseline->HasError());

	REQUIRE_NO_FAIL(con.Query(
	    "SET disabled_optimizers TO 'outer_yan_pre,outer_yan_dp,outer_yan_post'"));
	auto without_oy = con.Query(sql);
	REQUIRE(baseline->ToString() == without_oy->ToString());
}

TEST_CASE("outer_yan passes: DP+rebuild preserves results — FULL OUTER in head",
          "[outer_yan][passes][dp]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);

	const string sql = "SELECT a.id, b.bx, c.cx "
	                   "FROM a FULL OUTER JOIN b ON a.id = b.id "
	                   "       LEFT JOIN c ON a.id = c.id "
	                   "ORDER BY a.id NULLS FIRST, b.bx NULLS FIRST, c.cx NULLS FIRST";

	auto baseline = con.Query(sql);
	REQUIRE_FALSE(baseline->HasError());

	REQUIRE_NO_FAIL(con.Query(
	    "SET disabled_optimizers TO 'outer_yan_pre,outer_yan_dp,outer_yan_post'"));
	auto without_oy = con.Query(sql);
	REQUIRE(baseline->ToString() == without_oy->ToString());
}

// ============================================================================
// Resimplification — when the chosen ordering allows it, INNER edges that
// came from OUTER (recorded in `original_kind`) are reverted back to OUTER.
// Observable through correctness on outer-join queries; structural assertion
// hard without internal access. Tested via end-to-end correctness above.
// ============================================================================

TEST_CASE("outer_yan passes: applicability rejection short-circuits — no OT built",
          "[outer_yan][passes][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto tree = BuildIRFor(con, "SELECT a.id, b.bx FROM a JOIN b ON a.id = b.id");
	REQUIRE_FALSE(tree->Applicability().applicable);
	REQUIRE_FALSE(tree->HasOT());
	REQUIRE_FALSE(tree->HasOJT());
}
