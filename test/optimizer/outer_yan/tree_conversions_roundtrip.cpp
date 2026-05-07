#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/outer_yan/ordered_join_tree.hpp"
#include "duckdb/optimizer/outer_yan/tree_conversions.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

using namespace duckdb;

namespace {

void Seed(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(id INTEGER, ax INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b(id INTEGER, bx INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE c(id INTEGER, cx INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30), (4, 40)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO b VALUES (1, 100), (2, 200), (3, 300), (5, 500)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO c VALUES (1, 1000), (3, 3000), (4, 4000)"));
}

//! Run `sql` through:
//!   reference path: con.Query(sql) — full pipeline.
//!   roundtrip path: con.ExtractPlan(sql) → LogicalPlanToOJT → OJTToLogicalPlan
//!                   → wrap as LogicalPlanStatement → con.Query(stmt).
//! Assert both paths return identical materialised results. Caller must
//! supply an `ORDER BY` so the comparison is deterministic.
void AssertRoundtripMatches(Connection &con, const string &sql) {
	auto reference = con.Query(sql);
	REQUIRE_FALSE(reference->HasError());

	auto plan = con.ExtractPlan(sql);
	REQUIRE(plan);

	auto ojt = LogicalPlanToOJT(std::move(plan));
	REQUIRE(ojt);

	auto rebuilt = OJTToLogicalPlan(*con.context, std::move(ojt));
	REQUIRE(rebuilt);

	auto stmt = make_uniq<LogicalPlanStatement>(std::move(rebuilt));
	auto roundtripped = con.Query(std::move(stmt));
	REQUIRE_FALSE(roundtripped->HasError());

	// `MaterializedQueryResult::ToString` renders rows in storage order;
	// since the SQL pins ordering with ORDER BY, string equality is a
	// sufficient row-multiset + column-order check.
	REQUIRE(roundtripped->ToString() == reference->ToString());
}

} // namespace

// ============================================================================
// Round-trip cases — supported shapes
// ============================================================================

TEST_CASE("outer_yan tree_conversions: 2-relation INNER join roundtrip",
          "[outer_yan][tree_conversions]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	AssertRoundtripMatches(con, "SELECT a.id, b.bx "
	                            "FROM a INNER JOIN b ON a.id = b.id "
	                            "ORDER BY a.id, b.bx");
}

TEST_CASE("outer_yan tree_conversions: 2-relation LEFT join roundtrip",
          "[outer_yan][tree_conversions]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// Exercises LEFT_OUTER. Whether the OJT places A or B as parent depends
	// on the optimizer's chosen build order; either orientation must be
	// rebuilt with semantics-preserving JoinType (RIGHT-flip path covered
	// by the 3-way LEFT-chain case below).
	AssertRoundtripMatches(con, "SELECT a.id, b.bx "
	                            "FROM a LEFT JOIN b ON a.id = b.id "
	                            "ORDER BY a.id, b.bx NULLS FIRST");
}

TEST_CASE("outer_yan tree_conversions: 3-relation chain INNER roundtrip",
          "[outer_yan][tree_conversions]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	AssertRoundtripMatches(con, "SELECT a.id, b.bx, c.cx "
	                            "FROM a "
	                            "INNER JOIN b ON a.id = b.id "
	                            "INNER JOIN c ON b.id = c.id "
	                            "ORDER BY a.id, b.bx, c.cx");
}

TEST_CASE("outer_yan tree_conversions: 3-relation chain LEFT roundtrip "
          "(may force RIGHT-flip in OJT orientation)",
          "[outer_yan][tree_conversions]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// A LEFT JOIN B LEFT JOIN C — when the OJT's degree-driven parent pick
	// places B as the OJT-parent of the (A,B) edge, `edge.kind` must flip
	// from LEFT_OUTER to RIGHT_OUTER so the rebuilt join still preserves A.
	// See `OrientedJoinTypeToOJTEdgeKind` in
	// src/optimizer/outer_yan/tree_conversions.cpp.
	AssertRoundtripMatches(con, "SELECT a.id, b.bx, c.cx "
	                            "FROM a "
	                            "LEFT JOIN b ON a.id = b.id "
	                            "LEFT JOIN c ON a.id = c.id "
	                            "ORDER BY a.id, b.bx NULLS FIRST, c.cx NULLS FIRST");
}

TEST_CASE("outer_yan tree_conversions: single-relation residual filter roundtrip",
          "[outer_yan][tree_conversions]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// `a.ax > 15` is a single-relation residual; OJTToLogicalPlan step 2
	// must consume it onto leaf A (smallest covering set).
	AssertRoundtripMatches(con, "SELECT a.id, b.bx "
	                            "FROM a INNER JOIN b ON a.id = b.id "
	                            "WHERE a.ax > 15 "
	                            "ORDER BY a.id, b.bx");
}

TEST_CASE("outer_yan tree_conversions: multi-relation residual filter roundtrip",
          "[outer_yan][tree_conversions]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// `a.ax + c.cx > 1010` references {A, C} — must land at the lowest
	// fused subplan that covers both (step 3 pushdown), not at the root.
	AssertRoundtripMatches(con, "SELECT a.id, b.bx, c.cx "
	                            "FROM a "
	                            "INNER JOIN b ON a.id = b.id "
	                            "INNER JOIN c ON b.id = c.id "
	                            "WHERE a.ax + c.cx > 1010 "
	                            "ORDER BY a.id, b.bx, c.cx");
}

// ============================================================================
// Documented-unsupported cases — hidden by `[.]`, run with explicit tag.
// ============================================================================

// `LogicalPlanToOJT` throws NotImplementedException when a residual filter's
// `BoundColumnRef`s all resolve to a `table_index` introduced by a
// `LogicalProjection` above the join tree (no registered OJT base relation).
// See tree_conversions.cpp:336-338. Future work: register projection
// table_indices, or push the filter through the projection. Marked hidden so
// the limitation is visible as a TODO without failing the default suite.
TEST_CASE("outer_yan tree_conversions: residual filter via projection (UNSUPPORTED)",
          "[outer_yan][tree_conversions][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// Subquery-induced projection rebinds columns to a fresh table_index;
	// a WHERE on the rebinding column produces a residual filter whose
	// referenced relations are empty from the OJT's point of view.
	const string sql = "SELECT t.id, t.b_total "
	                   "FROM (SELECT a.id AS id, b.bx AS b_total "
	                   "      FROM a INNER JOIN b ON a.id = b.id) AS t "
	                   "WHERE t.b_total > 150 "
	                   "ORDER BY t.id";
	auto plan = con.ExtractPlan(sql);
	REQUIRE(plan);
	REQUIRE_THROWS_AS(LogicalPlanToOJT(std::move(plan)), NotImplementedException);
}
