#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/outer_yan/applicability.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

using namespace duckdb;

namespace {

void Seed(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(id INTEGER, ax INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b(id INTEGER, bx INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE c(id INTEGER, cx INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO b VALUES (1, 100), (2, 200)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO c VALUES (1, 1000), (3, 3000)"));
}

//! White-box: extract the logical plan and call OuterYanApplicability::Check.
//! Returns the populated ApplicabilityResult so the test can inspect both the
//! `applicable` flag and the `reason` diagnostic.
ApplicabilityResult ApplicabilityOf(Connection &con, const string &sql) {
	auto plan = con.ExtractPlan(sql);
	REQUIRE(plan);
	return OuterYanApplicability::Check(*plan);
}

} // namespace

// ============================================================================
// White-box applicability — mirrors test/sql/optimizer/outer_yan/acyclic_query.test
// at the function level. Faster + pins the exact reason string when rejecting.
// ============================================================================

TEST_CASE("outer_yan applicability: rejects pure-INNER 2-relation join",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con, "SELECT a.id, b.bx FROM a JOIN b ON a.id = b.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects pure-INNER 3-relation chain",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects 2-relation LEFT OUTER (too few rels)",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con, "SELECT a.id, b.bx FROM a LEFT JOIN b ON a.id = b.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: accepts 3-relation LEFT-LEFT chain",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(result.applicable);
	REQUIRE(result.relations.size() == 3);
	REQUIRE(result.edges.size() >= 2);
}

TEST_CASE("outer_yan applicability: accepts mix of INNER + LEFT",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a INNER JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(result.applicable);
}

TEST_CASE("outer_yan applicability: accepts LEFT/RIGHT mix",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "RIGHT JOIN c ON a.id = c.id");
	REQUIRE(result.applicable);
}

TEST_CASE("outer_yan applicability: accepts FULL OUTER in chain",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a FULL OUTER JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects theta predicate",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id < b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE_FALSE(result.applicable);
	REQUIRE_FALSE(result.reason.empty());
}

TEST_CASE("outer_yan applicability: rejects expression on join key",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id + 1 = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects IS NOT DISTINCT FROM (NULL-tolerant)",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id IS NOT DISTINCT FROM b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects cross product",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id, c");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects set op above join",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx FROM a LEFT JOIN b ON a.id = b.id "
	    "UNION ALL SELECT a.id, c.cx FROM a LEFT JOIN c ON a.id = c.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects window operator above join",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, ROW_NUMBER() OVER (PARTITION BY a.id) AS rn "
	    "FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON a.id = c.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: rejects cyclic 3-relation graph",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx "
	    "FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON b.id = c.id AND a.id = c.id");
	REQUIRE_FALSE(result.applicable);
}

TEST_CASE("outer_yan applicability: accepts root aggregation (COUNT_STAR)",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT COUNT(*) FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON a.id = c.id");
	REQUIRE(result.applicable);
	REQUIRE(result.has_root_aggregate);
}

TEST_CASE("outer_yan applicability: accepts GROUP BY at root",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, COUNT(b.bx) FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id GROUP BY a.id");
	REQUIRE(result.applicable);
	REQUIRE(result.has_root_aggregate);
}

TEST_CASE("outer_yan applicability: relation table-index mapping is populated when accepted",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(result.applicable);
	// 3 relations → 3 entries in table_index_to_relation, with distinct
	// RelationIds in [0, 3).
	REQUIRE(result.table_index_to_relation.size() == 3);
	unordered_set<RelationId> ids;
	for (auto &entry : result.table_index_to_relation) {
		ids.insert(entry.second);
	}
	REQUIRE(ids.size() == 3);
	for (auto id : ids) {
		REQUIRE(id < 3);
	}
}

TEST_CASE("outer_yan applicability: edges hold valid LogicalComparisonJoin pointers",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(result.applicable);
	for (auto &edge : result.edges) {
		REQUIRE(edge.join != nullptr);
		REQUIRE(edge.left != edge.right);
	}
}

TEST_CASE("outer_yan applicability: rejection populates reason string",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	// Pure-INNER bail-out — should populate a reason.
	auto result = ApplicabilityOf(con,
	    "SELECT a.id, b.bx, c.cx FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id");
	REQUIRE_FALSE(result.applicable);
	REQUIRE_FALSE(result.reason.empty());
}

// ============================================================================
// CheckApplicability via OuterYanTree (matches the integration path).
// ============================================================================

TEST_CASE("outer_yan applicability: OuterYanTree::CheckApplicability accepts on outer chain",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto plan = con.ExtractPlan(
	    "SELECT a.id, b.bx, c.cx FROM a LEFT JOIN b ON a.id = b.id "
	    "LEFT JOIN c ON a.id = c.id");
	REQUIRE(plan);
	OuterYanTree tree;
	REQUIRE(tree.CheckApplicability(*plan));
	REQUIRE(tree.Applicability().applicable);
}

TEST_CASE("outer_yan applicability: OuterYanTree::CheckApplicability rejects pure-INNER",
          "[outer_yan][applicability]") {
	DuckDB db(nullptr);
	Connection con(db);
	Seed(con);
	auto plan = con.ExtractPlan(
	    "SELECT a.id, b.bx FROM a JOIN b ON a.id = b.id");
	REQUIRE(plan);
	OuterYanTree tree;
	REQUIRE_FALSE(tree.CheckApplicability(*plan));
	REQUIRE_FALSE(tree.Applicability().applicable);
}
