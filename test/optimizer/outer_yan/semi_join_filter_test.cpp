#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"
#include "duckdb/optimizer/outer_yan/key_bitmap.hpp"
#include "duckdb/optimizer/outer_yan/semi_join_filter.hpp"
#include "duckdb/planner/column_binding.hpp"

using namespace duckdb;

// ============================================================================
// SemiJoinFilter — tagged-union state machine over {UNDECIDED, BITMAP, HASH}.
// Plan 3 contract: kind set once at PhysicalSJBuild::Finalize, no atomics on
// the payload, accessor mismatch is a D_ASSERT (debug-only).
// ============================================================================

TEST_CASE("outer_yan semi_join_filter: fresh instance is UNDECIDED",
          "[outer_yan][semi_join_filter]") {
	SemiJoinFilter f;
	REQUIRE(f.kind() == SemiJoinFilter::Kind::UNDECIDED);
	REQUIRE_FALSE(f.finalized());
	// UNDECIDED defensively reports IsEmpty so a probe before Finalize cannot
	// admit any rows.
	REQUIRE(f.IsEmpty());
}

TEST_CASE("outer_yan semi_join_filter: build/probe binding accumulation",
          "[outer_yan][semi_join_filter]") {
	SemiJoinFilter f;
	REQUIRE(f.BuildBindings().empty());
	REQUIRE(f.ProbeBindings().empty());

	f.AddBuildBinding(ColumnBinding(5, 0));
	f.AddBuildBinding(ColumnBinding(5, 2));
	f.AddProbeBinding(ColumnBinding(7, 1));

	REQUIRE(f.BuildBindings().size() == 2);
	REQUIRE(f.BuildBindings()[0].table_index == 5);
	REQUIRE(f.BuildBindings()[0].column_index == 0);
	REQUIRE(f.BuildBindings()[1].column_index == 2);
	REQUIRE(f.ProbeBindings().size() == 1);
	REQUIRE(f.ProbeBindings()[0].table_index == 7);
}

TEST_CASE("outer_yan semi_join_filter: FinalizeAsHash transitions to HASH and IsEmpty delegates",
          "[outer_yan][semi_join_filter]") {
	SemiJoinFilter f;
	auto hf = make_shared_ptr<HashFilter>();
	REQUIRE(hf->IsEmpty());

	f.FinalizeAsHash(hf);
	REQUIRE(f.kind() == SemiJoinFilter::Kind::HASH);
	REQUIRE(f.finalized());
	REQUIRE(f.IsEmpty());

	// hash_shared() returns the same shared_ptr we handed in.
	auto out = f.hash_shared();
	REQUIRE(out.get() == hf.get());

	// Now insert one row; IsEmpty must flip.
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER}, 1);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::INTEGER(42));
	hf->Build(chunk, {0});
	hf->Finalize();
	REQUIRE_FALSE(f.IsEmpty());
}

TEST_CASE("outer_yan semi_join_filter: FinalizeAsBitmap transitions to BITMAP and IsEmpty delegates",
          "[outer_yan][semi_join_filter]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Build a tiny ColumnDataCollection so KeyBitmap::TryBuild has something
	// to scan.
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::INTEGER};
	ColumnDataCollection cdc(ctx, types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, 4);
	chunk.SetCardinality(3);
	chunk.SetValue(0, 0, Value::INTEGER(1));
	chunk.SetValue(0, 1, Value::INTEGER(2));
	chunk.SetValue(0, 2, Value::INTEGER(3));
	cdc.Append(chunk);

	auto bitmap = KeyBitmap::TryBuild(ctx, cdc, {0}, types, /*byte_budget=*/1024);
	REQUIRE(bitmap);
	REQUIRE(bitmap->PopCount() == 3);

	SemiJoinFilter f;
	f.FinalizeAsBitmap(std::move(bitmap));
	REQUIRE(f.kind() == SemiJoinFilter::Kind::BITMAP);
	REQUIRE(f.finalized());
	REQUIRE_FALSE(f.IsEmpty());
	REQUIRE(f.bitmap().PopCount() == 3);
}

TEST_CASE("outer_yan semi_join_filter: double-finalize throws",
          "[outer_yan][semi_join_filter]") {
	SemiJoinFilter f;
	f.FinalizeAsHash(make_shared_ptr<HashFilter>());
	REQUIRE_THROWS_AS(f.FinalizeAsHash(make_shared_ptr<HashFilter>()), InternalException);

	SemiJoinFilter g;
	g.FinalizeAsHash(make_shared_ptr<HashFilter>());
	// Cross-kind double-finalize also throws.
	REQUIRE_THROWS_AS(g.FinalizeAsBitmap(nullptr), InternalException);
}

TEST_CASE("outer_yan semi_join_filter: empty hash filter reports IsEmpty",
          "[outer_yan][semi_join_filter]") {
	SemiJoinFilter f;
	auto hf = make_shared_ptr<HashFilter>();
	hf->Finalize();
	f.FinalizeAsHash(hf);
	REQUIRE(f.IsEmpty());
}

TEST_CASE("outer_yan semi_join_filter: zero-cardinality bitmap reports IsEmpty",
          "[outer_yan][semi_join_filter]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// Empty ColumnDataCollection — TryBuild emits a zero-range bitmap.
	vector<LogicalType> types {LogicalType::INTEGER};
	ColumnDataCollection cdc(ctx, types);
	auto bitmap = KeyBitmap::TryBuild(ctx, cdc, {0}, types, /*byte_budget=*/1024);
	REQUIRE(bitmap);
	REQUIRE(bitmap->IsEmpty());

	SemiJoinFilter f;
	f.FinalizeAsBitmap(std::move(bitmap));
	REQUIRE(f.IsEmpty());
}
