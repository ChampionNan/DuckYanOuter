#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"

using namespace duckdb;

namespace {

DataChunk MakeIntChunk(const vector<int32_t> &values) {
	vector<LogicalType> types {LogicalType::INTEGER};
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, std::max<idx_t>(1, values.size()));
	chunk.SetCardinality(values.size());
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(0, i, Value::INTEGER(values[i]));
	}
	return chunk;
}

DataChunk MakeIntChunkWithNulls(const vector<Value> &values) {
	vector<LogicalType> types {LogicalType::INTEGER};
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, std::max<idx_t>(1, values.size()));
	chunk.SetCardinality(values.size());
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(0, i, values[i]);
	}
	return chunk;
}

DataChunk MakeStringChunk(const vector<string> &values) {
	vector<LogicalType> types {LogicalType::VARCHAR};
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, std::max<idx_t>(1, values.size()));
	chunk.SetCardinality(values.size());
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(0, i, Value(values[i]));
	}
	return chunk;
}

DataChunk MakeIntStringChunk(const vector<int32_t> &ints, const vector<string> &strs) {
	vector<LogicalType> types {LogicalType::INTEGER, LogicalType::VARCHAR};
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, std::max<idx_t>(1, ints.size()));
	chunk.SetCardinality(ints.size());
	for (idx_t i = 0; i < ints.size(); i++) {
		chunk.SetValue(0, i, Value::INTEGER(ints[i]));
		chunk.SetValue(1, i, Value(strs[i]));
	}
	return chunk;
}

} // namespace

// ============================================================================
// HashFilter — open-addressed exact set-membership over Value tuples.
// ============================================================================

TEST_CASE("outer_yan hash_filter: fresh filter is empty",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	REQUIRE(f.Count() == 0);
	REQUIRE(f.IsEmpty());
	REQUIRE_FALSE(f.IsFinalized());
	// Capacity is power-of-2, pre-allocated.
	REQUIRE(f.Capacity() == 64);
}

TEST_CASE("outer_yan hash_filter: build + Contains round-trip on single-column INTEGER",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto chunk = MakeIntChunk({1, 2, 3});
	f.Build(chunk, {0});
	f.Finalize();

	REQUIRE(f.Count() == 3);
	REQUIRE_FALSE(f.IsEmpty());
	REQUIRE(f.IsFinalized());

	REQUIRE(f.Contains({Value::INTEGER(1)}));
	REQUIRE(f.Contains({Value::INTEGER(2)}));
	REQUIRE(f.Contains({Value::INTEGER(3)}));
	REQUIRE_FALSE(f.Contains({Value::INTEGER(0)}));
	REQUIRE_FALSE(f.Contains({Value::INTEGER(4)}));
}

TEST_CASE("outer_yan hash_filter: empty filter Contains is false for any key",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	f.Finalize();
	REQUIRE(f.IsEmpty());
	REQUIRE_FALSE(f.Contains({Value::INTEGER(0)}));
	REQUIRE_FALSE(f.Contains({Value(LogicalType::INTEGER)}));
}

TEST_CASE("outer_yan hash_filter: duplicate inserts collapse to one key (set semantics)",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto chunk = MakeIntChunk({5, 5, 5, 7, 5});
	f.Build(chunk, {0});
	REQUIRE(f.Count() == 2);
	REQUIRE(f.Contains({Value::INTEGER(5)}));
	REQUIRE(f.Contains({Value::INTEGER(7)}));
}

TEST_CASE("outer_yan hash_filter: NULL keys skipped at build time",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto chunk = MakeIntChunkWithNulls({
	    Value::INTEGER(10),
	    Value(LogicalType::INTEGER),
	    Value::INTEGER(20),
	    Value(LogicalType::INTEGER),
	});
	f.Build(chunk, {0});
	REQUIRE(f.Count() == 2);
	REQUIRE(f.Contains({Value::INTEGER(10)}));
	REQUIRE(f.Contains({Value::INTEGER(20)}));
}

TEST_CASE("outer_yan hash_filter: NULL probe key never matches",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto chunk = MakeIntChunk({1});
	f.Build(chunk, {0});
	REQUIRE_FALSE(f.Contains({Value(LogicalType::INTEGER)}));
}

TEST_CASE("outer_yan hash_filter: VARCHAR keys",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto chunk = MakeStringChunk({"alpha", "beta", "gamma"});
	f.Build(chunk, {0});
	REQUIRE(f.Count() == 3);
	REQUIRE(f.Contains({Value("alpha")}));
	REQUIRE(f.Contains({Value("gamma")}));
	REQUIRE_FALSE(f.Contains({Value("delta")}));
	REQUIRE_FALSE(f.Contains({Value("")}));
}

TEST_CASE("outer_yan hash_filter: composite (INTEGER + VARCHAR) keys",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto chunk = MakeIntStringChunk({1, 1, 2}, {"a", "b", "a"});
	f.Build(chunk, {0, 1});
	REQUIRE(f.Count() == 3);

	REQUIRE(f.Contains({Value::INTEGER(1), Value("a")}));
	REQUIRE(f.Contains({Value::INTEGER(1), Value("b")}));
	REQUIRE(f.Contains({Value::INTEGER(2), Value("a")}));
	REQUIRE_FALSE(f.Contains({Value::INTEGER(2), Value("b")}));
	REQUIRE_FALSE(f.Contains({Value::INTEGER(3), Value("a")}));
}

TEST_CASE("outer_yan hash_filter: composite key with one NULL column skipped",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	vector<LogicalType> types {LogicalType::INTEGER, LogicalType::VARCHAR};
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, 3);
	chunk.SetCardinality(3);
	chunk.SetValue(0, 0, Value::INTEGER(1));
	chunk.SetValue(1, 0, Value("a"));
	chunk.SetValue(0, 1, Value(LogicalType::INTEGER));  // first col NULL
	chunk.SetValue(1, 1, Value("b"));
	chunk.SetValue(0, 2, Value::INTEGER(3));
	chunk.SetValue(1, 2, Value(LogicalType::VARCHAR));  // second col NULL

	f.Build(chunk, {0, 1});
	REQUIRE(f.Count() == 1);
	REQUIRE(f.Contains({Value::INTEGER(1), Value("a")}));
}

TEST_CASE("outer_yan hash_filter: Build after Finalize throws",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto c1 = MakeIntChunk({1});
	f.Build(c1, {0});
	f.Finalize();

	auto c2 = MakeIntChunk({2});
	REQUIRE_THROWS_AS(f.Build(c2, {0}), InternalException);
}

TEST_CASE("outer_yan hash_filter: Merge after Finalize throws",
          "[outer_yan][hash_filter]") {
	HashFilter dst, src;
	auto c = MakeIntChunk({1});
	dst.Build(c, {0});
	dst.Finalize();

	REQUIRE_THROWS_AS(dst.Merge(src), InternalException);
}

TEST_CASE("outer_yan hash_filter: Build with empty build_col_ids throws",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto c = MakeIntChunk({1});
	REQUIRE_THROWS_AS(f.Build(c, {}), InternalException);
}

TEST_CASE("outer_yan hash_filter: load-factor stress — many resizes preserve all keys",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	// 10000 distinct keys triggers many resizes (starting capacity 64).
	const int32_t N = 10000;
	vector<int32_t> values;
	values.reserve(N);
	for (int32_t i = 0; i < N; i++) {
		values.push_back(i);
	}
	auto chunk = MakeIntChunk(values);
	f.Build(chunk, {0});
	f.Finalize();
	REQUIRE(f.Count() == static_cast<idx_t>(N));

	// Capacity must always satisfy 4*count <= 3*cap (load factor 3/4).
	REQUIRE(f.Count() * 4 <= f.Capacity() * 3);

	// Spot-check membership.
	for (int32_t k : {0, 1, 7, 63, 64, 1000, 9999}) {
		REQUIRE(f.Contains({Value::INTEGER(k)}));
	}
	REQUIRE_FALSE(f.Contains({Value::INTEGER(-1)}));
	REQUIRE_FALSE(f.Contains({Value::INTEGER(N)}));
}

TEST_CASE("outer_yan hash_filter: Merge union of two disjoint thread-local filters",
          "[outer_yan][hash_filter]") {
	HashFilter dst, src;
	auto c1 = MakeIntChunk({1, 2, 3});
	auto c2 = MakeIntChunk({4, 5, 6});
	dst.Build(c1, {0});
	src.Build(c2, {0});

	dst.Merge(src);
	REQUIRE(dst.Count() == 6);
	for (int32_t k = 1; k <= 6; k++) {
		REQUIRE(dst.Contains({Value::INTEGER(k)}));
	}
	// `src` is reset to a fresh empty filter.
	REQUIRE(src.Count() == 0);
	REQUIRE(src.IsEmpty());
}

TEST_CASE("outer_yan hash_filter: Merge with overlapping keys = union cardinality",
          "[outer_yan][hash_filter]") {
	HashFilter dst, src;
	auto c1 = MakeIntChunk({1, 2, 3, 4});
	auto c2 = MakeIntChunk({3, 4, 5, 6});
	dst.Build(c1, {0});
	src.Build(c2, {0});

	dst.Merge(src);
	REQUIRE(dst.Count() == 6);
	for (int32_t k = 1; k <= 6; k++) {
		REQUIRE(dst.Contains({Value::INTEGER(k)}));
	}
}

TEST_CASE("outer_yan hash_filter: Merge with empty src is no-op",
          "[outer_yan][hash_filter]") {
	HashFilter dst, src;
	auto c1 = MakeIntChunk({1, 2});
	dst.Build(c1, {0});
	dst.Merge(src);
	REQUIRE(dst.Count() == 2);
}

TEST_CASE("outer_yan hash_filter: Merge into empty dst",
          "[outer_yan][hash_filter]") {
	HashFilter dst, src;
	auto c = MakeIntChunk({10, 20});
	src.Build(c, {0});
	dst.Merge(src);
	REQUIRE(dst.Count() == 2);
	REQUIRE(dst.Contains({Value::INTEGER(10)}));
	REQUIRE(dst.Contains({Value::INTEGER(20)}));
	REQUIRE(src.IsEmpty());
}

TEST_CASE("outer_yan hash_filter: MurmurHash64A is deterministic and order-sensitive",
          "[outer_yan][hash_filter]") {
	const char *a = "abcdefgh";
	const char *b = "abcdefgh";
	const char *c = "abcdefgi";

	REQUIRE(HashFilter::MurmurHash64A(a, 8) == HashFilter::MurmurHash64A(b, 8));
	REQUIRE(HashFilter::MurmurHash64A(a, 8) != HashFilter::MurmurHash64A(c, 8));

	// Different seeds → different digests.
	REQUIRE(HashFilter::MurmurHash64A(a, 8, /*seed=*/0) !=
	        HashFilter::MurmurHash64A(a, 8, /*seed=*/1));
}

TEST_CASE("outer_yan hash_filter: HashKey composes per-column hashes",
          "[outer_yan][hash_filter]") {
	auto h1 = HashFilter::HashKey({Value::INTEGER(1), Value("a")});
	auto h2 = HashFilter::HashKey({Value::INTEGER(1), Value("a")});
	auto h3 = HashFilter::HashKey({Value("a"), Value::INTEGER(1)});
	REQUIRE(h1 == h2);
	// Order matters (Value::Hash() type-mixes, then MurmurHash over concat).
	REQUIRE(h1 != h3);
}

TEST_CASE("outer_yan hash_filter_kernel: filter narrows selection vector",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto build_chunk = MakeIntChunk({1, 3, 5});
	f.Build(build_chunk, {0});
	f.Finalize();

	// Probe chunk: {2, 1, 5, 4, 3} → expected survivors at indices 1, 2, 4.
	auto probe = MakeIntChunk({2, 1, 5, 4, 3});
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 0;
	HashFilterUseKernel::filter(probe, {0}, f, sel, approved);

	REQUIRE(approved == 3);
	REQUIRE(sel.get_index(0) == 1);
	REQUIRE(sel.get_index(1) == 2);
	REQUIRE(sel.get_index(2) == 4);
}

TEST_CASE("outer_yan hash_filter_kernel: empty filter survives no rows",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	f.Finalize();
	auto probe = MakeIntChunk({1, 2, 3});
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 99;
	HashFilterUseKernel::filter(probe, {0}, f, sel, approved);
	REQUIRE(approved == 0);
}

TEST_CASE("outer_yan hash_filter_kernel: NULL probe keys do not survive",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto build_chunk = MakeIntChunk({1, 2, 3});
	f.Build(build_chunk, {0});
	f.Finalize();

	auto probe = MakeIntChunkWithNulls({
	    Value::INTEGER(1),
	    Value(LogicalType::INTEGER),
	    Value::INTEGER(3),
	    Value(LogicalType::INTEGER),
	});
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 0;
	HashFilterUseKernel::filter(probe, {0}, f, sel, approved);
	REQUIRE(approved == 2);
	REQUIRE(sel.get_index(0) == 0);
	REQUIRE(sel.get_index(1) == 2);
}

TEST_CASE("outer_yan hash_filter_kernel: empty probe_col_ids throws",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto c = MakeIntChunk({1});
	f.Build(c, {0});
	f.Finalize();
	auto probe = MakeIntChunk({1});
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 0;
	REQUIRE_THROWS_AS(HashFilterUseKernel::filter(probe, {}, f, sel, approved), InternalException);
}

TEST_CASE("outer_yan hash_filter_kernel: composite-key probe filtering",
          "[outer_yan][hash_filter]") {
	HashFilter f;
	auto build = MakeIntStringChunk({1, 2, 3}, {"a", "b", "c"});
	f.Build(build, {0, 1});
	f.Finalize();

	// Probe: {(1,a), (1,b), (2,b), (3,c), (4,a)} → survivors at 0, 2, 3.
	auto probe = MakeIntStringChunk({1, 1, 2, 3, 4}, {"a", "b", "b", "c", "a"});
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 0;
	HashFilterUseKernel::filter(probe, {0, 1}, f, sel, approved);
	REQUIRE(approved == 3);
	REQUIRE(sel.get_index(0) == 0);
	REQUIRE(sel.get_index(1) == 2);
	REQUIRE(sel.get_index(2) == 3);
}
