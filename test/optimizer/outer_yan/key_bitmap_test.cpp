#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/outer_yan/key_bitmap.hpp"

using namespace duckdb;

namespace {

//! Materialise a single-column `int32` ColumnDataCollection from the given values.
//! NULLs in `values` are stored as Value() (typed-NULL via the column's type).
template <typename Builder>
ColumnDataCollection MakeCDC(ClientContext &ctx, const vector<LogicalType> &types,
                             idx_t row_count, Builder &&fill) {
	ColumnDataCollection cdc(ctx, types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, std::max<idx_t>(1, row_count));
	chunk.SetCardinality(row_count);
	fill(chunk);
	cdc.Append(chunk);
	return cdc;
}

ColumnDataCollection MakeIntCDC(ClientContext &ctx, const vector<int32_t> &values) {
	vector<LogicalType> types {LogicalType::INTEGER};
	return MakeCDC(ctx, types, values.size(), [&](DataChunk &chunk) {
		for (idx_t i = 0; i < values.size(); i++) {
			chunk.SetValue(0, i, Value::INTEGER(values[i]));
		}
	});
}

ColumnDataCollection MakeBigIntCDC(ClientContext &ctx, const vector<int64_t> &values) {
	vector<LogicalType> types {LogicalType::BIGINT};
	return MakeCDC(ctx, types, values.size(), [&](DataChunk &chunk) {
		for (idx_t i = 0; i < values.size(); i++) {
			chunk.SetValue(0, i, Value::BIGINT(values[i]));
		}
	});
}

} // namespace

// ============================================================================
// KeyBitmap — single-column dense bit-index over integer / date keys.
// ============================================================================

TEST_CASE("outer_yan key_bitmap: builds and probes contiguous integer range",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto cdc = MakeIntCDC(*con.context, {1, 2, 3, 5, 7});
	vector<LogicalType> types {LogicalType::INTEGER};

	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, /*byte_budget=*/1024);
	REQUIRE(bm);
	REQUIRE(bm->MinKey() == 1);
	REQUIRE(bm->MaxKey() == 7);
	REQUIRE(bm->PopCount() == 5);
	REQUIRE_FALSE(bm->IsEmpty());

	for (int32_t k : {1, 2, 3, 5, 7}) {
		REQUIRE(bm->Probe(k));
	}
	for (int32_t k : {-1, 0, 4, 6, 8, 100}) {
		REQUIRE_FALSE(bm->Probe(k));
	}
}

TEST_CASE("outer_yan key_bitmap: out-of-range probe never matches",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {50, 51, 52});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE_FALSE(bm->Probe(49));
	REQUIRE_FALSE(bm->Probe(53));
	// Even probes near INT64 bounds must not crash.
	REQUIRE_FALSE(bm->Probe(NumericLimits<int64_t>::Minimum()));
	REQUIRE_FALSE(bm->Probe(NumericLimits<int64_t>::Maximum()));
}

TEST_CASE("outer_yan key_bitmap: empty build emits zero-popcount bitmap",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->PopCount() == 0);
	REQUIRE(bm->IsEmpty());
	REQUIRE_FALSE(bm->Probe(0));
}

TEST_CASE("outer_yan key_bitmap: all-NULL build behaves as empty",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::INTEGER};
	auto cdc = MakeCDC(ctx, types, 3, [](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value(LogicalType::INTEGER));
		chunk.SetValue(0, 1, Value(LogicalType::INTEGER));
		chunk.SetValue(0, 2, Value(LogicalType::INTEGER));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->IsEmpty());
}

TEST_CASE("outer_yan key_bitmap: NULL keys skipped, non-NULL keys indexed",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::INTEGER};
	auto cdc = MakeCDC(ctx, types, 5, [](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value::INTEGER(10));
		chunk.SetValue(0, 1, Value(LogicalType::INTEGER));
		chunk.SetValue(0, 2, Value::INTEGER(12));
		chunk.SetValue(0, 3, Value(LogicalType::INTEGER));
		chunk.SetValue(0, 4, Value::INTEGER(15));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->MinKey() == 10);
	REQUIRE(bm->MaxKey() == 15);
	REQUIRE(bm->PopCount() == 3);
	REQUIRE(bm->Probe(10));
	REQUIRE(bm->Probe(12));
	REQUIRE(bm->Probe(15));
	REQUIRE_FALSE(bm->Probe(11));
	REQUIRE_FALSE(bm->Probe(13));
	REQUIRE_FALSE(bm->Probe(14));
}

TEST_CASE("outer_yan key_bitmap: single-row build sets exactly one bit",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {42});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->MinKey() == 42);
	REQUIRE(bm->MaxKey() == 42);
	REQUIRE(bm->PopCount() == 1);
	REQUIRE(bm->Probe(42));
	REQUIRE_FALSE(bm->Probe(41));
	REQUIRE_FALSE(bm->Probe(43));
}

TEST_CASE("outer_yan key_bitmap: word-boundary keys (63, 64, 65, 128 ...)",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Force keys to span two 64-bit words: 0 (word 0 bit 0) and 64 (word 1 bit 0).
	auto cdc = MakeIntCDC(*con.context, {0, 63, 64, 65, 127, 128});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->MinKey() == 0);
	REQUIRE(bm->MaxKey() == 128);
	REQUIRE(bm->PopCount() == 6);
	for (int32_t k : {0, 63, 64, 65, 127, 128}) {
		REQUIRE(bm->Probe(k));
	}
	for (int32_t k : {1, 62, 66, 126, 129}) {
		REQUIRE_FALSE(bm->Probe(k));
	}
}

TEST_CASE("outer_yan key_bitmap: negative integer keys",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {-3, -1, 0, 1, 3});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->MinKey() == -3);
	REQUIRE(bm->MaxKey() == 3);
	REQUIRE(bm->PopCount() == 5);
	REQUIRE(bm->Probe(-3));
	REQUIRE_FALSE(bm->Probe(-2));
	REQUIRE(bm->Probe(0));
	REQUIRE_FALSE(bm->Probe(-4));
}

TEST_CASE("outer_yan key_bitmap: BIGINT type dispatch",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeBigIntCDC(*con.context, {1000000000LL, 1000000005LL, 1000000010LL});
	vector<LogicalType> types {LogicalType::BIGINT};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->MinKey() == 1000000000LL);
	REQUIRE(bm->MaxKey() == 1000000010LL);
	REQUIRE(bm->Probe(1000000000LL));
	REQUIRE(bm->Probe(1000000005LL));
	REQUIRE_FALSE(bm->Probe(1000000003LL));
}

TEST_CASE("outer_yan key_bitmap: UINTEGER type dispatch",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::UINTEGER};
	auto cdc = MakeCDC(ctx, types, 3, [](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value::UINTEGER(100));
		chunk.SetValue(0, 1, Value::UINTEGER(105));
		chunk.SetValue(0, 2, Value::UINTEGER(110));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->Probe(100));
	REQUIRE(bm->Probe(105));
	REQUIRE_FALSE(bm->Probe(101));
}

TEST_CASE("outer_yan key_bitmap: DATE type dispatch",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::DATE};
	auto d1 = Date::FromString("2020-01-01");
	auto d2 = Date::FromString("2020-01-15");
	auto d3 = Date::FromString("2020-02-01");
	auto cdc = MakeCDC(ctx, types, 3, [&](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value::DATE(d1));
		chunk.SetValue(0, 1, Value::DATE(d2));
		chunk.SetValue(0, 2, Value::DATE(d3));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->Probe(d1.days));
	REQUIRE(bm->Probe(d2.days));
	REQUIRE(bm->Probe(d3.days));
	REQUIRE_FALSE(bm->Probe(Date::FromString("2020-01-02").days));
}

TEST_CASE("outer_yan key_bitmap: VARCHAR rejected — TryBuild returns nullptr",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::VARCHAR};
	auto cdc = MakeCDC(ctx, types, 2, [](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value("foo"));
		chunk.SetValue(0, 1, Value("bar"));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0}, types, 1024);
	REQUIRE(bm == nullptr);
}

TEST_CASE("outer_yan key_bitmap: DECIMAL rejected — TryBuild returns nullptr",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::DECIMAL(10, 2)};
	auto cdc = MakeCDC(ctx, types, 2, [&](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value::DECIMAL((int64_t)100, (uint8_t)10, (uint8_t)2));
		chunk.SetValue(0, 1, Value::DECIMAL((int64_t)200, (uint8_t)10, (uint8_t)2));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0}, types, 1024);
	REQUIRE(bm == nullptr);
}

TEST_CASE("outer_yan key_bitmap: UBIGINT rejected (overflow-prone)",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::UBIGINT};
	auto cdc = MakeCDC(ctx, types, 2, [](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value::UBIGINT(1));
		chunk.SetValue(0, 1, Value::UBIGINT(2));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0}, types, 1024);
	REQUIRE(bm == nullptr);
}

TEST_CASE("outer_yan key_bitmap: composite key rejected (v1 single-column only)",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;
	vector<LogicalType> types {LogicalType::INTEGER, LogicalType::INTEGER};
	auto cdc = MakeCDC(ctx, types, 2, [](DataChunk &chunk) {
		chunk.SetValue(0, 0, Value::INTEGER(1));
		chunk.SetValue(1, 0, Value::INTEGER(10));
		chunk.SetValue(0, 1, Value::INTEGER(2));
		chunk.SetValue(1, 1, Value::INTEGER(20));
	});
	auto bm = KeyBitmap::TryBuild(ctx, cdc, {0, 1}, types, 1024);
	REQUIRE(bm == nullptr);
}

TEST_CASE("outer_yan key_bitmap: byte_budget = 0 always returns nullptr",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {1, 2, 3});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, /*byte_budget=*/0);
	REQUIRE(bm == nullptr);
}

TEST_CASE("outer_yan key_bitmap: budget boundary — fits exactly vs overflows by 1",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Keys spanning [0, 79] need ceil(80/8) = 10 bytes.
	auto cdc = MakeIntCDC(*con.context, {0, 79});
	vector<LogicalType> types {LogicalType::INTEGER};

	auto bm_fit = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, /*byte_budget=*/10);
	REQUIRE(bm_fit);
	REQUIRE(bm_fit->MinKey() == 0);
	REQUIRE(bm_fit->MaxKey() == 79);

	auto bm_over = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, /*byte_budget=*/9);
	REQUIRE(bm_over == nullptr);
}

TEST_CASE("outer_yan key_bitmap: ProbeBatch narrows selection vector",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {1, 3, 5});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);

	// Probe chunk: {2, 1, 5, 4, 3} → expected survivors at original indices 1, 2, 4.
	DataChunk probe;
	probe.Initialize(Allocator::DefaultAllocator(), types, 5);
	probe.SetCardinality(5);
	probe.SetValue(0, 0, Value::INTEGER(2));
	probe.SetValue(0, 1, Value::INTEGER(1));
	probe.SetValue(0, 2, Value::INTEGER(5));
	probe.SetValue(0, 3, Value::INTEGER(4));
	probe.SetValue(0, 4, Value::INTEGER(3));

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 0;
	bm->ProbeBatch(probe, {0}, sel, approved);

	REQUIRE(approved == 3);
	REQUIRE(sel.get_index(0) == 1);
	REQUIRE(sel.get_index(1) == 2);
	REQUIRE(sel.get_index(2) == 4);
}

TEST_CASE("outer_yan key_bitmap: ProbeBatch with all out-of-range keys filters everything",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {100, 101, 102});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);

	DataChunk probe;
	probe.Initialize(Allocator::DefaultAllocator(), types, 4);
	probe.SetCardinality(4);
	for (idx_t i = 0; i < 4; i++) {
		probe.SetValue(0, i, Value::INTEGER(static_cast<int32_t>(i)));
	}
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 100;
	bm->ProbeBatch(probe, {0}, sel, approved);
	REQUIRE(approved == 0);
}

TEST_CASE("outer_yan key_bitmap: ProbeBatch on empty bitmap survives no rows",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->IsEmpty());

	DataChunk probe;
	probe.Initialize(Allocator::DefaultAllocator(), types, 3);
	probe.SetCardinality(3);
	probe.SetValue(0, 0, Value::INTEGER(1));
	probe.SetValue(0, 1, Value::INTEGER(2));
	probe.SetValue(0, 2, Value::INTEGER(3));
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 100;
	bm->ProbeBatch(probe, {0}, sel, approved);
	REQUIRE(approved == 0);
}

TEST_CASE("outer_yan key_bitmap: ProbeBatch with NULL probe keys skips them",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {1, 2, 3});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);

	DataChunk probe;
	probe.Initialize(Allocator::DefaultAllocator(), types, 4);
	probe.SetCardinality(4);
	probe.SetValue(0, 0, Value::INTEGER(1));
	probe.SetValue(0, 1, Value(LogicalType::INTEGER));
	probe.SetValue(0, 2, Value::INTEGER(3));
	probe.SetValue(0, 3, Value(LogicalType::INTEGER));

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 0;
	bm->ProbeBatch(probe, {0}, sel, approved);
	REQUIRE(approved == 2);
	REQUIRE(sel.get_index(0) == 0);
	REQUIRE(sel.get_index(1) == 2);
}

TEST_CASE("outer_yan key_bitmap: ProbeBatch with empty probe_col_ids throws",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {1, 2, 3});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);

	DataChunk probe;
	probe.Initialize(Allocator::DefaultAllocator(), types, 1);
	probe.SetCardinality(1);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t approved = 0;
	REQUIRE_THROWS_AS(bm->ProbeBatch(probe, {}, sel, approved), InternalException);
	REQUIRE_THROWS_AS(bm->ProbeBatch(probe, {0, 0}, sel, approved), InternalException);
}

TEST_CASE("outer_yan key_bitmap: empty / mismatched build_col_ids → nullptr",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {1, 2});
	vector<LogicalType> types {LogicalType::INTEGER};

	// Empty build_col_ids.
	REQUIRE(KeyBitmap::TryBuild(*con.context, cdc, {}, types, 1024) == nullptr);

	// Size mismatch: ids says 2 cols, types says 1.
	REQUIRE(KeyBitmap::TryBuild(*con.context, cdc, {0, 0}, types, 1024) == nullptr);
}

TEST_CASE("outer_yan key_bitmap: large range across many words",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	// 1024 keys [0..1023] → 128 bytes; well within 1024.
	vector<int32_t> values;
	for (int32_t i = 0; i < 1024; i++) {
		values.push_back(i);
	}
	auto cdc = MakeIntCDC(*con.context, values);
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->PopCount() == 1024);
	REQUIRE(bm->MinKey() == 0);
	REQUIRE(bm->MaxKey() == 1023);
	// Probe every value.
	for (int32_t k = 0; k < 1024; k++) {
		REQUIRE(bm->Probe(k));
	}
	REQUIRE_FALSE(bm->Probe(-1));
	REQUIRE_FALSE(bm->Probe(1024));
}

TEST_CASE("outer_yan key_bitmap: sparse range still indexed if budget allows",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	// {0, 999} — range is 1000 bits = 125 bytes.
	auto cdc = MakeIntCDC(*con.context, {0, 999});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, /*byte_budget=*/256);
	REQUIRE(bm);
	REQUIRE(bm->PopCount() == 2);
	REQUIRE(bm->Probe(0));
	REQUIRE(bm->Probe(999));
	REQUIRE_FALSE(bm->Probe(500));
	// Same data with budget=100 must reject.
	auto bm_small = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, /*byte_budget=*/100);
	REQUIRE(bm_small == nullptr);
}

TEST_CASE("outer_yan key_bitmap: duplicate keys in build set produce a single bit",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	// 1 appears 4 times, 2 appears 1 time → popcount must be 2, not 5.
	auto cdc = MakeIntCDC(*con.context, {1, 1, 1, 2, 1});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->PopCount() == 2);
}

TEST_CASE("outer_yan key_bitmap: SizeBytes matches bit_count_/8",
          "[outer_yan][key_bitmap]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto cdc = MakeIntCDC(*con.context, {0, 100});
	vector<LogicalType> types {LogicalType::INTEGER};
	auto bm = KeyBitmap::TryBuild(*con.context, cdc, {0}, types, 1024);
	REQUIRE(bm);
	REQUIRE(bm->BitCount() % 64 == 0);
	REQUIRE(bm->SizeBytes() == bm->BitCount() / 8);
}
