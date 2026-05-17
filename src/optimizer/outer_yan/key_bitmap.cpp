#include "duckdb/optimizer/outer_yan/key_bitmap.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

KeyBitmap::KeyBitmap() = default;

namespace {

// Type-erased loader: read a key from `data + idx * sizeof(T)` as int64_t,
// for the physical type T that backs `type_id`. Supported types listed in
// IsBitmapEligibleType below.
template <typename T>
inline int64_t LoadAsInt64(const_data_ptr_t data, idx_t idx) {
	return static_cast<int64_t>(reinterpret_cast<const T *>(data)[idx]);
}

bool IsBitmapEligibleType(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::DATE:
		return true;
	default:
		// UBIGINT / UHUGEINT may overflow int64; defer.
		return false;
	}
}

// Per-row dispatch — small enough to inline at the switch site.
inline int64_t LoadKeyAsInt64(LogicalTypeId id, const_data_ptr_t data, idx_t idx) {
	switch (id) {
	case LogicalTypeId::INTEGER:
		return LoadAsInt64<int32_t>(data, idx);
	case LogicalTypeId::BIGINT:
		return LoadAsInt64<int64_t>(data, idx);
	case LogicalTypeId::UINTEGER:
		return LoadAsInt64<uint32_t>(data, idx);
	case LogicalTypeId::DATE:
		// date_t holds days as int32_t.
		return LoadAsInt64<int32_t>(data, idx);
	default:
		throw InternalException("KeyBitmap: type not bitmap-eligible (should have been gated)");
	}
}

} // namespace

unique_ptr<KeyBitmap> KeyBitmap::TryBuild(ClientContext &context,
                                          const ColumnDataCollection &build_data,
                                          const vector<idx_t> &build_col_ids,
                                          const vector<LogicalType> &build_col_types,
                                          idx_t byte_budget) {
	// 0. Sanity.
	if (build_col_ids.empty() || build_col_ids.size() != build_col_types.size()) {
		return nullptr;
	}
	// v1: single-column only. Composite-key flattening is plan-3 open question 1.
	if (build_col_ids.size() != 1) {
		return nullptr;
	}
	if (byte_budget == 0) {
		// PRAGMA outer_yan_bitmap_max_bytes = 0 forces HashFilter.
		return nullptr;
	}

	// 1. Type gate.
	auto type_id = build_col_types[0].id();
	if (!IsBitmapEligibleType(type_id)) {
		return nullptr;
	}
	auto col_id = build_col_ids[0];

	// 2. First pass — min / max over the non-NULL keys.
	int64_t min_key = NumericLimits<int64_t>::Maximum();
	int64_t max_key = NumericLimits<int64_t>::Minimum();
	idx_t non_null_rows = 0;

	DataChunk scan_chunk;
	build_data.InitializeScanChunk(scan_chunk);

	ColumnDataParallelScanState gstate;
	ColumnDataLocalScanState lstate;
	build_data.InitializeScan(gstate);

	while (build_data.Scan(gstate, lstate, scan_chunk)) {
		auto &v = scan_chunk.data[col_id];
		UnifiedVectorFormat format;
		v.ToUnifiedFormat(scan_chunk.size(), format);
		auto data = const_data_ptr_cast(format.data);
		for (idx_t i = 0; i < scan_chunk.size(); i++) {
			auto idx = format.sel->get_index(i);
			if (!format.validity.RowIsValid(idx)) {
				continue;
			}
			int64_t value = LoadKeyAsInt64(type_id, data, idx);
			if (value < min_key) {
				min_key = value;
			}
			if (value > max_key) {
				max_key = value;
			}
			non_null_rows++;
		}
		scan_chunk.Reset();
	}

	if (non_null_rows == 0) {
		// Build relation is empty (or all-NULL) — still emit a bitmap with
		// zero capacity so the probe correctly filters everything.
		auto bm = make_uniq<KeyBitmap>();
		bm->min_key_ = 0;
		bm->max_key_ = -1;
		bm->bit_count_ = 0;
		bm->pop_count_ = 0;
		bm->key_type_ = build_col_types[0];
		return bm;
	}

	// 3. Range check.
	// (max - min + 1) bits required. Use uint64_t arithmetic since the
	// range can exceed int64_t bounds in pathological cases.
	uint64_t range = static_cast<uint64_t>(max_key - min_key) + 1ULL;
	uint64_t bytes_needed = (range + 7ULL) / 8ULL;
	if (bytes_needed > byte_budget) {
		return nullptr;
	}

	// 4. Allocate.
	uint64_t words_needed = (range + 63ULL) / 64ULL;

	auto bm = make_uniq<KeyBitmap>();
	bm->min_key_ = min_key;
	bm->max_key_ = max_key;
	bm->bit_count_ = NumericCast<idx_t>(words_needed * 64ULL);
	bm->key_type_ = build_col_types[0];

	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &allocator = buffer_manager.GetBufferAllocator();
	// Over-allocate by 64 bytes so we can align the start to a cache line.
	bm->buffer_ = allocator.Allocate(words_needed * sizeof(uint64_t) + 64);
	auto raw = reinterpret_cast<uintptr_t>(bm->buffer_.get());
	auto aligned = (raw + 63ULL) & ~63ULL;
	bm->words_ = reinterpret_cast<uint64_t *>(aligned);
	std::memset(bm->words_, 0, words_needed * sizeof(uint64_t));

	// 5. Second pass — set bits.
	build_data.InitializeScan(gstate);
	while (build_data.Scan(gstate, lstate, scan_chunk)) {
		auto &v = scan_chunk.data[col_id];
		UnifiedVectorFormat format;
		v.ToUnifiedFormat(scan_chunk.size(), format);
		auto data = const_data_ptr_cast(format.data);
		for (idx_t i = 0; i < scan_chunk.size(); i++) {
			auto idx = format.sel->get_index(i);
			if (!format.validity.RowIsValid(idx)) {
				continue;
			}
			int64_t value = LoadKeyAsInt64(type_id, data, idx);
			// Guarded by the min/max from pass 1; bit_idx is always in range.
			uint64_t bit_idx = static_cast<uint64_t>(value - min_key);
			bm->words_[bit_idx >> 6] |= (1ULL << (bit_idx & 63ULL));
		}
		scan_chunk.Reset();
	}

	// 6. Popcount — used by IsEmpty (and one day by selectivity estimation).
	idx_t pop = 0;
	for (uint64_t w = 0; w < words_needed; w++) {
		pop += static_cast<idx_t>(__builtin_popcountll(bm->words_[w]));
	}
	bm->pop_count_ = pop;

	return bm;
}

bool KeyBitmap::Probe(int64_t flat_key) const {
	if (pop_count_ == 0) {
		return false;
	}
	if (flat_key < min_key_ || flat_key > max_key_) {
		return false;
	}
	uint64_t bit_idx = static_cast<uint64_t>(flat_key - min_key_);
	return (words_[bit_idx >> 6] >> (bit_idx & 63ULL)) & 1ULL;
}

void KeyBitmap::ProbeBatch(const DataChunk &input,
                           const vector<idx_t> &probe_col_ids,
                           SelectionVector &sel, idx_t &approved) const {
	approved = 0;
	if (probe_col_ids.empty()) {
		throw InternalException("KeyBitmap::ProbeBatch: no probe columns");
	}
	if (probe_col_ids.size() != 1) {
		throw InternalException("KeyBitmap::ProbeBatch: composite keys unsupported in v1");
	}
	if (pop_count_ == 0) {
		// Build was empty — no probe row survives.
		return;
	}

	auto type_id = key_type_.id();
	auto col_id = probe_col_ids[0];
	auto &v = input.data[col_id];

	UnifiedVectorFormat format;
	v.ToUnifiedFormat(input.size(), format);
	auto data = const_data_ptr_cast(format.data);

	for (idx_t i = 0; i < input.size(); i++) {
		auto idx = format.sel->get_index(i);
		// NULL probe key never matches (semi-join semantics).
		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		int64_t value = LoadKeyAsInt64(type_id, data, idx);
		// Out-of-range definitively means "not in build set" — no hash fallback.
		if (value < min_key_ || value > max_key_) {
			continue;
		}
		uint64_t bit_idx = static_cast<uint64_t>(value - min_key_);
		uint64_t hit = (words_[bit_idx >> 6] >> (bit_idx & 63ULL)) & 1ULL;
		if (hit) {
			sel.set_index(approved++, i);
		}
	}
}

} // namespace duckdb
