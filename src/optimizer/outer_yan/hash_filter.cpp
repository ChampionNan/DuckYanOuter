#include "duckdb/optimizer/outer_yan/hash_filter.hpp"

#include "duckdb/common/exception.hpp"

#include <cstring>

namespace duckdb {

// =============================================================================
// MurmurHash64A (Austin Appleby, public domain)
// =============================================================================
//
// Reference: https://github.com/aappleby/smhasher (MurmurHash2 family). Same
// digest used by Apache Arrow Acero and various DuckDB internals for byte-
// sequence hashing. 64-bit output, designed for x86-64 (little-endian
// optimised) but works on big-endian via the byte-swap tail.
hash_t HashFilter::MurmurHash64A(const void *data, idx_t len, uint64_t seed) {
	constexpr uint64_t m = 0xc6a4a7935bd1e995ULL;
	constexpr int r = 47;

	uint64_t h = seed ^ (static_cast<uint64_t>(len) * m);

	auto data8 = reinterpret_cast<const uint8_t *>(data);
	while (len >= 8) {
		uint64_t k;
		std::memcpy(&k, data8, sizeof(k));
		k *= m;
		k ^= k >> r;
		k *= m;
		h ^= k;
		h *= m;
		data8 += 8;
		len -= 8;
	}

	switch (len) {
	case 7:
		h ^= static_cast<uint64_t>(data8[6]) << 48; // fall through
	case 6:
		h ^= static_cast<uint64_t>(data8[5]) << 40; // fall through
	case 5:
		h ^= static_cast<uint64_t>(data8[4]) << 32; // fall through
	case 4:
		h ^= static_cast<uint64_t>(data8[3]) << 24; // fall through
	case 3:
		h ^= static_cast<uint64_t>(data8[2]) << 16; // fall through
	case 2:
		h ^= static_cast<uint64_t>(data8[1]) << 8; // fall through
	case 1:
		h ^= static_cast<uint64_t>(data8[0]);
		h *= m;
	default:
		break;
	}

	h ^= h >> r;
	h *= m;
	h ^= h >> r;
	return h;
}

hash_t HashFilter::HashKey(const vector<Value> &key) {
	// Concatenate per-column Value::Hash() digests then MurmurHash64A over
	// the result. Value::Hash() already type-mixes, so this composes two
	// good hashes — overkill cryptographically but cheap and decorrelates
	// per-column collisions from full-tuple collisions.
	vector<hash_t> per_col;
	per_col.reserve(key.size());
	for (auto &v : key) {
		per_col.emplace_back(v.Hash());
	}
	return MurmurHash64A(per_col.data(), per_col.size() * sizeof(hash_t));
}

bool HashFilter::KeysEqual(const vector<Value> &a, const vector<Value> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.size(); i++) {
		if (!(a[i] == b[i])) {
			return false;
		}
	}
	return true;
}

HashFilter::HashFilter() {
	// Pre-allocate the slot array so the hot path never sees an empty table.
	slots_.assign(INITIAL_CAPACITY, Slot {0, EMPTY_KEY_INDEX});
	mask_ = INITIAL_CAPACITY - 1;
}

HashFilter::~HashFilter() = default;

void HashFilter::EnsureCapacityFor(idx_t additional) {
	// Load factor 3/4 — check via integer arithmetic to avoid FP.
	while ((count_ + additional) * 4 > Capacity() * 3) {
		Resize(Capacity() * 2);
	}
}

void HashFilter::Resize(idx_t new_capacity) {
	D_ASSERT((new_capacity & (new_capacity - 1)) == 0); // power of 2
	vector<Slot> new_slots;
	new_slots.assign(new_capacity, Slot {0, EMPTY_KEY_INDEX});
	idx_t new_mask = new_capacity - 1;

	// Rehash by reading existing slots' stored hashes (we kept them).
	for (auto &slot : slots_) {
		if (slot.key_index == EMPTY_KEY_INDEX) {
			continue;
		}
		idx_t idx = slot.hash & new_mask;
		while (new_slots[idx].key_index != EMPTY_KEY_INDEX) {
			idx = (idx + 1) & new_mask;
		}
		new_slots[idx] = slot;
	}
	slots_ = std::move(new_slots);
	mask_ = new_mask;
}

void HashFilter::InsertHashedKey(vector<Value> key, hash_t hash) {
	EnsureCapacityFor(1);
	idx_t idx = hash & mask_;
	while (true) {
		auto &slot = slots_[idx];
		if (slot.key_index == EMPTY_KEY_INDEX) {
			stored_keys_.emplace_back(std::move(key));
			slot.hash = hash;
			slot.key_index = stored_keys_.size() - 1;
			count_++;
			return;
		}
		// Same hash + same key tuple → already present (set semantics).
		if (slot.hash == hash && KeysEqual(stored_keys_[slot.key_index], key)) {
			return;
		}
		idx = (idx + 1) & mask_;
	}
}

void HashFilter::Build(const DataChunk &chunk, const vector<idx_t> &build_col_ids) {
	if (finalized_) {
		throw InternalException("HashFilter::Build called after Finalize");
	}
	if (build_col_ids.empty()) {
		throw InternalException("HashFilter::Build: empty build_col_ids");
	}

	const idx_t row_count = chunk.size();
	vector<Value> key;
	key.reserve(build_col_ids.size());

	for (idx_t r = 0; r < row_count; r++) {
		key.clear();
		bool any_null = false;
		for (auto col_id : build_col_ids) {
			auto v = chunk.GetValue(col_id, r);
			if (v.IsNull()) {
				any_null = true;
				break;
			}
			key.emplace_back(std::move(v));
		}
		// Semi-join semantics: NULL keys never match. Skip them at build time.
		if (any_null) {
			continue;
		}
		auto h = HashKey(key);
		// Copy `key` per insert; vector<Value> still owns its memory after the
		// insert below (move only happens on actual insertion, not duplicate hit).
		InsertHashedKey(key, h);
	}
}

void HashFilter::Merge(HashFilter &other) {
	if (finalized_) {
		throw InternalException("HashFilter::Merge called after Finalize");
	}
	// Move keys to avoid copying Value tuples. We rehash because `other`'s
	// slots may live in a different-sized table.
	for (auto &k : other.stored_keys_) {
		auto h = HashKey(k);
		InsertHashedKey(std::move(k), h);
	}
	other.stored_keys_.clear();
	other.slots_.assign(INITIAL_CAPACITY, Slot {0, EMPTY_KEY_INDEX});
	other.mask_ = INITIAL_CAPACITY - 1;
	other.count_ = 0;
}

void HashFilter::Finalize() {
	finalized_ = true;
}

bool HashFilter::Contains(const vector<Value> &key) const {
	auto hash = HashKey(key);
	idx_t idx = hash & mask_;
	// Linear probe until we hit empty (definitive miss) or matching slot.
	while (true) {
		const auto &slot = slots_[idx];
		if (slot.key_index == EMPTY_KEY_INDEX) {
			return false;
		}
		if (slot.hash == hash && KeysEqual(stored_keys_[slot.key_index], key)) {
			return true;
		}
		idx = (idx + 1) & mask_;
	}
}

void HashFilterUseKernel::filter(const DataChunk &input, const vector<idx_t> &probe_col_ids,
                                 const HashFilter &hash_filter, SelectionVector &sel, idx_t &approved) {
	approved = 0;
	if (probe_col_ids.empty()) {
		throw InternalException("HashFilterUseKernel::filter: empty probe_col_ids");
	}
	if (hash_filter.IsEmpty()) {
		return;
	}

	const idx_t row_count = input.size();
	vector<Value> key;
	key.reserve(probe_col_ids.size());

	for (idx_t r = 0; r < row_count; r++) {
		key.clear();
		bool any_null = false;
		for (auto col_id : probe_col_ids) {
			auto v = input.GetValue(col_id, r);
			if (v.IsNull()) {
				any_null = true;
				break;
			}
			key.emplace_back(std::move(v));
		}
		// NULL probe keys never match (semi-join semantics).
		if (any_null) {
			continue;
		}
		if (hash_filter.Contains(key)) {
			sel.set_index(approved++, r);
		}
	}
}

} // namespace duckdb
