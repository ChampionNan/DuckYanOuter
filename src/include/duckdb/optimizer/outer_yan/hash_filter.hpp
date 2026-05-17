//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/hash_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// =============================================================================
// HashFilter — exact set-membership fallback when KeyBitmap doesn't apply
// =============================================================================
//
// Used as the HASH branch of SemiJoinFilter when:
//   - The build key type is not bitmap-eligible (varchar, decimal, etc.)
//   - The key range exceeds PRAGMA outer_yan_bitmap_max_bytes * 8
//   - Composite build keys (v1: bitmap only supports single-column)
//
// Design — open-addressed hash set with linear probing, MurmurHash64A on
// the concatenation of per-column Value::Hash() outputs. Inspired by Apache
// Arrow Acero's `SwissTable`/`KeyEncoder` and DuckDB's `JoinHashTable` but
// pared down to exact set-membership (no rowstore, no projection, no probe
// scan structure). Slot layout:
//
//     Slot { hash_t full_hash; idx_t key_index_or_EMPTY; }
//
// `slots_` is a power-of-2 array (probed via `hash & mask_`). Stored keys
// live in `stored_keys_` (one vector<Value> per unique build tuple).
// Collisions follow linear probing — `(idx + 1) & mask_` — chosen for
// cache friendliness over double hashing.
//
// Concurrency model: each thread builds a thread-local HashFilter, all of
// which are folded into the global one at Combine/Finalize via Merge. So
// neither Build nor Contains needs internal synchronisation. Merge is
// single-threaded by construction (called once at Finalize).
//
// Exact semantics: no false positives. NULL keys are skipped at Build time
// and at probe time (semi-join NULL never matches).
class HashFilter {
public:
	HashFilter();
	~HashFilter();

	// Extract the build-side join keys from `chunk` and insert them. NULL
	// keys are skipped. Idempotent: the table is a set (duplicate inserts
	// are no-ops).
	void Build(const DataChunk &chunk, const vector<idx_t> &build_col_ids);

	// Move all keys from `other` into this; `other` becomes empty. Used to
	// fold per-thread HashFilters into the global one. Single-threaded.
	void Merge(HashFilter &other);

	// Marks the filter probe-ready. Currently just a flag; reserved for
	// future repack / shrink-to-fit optimisations.
	void Finalize();

	// Per-row existence test. Single-key probe; called by HashFilterUseKernel.
	bool Contains(const vector<Value> &key) const;

	idx_t Count() const {
		return count_;
	}
	idx_t Capacity() const {
		return mask_ + 1;
	}
	bool IsEmpty() const {
		return count_ == 0;
	}
	bool IsFinalized() const {
		return finalized_;
	}

	// Reference-quality MurmurHash64A (Austin Appleby). Public so callers
	// can hash custom byte sequences with the same family used internally.
	static hash_t MurmurHash64A(const void *data, idx_t len, uint64_t seed = 0);
	// Combine per-column Value::Hash() outputs into one MurmurHash64A digest.
	static hash_t HashKey(const vector<Value> &key);

private:
	struct Slot {
		hash_t hash;
		idx_t key_index; // EMPTY_KEY_INDEX when slot is free
	};

	static constexpr idx_t EMPTY_KEY_INDEX = NumericLimits<idx_t>::Maximum();
	// Power-of-2 starting capacity. Small so that filters over tiny build
	// sides don't waste memory on slot allocation.
	static constexpr idx_t INITIAL_CAPACITY = 64;

	// Load factor 3/4 — checked as (count+1)*4 > capacity*3 to avoid FP.
	void EnsureCapacityFor(idx_t additional);
	void Resize(idx_t new_capacity);
	// Insert (or no-op on duplicate). Caller pre-computes `hash` to avoid
	// re-hashing during Merge.
	void InsertHashedKey(vector<Value> key, hash_t hash);
	static bool KeysEqual(const vector<Value> &a, const vector<Value> &b);

	vector<Slot> slots_;
	idx_t mask_ = 0; // capacity - 1
	vector<vector<Value>> stored_keys_;
	idx_t count_ = 0;
	bool finalized_ = false;
};

// =============================================================================
// HashFilterUseKernel — per-chunk probe kernel
// =============================================================================
//
// Per-row Contains check, walking `probe_col_ids` to extract each row's key
// tuple. SIMD acceleration is plausible (batched MurmurHash + gather) but
// deferred — KeyBitmap is the primary path in plan 3 and the HASH branch
// is rarely hot.
class HashFilterUseKernel {
public:
	//! Narrows `sel` in-place to surviving rows from `input`. `probe_col_ids`
	//! identify the join-key columns in `input`. NULL probe keys never match.
	static void filter(const DataChunk &input, const vector<idx_t> &probe_col_ids,
	                   const HashFilter &hash_filter, SelectionVector &sel, idx_t &approved);
};

} // namespace duckdb
