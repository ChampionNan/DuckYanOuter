//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/hash_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class BufferManager;

// =============================================================================
// HashTable — open-addressed backing store
// =============================================================================
//
// Mirrors the API surface of RPT's `HashTable` (`Build`, `Merge`, `Probe`,
// `ScanStructure`) but is a fresh implementation:
//
//   - MurmurHash for key hashing
//   - Linear probing open addressing for cache-friendly lookups
//   - SIMD-batched probing in the hot loop (see HashFilterUseKernel below);
//     scalar fallback per CLAUDE.md §2.3
//
// No false positives — semi-join reduction here must be exact.
class HashTable {
public:
	struct ScanStructure {
		Vector pointers;
		idx_t count = 0;
		SelectionVector sel_vector;
		HashTable &ht;
		bool finished = false;

		explicit ScanStructure(HashTable &ht);
		void Next(DataChunk &keys, SelectionVector &match_sel, idx_t &result_count);
	};

public:
	HashTable(BufferManager &buffer_manager, vector<LogicalType> condition_types);
	~HashTable();

	void Build(DataChunk &keys);
	void Merge(HashTable &other);
	void Finalize();

	unique_ptr<ScanStructure> Probe(DataChunk &keys);

	idx_t Count() const;

private:
	BufferManager &buffer_manager;
	vector<LogicalType> condition_types;
};

// =============================================================================
// HashFilter — shared runtime carrier for an SJBuild / SJProbe pair
// =============================================================================
//
// Both sides of the pair — `LogicalSJBuild::sj_to_create` and
// `LogicalSJProbe::sj_to_use` — hold `vector<shared_ptr<HashFilter>>`
// pointing to the same instances. The shared object IS the binding; no
// separate registry / handle id is needed.
//
// Hash filter (not bloom filter) — semi-join reduction here must be exact,
// so we use a hash-based set with no false-positive rate.
//
// Column-binding metadata for the build and probe sides is stored on the
// filter itself, matching RPT's pattern.
class HashFilter {
public:
	HashFilter();

	void AddColumnBindingBuilt(ColumnBinding binding);
	void AddColumnBindingApplied(ColumnBinding binding);

	const vector<ColumnBinding> &GetColumnBindingsBuilt() const {
		return column_bindings_built;
	}
	const vector<ColumnBinding> &GetColumnBindingsApplied() const {
		return column_bindings_applied;
	}

	bool IsEmpty() const;
	bool IsUsed() const {
		return used.load();
	}
	void SetUsed() {
		used.store(true);
	}

	//! The underlying open-addressed table (MurmurHash + linear probing).
	shared_ptr<HashTable> hash_table;

private:
	vector<ColumnBinding> column_bindings_built;
	vector<ColumnBinding> column_bindings_applied;
	mutable atomic<bool> used;
};

// =============================================================================
// HashFilterUseKernel — SIMD-batched probe kernel
// =============================================================================
//
// Mirrors RPT's `HashFilterUseKernel::filter`. Given a probe-side input, a
// filter, and an output selection vector, narrows the selection to rows
// whose keys hit the filter.
//
// SIMD-batched in the hot loop (target SSE4.2 baseline + AVX2 opportunistic
// on x86-64; NEON on ARM64). Scalar fallback retained per CLAUDE.md §2.3.
// The caller is responsible for checking that the filter is non-empty and
// valid before invoking.
class HashFilterUseKernel {
public:
	static void filter(vector<Vector> &input, shared_ptr<HashFilter> hash_filter, SelectionVector &sel,
	                   idx_t &approved_tuple_count, idx_t row_num);
};

} // namespace duckdb
