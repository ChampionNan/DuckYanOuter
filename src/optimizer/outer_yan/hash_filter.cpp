#include "duckdb/optimizer/outer_yan/hash_filter.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

// =============================================================================
// HashTable
// =============================================================================
//
// TODO: open-addressed table backing — MurmurHash + linear probing. Backing
// storage is allocated through BufferManager so very large filter tables can
// spill (per CLAUDE.md §2.2 "Unbounded state ... should route through
// BufferManager"). Plain heap acceptable for prototype if stated.

HashTable::HashTable(BufferManager &buffer_manager_p, vector<LogicalType> condition_types_p)
    : buffer_manager(buffer_manager_p), condition_types(std::move(condition_types_p)) {
}

HashTable::~HashTable() = default;

void HashTable::Build(DataChunk &keys) {
	throw NotImplementedException("HashTable::Build");
}

void HashTable::Merge(HashTable &other) {
	throw NotImplementedException("HashTable::Merge");
}

void HashTable::Finalize() {
	throw NotImplementedException("HashTable::Finalize");
}

unique_ptr<HashTable::ScanStructure> HashTable::Probe(DataChunk &keys) {
	throw NotImplementedException("HashTable::Probe");
}

idx_t HashTable::Count() const {
	throw NotImplementedException("HashTable::Count");
}

HashTable::ScanStructure::ScanStructure(HashTable &ht_p)
    : pointers(LogicalType::POINTER), sel_vector(STANDARD_VECTOR_SIZE), ht(ht_p) {
}

void HashTable::ScanStructure::Next(DataChunk &keys, SelectionVector &match_sel, idx_t &result_count) {
	throw NotImplementedException("HashTable::ScanStructure::Next");
}

// =============================================================================
// HashFilter
// =============================================================================

HashFilter::HashFilter() : used(false) {
}

void HashFilter::AddColumnBindingBuilt(ColumnBinding binding) {
	column_bindings_built.emplace_back(binding);
}

void HashFilter::AddColumnBindingApplied(ColumnBinding binding) {
	column_bindings_applied.emplace_back(binding);
}

bool HashFilter::IsEmpty() const {
	return !hash_table || hash_table->Count() == 0;
}

// =============================================================================
// HashFilterUseKernel
// =============================================================================
//
// TODO: SIMD-batched probe of `hash_filter->hash_table` using `input`. Caller
// guarantees the filter is non-empty. The kernel narrows `sel` and updates
// `approved_tuple_count` to the surviving rows. Use SSE4.2 / AVX2 on x86-64,
// NEON on ARM64; scalar fallback required.

void HashFilterUseKernel::filter(vector<Vector> &input, shared_ptr<HashFilter> hash_filter, SelectionVector &sel,
                                 idx_t &approved_tuple_count, idx_t row_num) {
	throw NotImplementedException("HashFilterUseKernel::filter");
}

} // namespace duckdb
