//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_yan/semi_join_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class KeyBitmap;
class HashFilter;

// =============================================================================
// SemiJoinFilter — tagged-union carrier between SJBuild and SJProbe.
// =============================================================================
//
// Plan 3 contract. Both `LogicalSJBuild::sj_to_create` and
// `LogicalSJProbe::sj_to_use` hold `vector<shared_ptr<SemiJoinFilter>>`
// pointing at the same instances. The shared_ptr IS the binding (RPT
// pattern); no registry / handle id.
//
// `kind` is set ONCE by `PhysicalSJBuild::Finalize` (either bitmap if the
// build keys' range fits the PRAGMA outer_yan_bitmap_max_bytes budget, or
// hash filter as fallback). The probe path branches once on `kind`
// outside the per-tuple loop — no per-row branch.
//
// Cross-thread visibility of the kind / payload writes is handled by the
// DuckDB pipeline scheduler: the probe pipeline depends on the build
// pipeline's Finalize (via PhysicalSJBuild::BuildPipelinesFromRelated ->
// AddDependency), and the event/task graph establishes a happens-before
// edge between Finalize completing and the first probe call. So no
// atomic / memory-order plumbing is needed on this struct.
class SemiJoinFilter {
public:
	enum class Kind : uint8_t { UNDECIDED, BITMAP, HASH };

	SemiJoinFilter();
	~SemiJoinFilter();

	// Bindings — populated by SemijoinInsertion::Apply when the
	// LogicalSJBuild + LogicalSJProbe pair is emitted.
	void AddBuildBinding(ColumnBinding b) {
		build_bindings_.emplace_back(b);
	}
	void AddProbeBinding(ColumnBinding b) {
		probe_bindings_.emplace_back(b);
	}
	const vector<ColumnBinding> &BuildBindings() const {
		return build_bindings_;
	}
	const vector<ColumnBinding> &ProbeBindings() const {
		return probe_bindings_;
	}

	// Lifecycle. Set once at Finalize.
	Kind kind() const {
		return kind_;
	}
	bool finalized() const {
		return kind_ != Kind::UNDECIDED;
	}

	void FinalizeAsBitmap(unique_ptr<KeyBitmap> b);
	void FinalizeAsHash(shared_ptr<HashFilter> h);

	const KeyBitmap &bitmap() const;
	HashFilter &hash() const;
	// Shared-ptr accessor for HashFilterUseKernel::filter, which takes a
	// shared_ptr<HashFilter>. Only valid when kind() == HASH.
	shared_ptr<HashFilter> hash_shared() const;

	// True iff the build set is empty (no probe row survives).
	bool IsEmpty() const;

private:
	Kind kind_;
	unique_ptr<KeyBitmap> bitmap_;
	shared_ptr<HashFilter> hash_;
	vector<ColumnBinding> build_bindings_;
	vector<ColumnBinding> probe_bindings_;
};

} // namespace duckdb
