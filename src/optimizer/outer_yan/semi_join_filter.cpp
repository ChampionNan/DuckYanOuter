#include "duckdb/optimizer/outer_yan/semi_join_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"
#include "duckdb/optimizer/outer_yan/key_bitmap.hpp"

namespace duckdb {

SemiJoinFilter::SemiJoinFilter() : kind_(Kind::UNDECIDED) {
}

SemiJoinFilter::~SemiJoinFilter() = default;

void SemiJoinFilter::FinalizeAsBitmap(unique_ptr<KeyBitmap> b) {
	if (kind_ != Kind::UNDECIDED) {
		throw InternalException("SemiJoinFilter::FinalizeAsBitmap: already finalized");
	}
	bitmap_ = std::move(b);
	kind_ = Kind::BITMAP;
}

void SemiJoinFilter::FinalizeAsHash(shared_ptr<HashFilter> h) {
	if (kind_ != Kind::UNDECIDED) {
		throw InternalException("SemiJoinFilter::FinalizeAsHash: already finalized");
	}
	hash_ = std::move(h);
	kind_ = Kind::HASH;
}

const KeyBitmap &SemiJoinFilter::bitmap() const {
	D_ASSERT(kind_ == Kind::BITMAP);
	return *bitmap_;
}

HashFilter &SemiJoinFilter::hash() const {
	D_ASSERT(kind_ == Kind::HASH);
	return *hash_;
}

shared_ptr<HashFilter> SemiJoinFilter::hash_shared() const {
	D_ASSERT(kind_ == Kind::HASH);
	return hash_;
}

bool SemiJoinFilter::IsEmpty() const {
	switch (kind_) {
	case Kind::UNDECIDED:
		// Callers must not probe before Finalize; treat as "no matches" defensively.
		return true;
	case Kind::BITMAP:
		return bitmap_->IsEmpty();
	case Kind::HASH:
		return hash_->IsEmpty();
	}
	throw InternalException("SemiJoinFilter::IsEmpty: unreachable");
}

} // namespace duckdb
