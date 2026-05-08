#include "duckdb/optimizer/outer_yan/outer_yan_dp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/outer_yan/outer_yan_tree.hpp"

namespace duckdb {

// ----------------------------------------------------------------------------
// DPMemo
// ----------------------------------------------------------------------------

DPMemo::DPMemo() = default;

bool DPMemo::Has(idx_t relation_id) const {
	return memo.find(relation_id) != memo.end();
}

const DPMemoEntry &DPMemo::Get(idx_t relation_id) const {
	return memo.at(relation_id);
}

void DPMemo::Put(idx_t relation_id, DPMemoEntry entry) {
	memo[relation_id] = std::move(entry);
}

void DPMemo::Clear() {
	memo.clear();
}

unique_ptr<OJTNode> DPMemo::ExtractBest() {
	throw NotImplementedException("DPMemo::ExtractBest");
}

// ----------------------------------------------------------------------------
// OuterYanDP
// ----------------------------------------------------------------------------

OuterYanDP::OuterYanDP(ClientContext &context_p) : context(context_p) {
}

void OuterYanDP::Optimize(OuterYanTree &tree) {
	// Pipeline:
	//   tree.ot -> OTToOJT -> EnumerateRoot (fills memo) -> rebuilt tree.ojt
	throw NotImplementedException("OuterYanDP::Optimize");
}

void OuterYanDP::EnumerateRoot(OrderedJoinTree &ojt) {
	throw NotImplementedException("OuterYanDP::EnumerateRoot");
}

void OuterYanDP::EnumerateNode(OJTNode &node) {
	throw NotImplementedException("OuterYanDP::EnumerateNode");
}

} // namespace duckdb
