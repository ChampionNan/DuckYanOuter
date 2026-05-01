//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_sj_probe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/optimizer/outer_yan/hash_filter.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_sj_build.hpp"

namespace duckdb {

//! LogicalSJProbe — OuterYan semi-join probe side.
//!
//! Pairing via shared HashFilter shared_ptrs (same instances held by the
//! upstream LogicalSJBuild). Additionally tracks back-pointers to the
//! upstream LogicalSJBuild operators (`related_sj_build`) — used by the
//! physical plan generator to wire pipeline dependencies, mirroring RPT's
//! `LogicalUseBF::related_create_bf`.
//!
//! Pass-through filter on the data path — schema unchanged from the input.
class LogicalSJProbe : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_SJ_PROBE;

public:
	explicit LogicalSJProbe(vector<shared_ptr<HashFilter>> sj_to_use);

	//! Filters this operator probes against. Same shared_ptr instances are
	//! populated by the paired LogicalSJBuild operators.
	vector<shared_ptr<HashFilter>> sj_to_use;

	//! Back-pointers to the upstream LogicalSJBuild operators populating the
	//! filters in `sj_to_use`. Mirrors RPT's `related_create_bf`.
	vector<LogicalSJBuild *> related_sj_build;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;
	vector<ColumnBinding> GetColumnBindings() override;

	//! Append an upstream LogicalSJBuild that populates one of `sj_to_use`'s filters.
	void AddDownStreamOperator(LogicalSJBuild *op);

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
