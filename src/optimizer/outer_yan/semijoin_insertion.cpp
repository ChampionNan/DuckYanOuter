#include "duckdb/optimizer/outer_yan/semijoin_insertion.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_sj_build.hpp"
#include "duckdb/planner/operator/logical_sj_probe.hpp"

#include <algorithm>

namespace duckdb {

bool SemijoinInsertion::BuildKey::operator==(const BuildKey &o) const {
	if (sorted_bindings.size() != o.sorted_bindings.size()) {
		return false;
	}
	for (idx_t i = 0; i < sorted_bindings.size(); i++) {
		if (!(sorted_bindings[i] == o.sorted_bindings[i])) {
			return false;
		}
	}
	return true;
}

// FNV-1a 64-bit. Constants from the official reference:
//   - http://www.isthe.com/chongo/tech/comp/fnv/
//   - https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
// offset basis = 14695981039346656037   (0xcbf29ce484222325)
// FNV prime    =      1099511628211     (0x00000100000001b3)
// Plain integer mix; not cryptographic. Suitable for hashtable bucket
// selection over small (≤ a few dozen) ColumnBinding sets.
size_t SemijoinInsertion::BuildKeyHash::operator()(const BuildKey &k) const {
	constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
	constexpr uint64_t kFnvPrime = 1099511628211ULL;
	uint64_t h = kFnvOffsetBasis;
	for (auto &b : k.sorted_bindings) {
		h ^= static_cast<uint64_t>(b.table_index.index);
		h *= kFnvPrime;
		// ProjectionIndex::index is private; use GetIndexUnsafe() (the
		// public accessor that skips validity checks).
		h ^= static_cast<uint64_t>(b.column_index.GetIndexUnsafe());
		h *= kFnvPrime;
	}
	return static_cast<size_t>(h);
}

SemijoinInsertion::BuildKey SemijoinInsertion::MakeBuildKey(const OuterYanSemiPair &pair) {
	BuildKey k;
	k.sorted_bindings.reserve(pair.keys.size());
	for (auto &key : pair.keys) {
		k.sorted_bindings.emplace_back(key.build_binding);
	}
	std::sort(k.sorted_bindings.begin(), k.sorted_bindings.end(),
	          [](const ColumnBinding &a, const ColumnBinding &b) {
		          if (a.table_index.index != b.table_index.index) {
			          return a.table_index.index < b.table_index.index;
		          }
		          // ProjectionIndex has operator< on its internal index.
		          return a.column_index < b.column_index;
	          });
	return k;
}

void SemijoinInsertion::SpliceAt(unique_ptr<LogicalOperator> &node, RelationId rid,
                                 unordered_map<RelationId, vector<ProbeWrap>> &probe_wraps,
                                 unordered_map<RelationId, unique_ptr<LogicalSJBuild>> &pending_builds,
                                 const unordered_map<RelationId, LogicalSJBuild *> &build_ptr_map) {
	// Whatever `node` currently is (bare LogicalGet or LogicalFilter+LogicalGet
	// unit) becomes the innermost — probes wrap it, then a build wraps those.
	unique_ptr<LogicalOperator> current = std::move(node);

	// 1) Probe wraps. `probe_wraps[rid]` was populated bottom_up first and
	//    top_down second in Apply; the first vector entry ends up innermost
	//    (closest to the base), the last outermost. Data flowing up from the
	//    base passes through the earliest-decided reduction first.
	auto probe_it = probe_wraps.find(rid);
	if (probe_it != probe_wraps.end()) {
		for (auto &pw : probe_it->second) {
			vector<shared_ptr<SemiJoinFilter>> fv;
			fv.emplace_back(pw.filter);
			auto probe = make_uniq<LogicalSJProbe>(std::move(fv));
			// Record this relation's probe bindings on the shared filter.
			// Each filter is created fresh per BuildKey group in Apply, so
			// the same (filter, probe_relation) is contributed at most once.
			for (auto &b : pw.probe_bindings) {
				pw.filter->AddProbeBinding(b);
			}
			auto related_it = build_ptr_map.find(pw.related_build_rid);
			if (related_it == build_ptr_map.end()) {
				throw InternalException(
				    "SemijoinInsertion: probe references unknown build relation %llu",
				    static_cast<unsigned long long>(pw.related_build_rid));
			}
			probe->AddDownStreamOperator(related_it->second);
			probe->children.emplace_back(std::move(current));
			current = std::move(probe);
		}
	}

	// 2) Build wrap (outermost over probes). Pre-created in Apply so the
	//    raw pointer in build_ptr_map stays valid for cross-relation
	//    probe wiring.
	auto build_pending_it = pending_builds.find(rid);
	if (build_pending_it != pending_builds.end() && build_pending_it->second) {
		auto build = std::move(build_pending_it->second);
		build->children.emplace_back(std::move(current));
		current = std::move(build);
	}

	node = std::move(current);
}

void SemijoinInsertion::Splice(unique_ptr<LogicalOperator> &node,
                               const unordered_map<idx_t, RelationId> &table_to_relation,
                               unordered_map<RelationId, vector<ProbeWrap>> &probe_wraps,
                               unordered_map<RelationId, unique_ptr<LogicalSJBuild>> &pending_builds,
                               const unordered_map<RelationId, LogicalSJBuild *> &build_ptr_map) {
	if (!node) {
		return;
	}

	// Splice case A: LogicalFilter whose single child is a LogicalGet. Treat
	// (filter, get) as the base unit and wrap above the filter. Materialised
	// build sees post-filter data → smaller SemiJoinFilter, less work for
	// downstream SJProbes.
	if (node->type == LogicalOperatorType::LOGICAL_FILTER && node->children.size() == 1 && node->children[0] &&
	    node->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node->children[0]->Cast<LogicalGet>();
		auto it = table_to_relation.find(get.table_index.index);
		if (it != table_to_relation.end()) {
			SpliceAt(node, it->second, probe_wraps, pending_builds, build_ptr_map);
		}
		return;
	}

	// Splice case B: bare LogicalGet (no filter directly above). Wrap above
	// the Get.
	if (node->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node->Cast<LogicalGet>();
		auto it = table_to_relation.find(get.table_index.index);
		if (it != table_to_relation.end()) {
			SpliceAt(node, it->second, probe_wraps, pending_builds, build_ptr_map);
		}
		return;
	}

	// Anything else (joins, projections, aggregates, ...): recurse. This
	// pushes the SJ wraps DOWN past pushed-down aggregates / projections
	// sitting between the join and the Get.
	for (auto &child : node->children) {
		Splice(child, table_to_relation, probe_wraps, pending_builds, build_ptr_map);
	}
}

unique_ptr<LogicalOperator> SemijoinInsertion::Apply(unique_ptr<LogicalOperator> plan,
                                                     const OuterYanTree &tree) {
	if (!plan) {
		return plan;
	}

	const auto &bu = tree.bottom_up_pairs;
	const auto &td = tree.top_down_pairs;
	if (bu.empty() && td.empty()) {
		return plan;
	}

	// -------------------------------------------------------------------------
	// 1) Group pairs by BuildKey → one shared SemiJoinFilter per group.
	//    bottom_up_pairs are processed FIRST so they end up innermost in the
	//    wrap chain (closest to the LogicalGet); top_down_pairs wrap outside.
	// -------------------------------------------------------------------------
	unordered_map<BuildKey, shared_ptr<SemiJoinFilter>, BuildKeyHash> dedup;
	unordered_map<RelationId, vector<shared_ptr<SemiJoinFilter>>> build_filters_by_rid;
	unordered_map<RelationId, vector<ProbeWrap>> probe_wraps;

	auto process_pair = [&](const OuterYanSemiPair &pair) {
		auto key = MakeBuildKey(pair);
		auto it = dedup.find(key);
		shared_ptr<SemiJoinFilter> filter;
		if (it == dedup.end()) {
			filter = make_shared_ptr<SemiJoinFilter>();
			for (auto &b : key.sorted_bindings) {
				filter->AddBuildBinding(b);
			}
			dedup.emplace(key, filter);
			build_filters_by_rid[pair.build].emplace_back(filter);
		} else {
			filter = it->second;
		}

		ProbeWrap pw;
		pw.filter = filter;
		pw.related_build_rid = pair.build;
		pw.probe_bindings.reserve(pair.keys.size());
		for (auto &k : pair.keys) {
			pw.probe_bindings.emplace_back(k.probe_binding);
		}
		probe_wraps[pair.probe].emplace_back(std::move(pw));
	};

	for (auto &p : bu) {
		process_pair(p);
	}
	for (auto &p : td) {
		process_pair(p);
	}

	// -------------------------------------------------------------------------
	// 2) Pre-create one LogicalSJBuild per build relation, holding ALL filters
	//    for that relation. Keep raw pointers in build_ptr_map for probes.
	// -------------------------------------------------------------------------
	unordered_map<RelationId, unique_ptr<LogicalSJBuild>> pending_builds;
	unordered_map<RelationId, LogicalSJBuild *> build_ptr_map;
	for (auto &entry : build_filters_by_rid) {
		auto rid = entry.first;
		auto build = make_uniq<LogicalSJBuild>(std::move(entry.second));
		build_ptr_map[rid] = build.get();
		pending_builds.emplace(rid, std::move(build));
	}

	// -------------------------------------------------------------------------
	// 3) Walk the rebuilt plan and splice wraps at the filter+get unit (or
	//    bare get) for every relation that has wraps assigned.
	// -------------------------------------------------------------------------
	Splice(plan, tree.table_to_relation, probe_wraps, pending_builds, build_ptr_map);

	// -------------------------------------------------------------------------
	// 4) Every pre-created build must have been spliced into the plan. If a
	//    pending build is still around, the rebuilt plan is missing the
	//    LogicalGet for that relation — a structural inconsistency.
	// -------------------------------------------------------------------------
	for (auto &kv : pending_builds) {
		if (kv.second) {
			throw InternalException(
			    "SemijoinInsertion: build for relation %llu was not spliced "
			    "(LogicalGet for table_index not found in rebuilt plan)",
			    static_cast<unsigned long long>(kv.first));
		}
	}

	return plan;
}

} // namespace duckdb
