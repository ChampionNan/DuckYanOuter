#include "duckdb/optimizer/outer_yan/applicability.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

namespace {

void CollectColumnTables(const Expression &expr, unordered_set<idx_t> &tables) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		tables.insert(expr.Cast<BoundColumnRefExpression>().binding.table_index);
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { CollectColumnTables(child, tables); });
}

} // namespace

OuterYanApplicability::OuterYanApplicability() {
}

ApplicabilityResult OuterYanApplicability::Check(LogicalOperator &plan) {
	OuterYanApplicability v;
	auto &entry = v.SkeletonRoot(plan);
	v.VisitOperator(entry);

	if (v.rejected) {
		v.result.applicable = false;
		return std::move(v.result);
	}
	if (v.result.relations.size() < 2) {
		v.result.applicable = false;
		v.result.reason = "fewer than two base relations in join skeleton";
		return std::move(v.result);
	}
	if (!v.has_outer) {
		v.result.applicable = false;
		v.result.reason = "pure inner-join query — defer to JoinOrderOptimizer";
		return std::move(v.result);
	}
	v.result.applicable = true;
	return std::move(v.result);
}

LogicalOperator &OuterYanApplicability::SkeletonRoot(LogicalOperator &plan) {
	LogicalOperator *cur = &plan;
	auto skip_pf = [&]() {
		while (cur->type == LogicalOperatorType::LOGICAL_PROJECTION ||
		       cur->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (cur->children.empty()) {
				return;
			}
			cur = cur->children[0].get();
		}
	};
	skip_pf();
	if (cur->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		result.has_root_aggregate = true;
		if (!cur->children.empty()) {
			cur = cur->children[0].get();
		}
		skip_pf();
	}
	return *cur;
}

void OuterYanApplicability::VisitOperator(LogicalOperator &op) {
	if (rejected) {
		return;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		VisitComparisonJoin(op.Cast<LogicalComparisonJoin>());
		return;

	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		VisitOperatorChildren(op);
		return;

	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		Reject("aggregation inside join skeleton not supported");
		return;
	case LogicalOperatorType::LOGICAL_WINDOW:
		Reject("window operator inside join skeleton not supported");
		return;
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		Reject("set operation inside join skeleton not supported");
		return;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
		Reject("unsupported join operator (only comparison joins allowed)");
		return;

	default:
		// Opaque leaf: LogicalGet, LogicalExpressionGet, LogicalCTERef,
		// LogicalDummyScan, sub-plans, etc.
		RegisterLeaf(op);
		return;
	}
}

void OuterYanApplicability::VisitComparisonJoin(LogicalComparisonJoin &join) {
	switch (join.join_type) {
	case JoinType::INNER:
		break;
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		has_outer = true;
		break;
	default:
		Reject("unsupported join type (semi/anti/mark/single not allowed)");
		return;
	}

	if (join.conditions.empty()) {
		Reject("cross product (join with no conditions) not supported");
		return;
	}

	VisitOperator(*join.children[0]);
	if (rejected) {
		return;
	}
	VisitOperator(*join.children[1]);
	if (rejected) {
		return;
	}

	for (idx_t cond_idx = 0; cond_idx < join.conditions.size(); cond_idx++) {
		const auto &cond = join.conditions[cond_idx];
		if (!cond.IsComparison()) {
			Reject("non-comparison join condition not supported");
			return;
		}
		auto cmp = cond.GetComparisonType();
		if (cmp == ExpressionType::COMPARE_DISTINCT_FROM ||
		    cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			Reject("NULL-tolerant join condition (IS [NOT] DISTINCT FROM) not supported");
			return;
		}

		unordered_set<idx_t> tables;
		CollectColumnTables(cond.GetLHS(), tables);
		CollectColumnTables(cond.GetRHS(), tables);

		unordered_set<RelationId> rels;
		for (auto ti : tables) {
			auto it = result.table_index_to_relation.find(ti);
			if (it == result.table_index_to_relation.end()) {
				Reject("join condition references a column outside the join skeleton");
				return;
			}
			rels.insert(it->second);
		}
		if (rels.size() != 2) {
			Reject("join condition must reference exactly two base relations");
			return;
		}

		auto rit = rels.begin();
		RelationId a = *rit++;
		RelationId b = *rit;
		RelationId lo = a < b ? a : b;
		RelationId hi = a < b ? b : a;
		uint64_t key = (uint64_t(lo) << 32) | uint64_t(hi);
		bool first_for_pair = seen_pairs.insert(key).second;
		if (first_for_pair) {
			if (!UnionRelations(a, b)) {
				Reject("join graph has a cycle");
				return;
			}
			result.edges.push_back({a, b, &join, cond_idx});
		}
		// Multi-edges between the same pair are tolerated: same node pair,
		// no cycle introduced.
	}
}

void OuterYanApplicability::RegisterLeaf(LogicalOperator &leaf) {
	RelationId id = result.relations.size();
	result.relations.push_back(&leaf);
	uf_parent.push_back(id);
	for (auto &ti : leaf.GetTableIndex()) {
		auto inserted = result.table_index_to_relation.emplace(ti.index, id);
		if (!inserted.second) {
			Reject("duplicate table index across leaf relations");
			return;
		}
	}
}

void OuterYanApplicability::Reject(string reason) {
	if (!rejected) {
		rejected = true;
		result.reason = std::move(reason);
	}
}

RelationId OuterYanApplicability::Find(RelationId x) {
	while (uf_parent[x] != x) {
		uf_parent[x] = uf_parent[uf_parent[x]];
		x = uf_parent[x];
	}
	return x;
}

bool OuterYanApplicability::UnionRelations(RelationId a, RelationId b) {
	auto ra = Find(a);
	auto rb = Find(b);
	if (ra == rb) {
		return false;
	}
	uf_parent[ra] = rb;
	return true;
}

} // namespace duckdb
