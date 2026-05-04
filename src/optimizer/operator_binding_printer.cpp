#include "duckdb/optimizer/operator_binding_printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void OperatorBindingPrinter::Print(LogicalOperator* plan, ClientContext& context) {
	if (!plan) {
		return;
	}

	Printer::Print("\n========================================");
	Printer::Print("      OPERATOR BINDING INFORMATION");
	Printer::Print("========================================\n");

	// 1. Print standard DuckDB tree format
	Printer::Print("[TREE FORMAT]\n");
	plan->Print();

	// 2. Print column bindings
	Printer::Print("\n[COLUMN BINDINGS]\n");
	plan->PrintColumnBindings();

	// 3. Print detailed operator info
	Printer::Print("\n[DETAILED OPERATOR INFO]\n");
	case_insensitive_map_t<string> table_map;
	BuildTableIndexMap(plan, table_map);
	PrintOperatorDetails(plan, table_map, "");

	// 4. Print JSON format
	Printer::Print("\n[JSON FORMAT]\n");
	PrintJSONFormat(plan, context, table_map);

	Printer::Print("\n========================================\n");
}

void OperatorBindingPrinter::BuildTableIndexMap(LogicalOperator* op,
                                               case_insensitive_map_t<string>& table_map) {
	if (!op) {
		return;
	}

	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto& get_op = op->Cast<LogicalGet>();
		// Use input_table_names if available, otherwise use a generic name
		string table_name = get_op.input_table_names.empty() ? "table" : get_op.input_table_names[0];
		table_map[StringUtil::Format("%llu", get_op.table_index)] = table_name;
	}

	for (auto& child : op->children) {
		BuildTableIndexMap(child.get(), table_map);
	}
}

void OperatorBindingPrinter::PrintOperatorDetails(LogicalOperator* op,
                                                  const case_insensitive_map_t<string>& table_map,
                                                  const string& prefix) {
	if (!op) {
		return;
	}

	Printer::Print(prefix + "Op: " + op->ToString());

	// Get and print column bindings
	auto bindings = op->GetColumnBindings();
	if (!bindings.empty()) {
		Printer::Print(prefix + "  Bindings: ");
		string binding_str = "";
		for (idx_t i = 0; i < (idx_t)bindings.size(); i++) {
			binding_str += "(" + StringUtil::Format("%llu", bindings[i].table_index) + "," +
			               StringUtil::Format("%llu", bindings[i].column_index) + ")";
			if (i < (idx_t)bindings.size() - 1) {
				binding_str += " ";
			}
		}
		Printer::Print(prefix + "    " + binding_str);
	}

	// Operator-specific details
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto& get_op = op->Cast<LogicalGet>();
		string table_name = get_op.input_table_names.empty() ? "table" : get_op.input_table_names[0];
		Printer::Print(prefix + "  Table: " + table_name);
		Printer::Print(prefix + "  TableIndex: " + StringUtil::Format("%llu", get_op.table_index));
		Printer::Print(prefix + "  Columns: " + StringUtil::Join(get_op.names, ", "));
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto& proj_op = op->Cast<LogicalProjection>();
		Printer::Print(prefix + "  Expressions:");
		for (idx_t i = 0; i < (idx_t)proj_op.expressions.size(); i++) {
			Printer::Print(prefix + "    [" + StringUtil::Format("%llu", i) + "] " +
			               proj_op.expressions[i]->ToString());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto& agg_op = op->Cast<LogicalAggregate>();
		if (!agg_op.groups.empty()) {
			Printer::Print(prefix + "  GroupExpressions:");
			for (idx_t i = 0; i < (idx_t)agg_op.groups.size(); i++) {
				Printer::Print(prefix + "    [" + StringUtil::Format("%llu", i) + "] " +
				               agg_op.groups[i]->ToString());
			}
		}
		Printer::Print(prefix + "  AggregateExpressions:");
		for (idx_t i = 0; i < (idx_t)agg_op.expressions.size(); i++) {
			Printer::Print(prefix + "    [" + StringUtil::Format("%llu", i) + "] " +
			               agg_op.expressions[i]->ToString());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto& join_op = op->Cast<LogicalComparisonJoin>();
		Printer::Print(prefix + "  JoinType: " + StringUtil::Upper(JoinTypeToString(join_op.join_type)));
		Printer::Print(prefix + "  JoinConditions:");
		for (idx_t i = 0; i < (idx_t)join_op.conditions.size(); i++) {
			const auto& cond = join_op.conditions[i];
			Printer::Print(prefix + "    [" + StringUtil::Format("%llu", i) + "] " +
			               cond.GetLHS().ToString() + " " +
			               ExpressionTypeToString(cond.GetComparisonType()) + " " +
			               cond.GetRHS().ToString());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto& filter_op = op->Cast<LogicalFilter>();
		Printer::Print(prefix + "  Expressions:");
		for (idx_t i = 0; i < (idx_t)filter_op.expressions.size(); i++) {
			Printer::Print(prefix + "    [" + StringUtil::Format("%llu", i) + "] " +
			               filter_op.expressions[i]->ToString());
		}
		break;
	}
	default:
		break;
	}

	// Recurse to children
	if (!op->children.empty()) {
		Printer::Print(prefix + "  Children:");
		for (auto& child : op->children) {
			PrintOperatorDetails(child.get(), table_map, prefix + "    ");
		}
	}
}

void OperatorBindingPrinter::PrintJSONFormat(LogicalOperator* plan, ClientContext& context,
                                             const case_insensitive_map_t<string>& table_map) {
	idx_t op_id = 0;
	Printer::Print("{");
	Printer::Print("  \"operators\": [");
	PrintOperatorJSON(plan, op_id, table_map, true);
	Printer::Print("  ]");
	Printer::Print("}");
}

void OperatorBindingPrinter::PrintOperatorJSON(LogicalOperator* op, idx_t& op_id,
                                               const case_insensitive_map_t<string>& table_map,
                                               bool is_first) {
	if (!op) {
		return;
	}

	if (!is_first) {
		Printer::Print(",");
	}

	Printer::Print("    {");
	Printer::Print("      \"id\": " + StringUtil::Format("%llu", op_id++) + ",");
	Printer::Print("      \"type\": \"" + op->ToString() + "\",");

	// Column bindings
	auto bindings = op->GetColumnBindings();
	Printer::Print("      \"column_bindings\": [");
	for (idx_t i = 0; i < (idx_t)bindings.size(); i++) {
		Printer::Print("        {");
		Printer::Print("          \"table_index\": " + StringUtil::Format("%llu", bindings[i].table_index) + ",");
		Printer::Print("          \"column_index\": " + StringUtil::Format("%llu", bindings[i].column_index));
		Printer::Print(string("        }") + (i < (idx_t)bindings.size() - 1 ? "," : ""));
	}
	Printer::Print("      ],");

	// Operator-specific info
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto& get_op = op->Cast<LogicalGet>();
		string table_name = get_op.input_table_names.empty() ? "table" : get_op.input_table_names[0];
		Printer::Print("      \"table_index\": " + StringUtil::Format("%llu", get_op.table_index) + ",");
		Printer::Print("      \"table_name\": \"" + table_name + "\"");
	} else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto& join_op = op->Cast<LogicalComparisonJoin>();
		Printer::Print("      \"join_type\": \"" + StringUtil::Upper(JoinTypeToString(join_op.join_type)) +
		               "\"");
	} else {
		Printer::Print("      \"info\": \"" + op->ToString() + "\"");
	}

	Printer::Print("    }");

	// Recurse to children
	for (auto& child : op->children) {
		PrintOperatorJSON(child.get(), op_id, table_map, false);
	}
}

void OperatorBindingPrinter::VisitOperator(LogicalOperator &op) {
	// Default implementation: just recurse
	VisitOperatorChildren(op);
}

} // namespace duckdb
