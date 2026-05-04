//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/operator_binding_printer.hpp
//
// Temporary debug utility to print operator tree with table_index and
// column binding information in both tree and JSON formats.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

class ClientContext;

//! Visitor to collect operator info and output in tree + JSON formats
class OperatorBindingPrinter : public LogicalOperatorVisitor {
public:
	OperatorBindingPrinter() = default;

	//! Main entry point: prints operator tree with bindings info
	void Print(LogicalOperator* plan, ClientContext& context);

private:
	//! Override visitor to collect operator information
	void VisitOperator(LogicalOperator &op) override;

	//! Collect table_index and column bindings for a single operator
	struct OperatorInfo {
		string operator_type;
		idx_t table_index;
		vector<pair<idx_t, idx_t>> column_bindings; // (table_idx, col_idx)
		vector<string> column_names;
		idx_t operator_id;
	};

	//! Recursively collect all operator info
	void CollectOperatorInfo(LogicalOperator* op, idx_t& op_counter,
	                          vector<OperatorInfo>& op_infos,
	                          case_insensitive_map_t<string>& table_index_map);

	//! Build table_index to table_name mapping from LogicalGet operators
	void BuildTableIndexMap(LogicalOperator* op,
	                        case_insensitive_map_t<string>& table_index_map);

	//! Print detailed operator information
	void PrintOperatorDetails(LogicalOperator* op,
	                          const case_insensitive_map_t<string>& table_map,
	                          const string& prefix);

	//! Write JSON format output
	void PrintJSONFormat(LogicalOperator* plan, ClientContext& context,
	                     const case_insensitive_map_t<string>& table_map);

	//! Recursively print operators in JSON format
	void PrintOperatorJSON(LogicalOperator* op, idx_t& op_id,
	                       const case_insensitive_map_t<string>& table_map,
	                       bool is_first);
};

} // namespace duckdb
