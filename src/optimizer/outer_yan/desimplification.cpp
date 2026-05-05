#include "duckdb/optimizer/outer_yan/desimplification.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void Desimplification::Apply(LogicalOperator &plan) {
	throw NotImplementedException("Desimplification::Apply");
}

bool Desimplification::ApplyRule1(LogicalOperator &node) {
	throw NotImplementedException("Desimplification::ApplyRule1");
}

bool Desimplification::ApplyRule2(LogicalOperator &node) {
	throw NotImplementedException("Desimplification::ApplyRule2");
}

bool Desimplification::ApplyRule3(LogicalOperator &node) {
	throw NotImplementedException("Desimplification::ApplyRule3");
}

bool Desimplification::ApplyRule4(LogicalOperator &node) {
	throw NotImplementedException("Desimplification::ApplyRule4");
}

bool Desimplification::ApplyRule5(LogicalOperator &node) {
	throw NotImplementedException("Desimplification::ApplyRule5");
}

bool Desimplification::ApplyRule6(LogicalOperator &node) {
	throw NotImplementedException("Desimplification::ApplyRule6");
}

} // namespace duckdb
