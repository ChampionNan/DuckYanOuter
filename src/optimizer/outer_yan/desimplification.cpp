#include "duckdb/optimizer/outer_yan/desimplification.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void Desimplification::Apply(OperatorTree &ot) {
	throw NotImplementedException("Desimplification::Apply");
}

bool Desimplification::ApplyRule1(OTNode &node) {
	throw NotImplementedException("Desimplification::ApplyRule1");
}

bool Desimplification::ApplyRule2(OTNode &node) {
	throw NotImplementedException("Desimplification::ApplyRule2");
}

bool Desimplification::ApplyRule3(OTNode &node) {
	throw NotImplementedException("Desimplification::ApplyRule3");
}

bool Desimplification::ApplyRule4(OTNode &node) {
	throw NotImplementedException("Desimplification::ApplyRule4");
}

bool Desimplification::ApplyRule5(OTNode &node) {
	throw NotImplementedException("Desimplification::ApplyRule5");
}

bool Desimplification::ApplyRule6(OTNode &node) {
	throw NotImplementedException("Desimplification::ApplyRule6");
}

} // namespace duckdb
