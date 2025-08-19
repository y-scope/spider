#ifndef SPIDER_TDL_PARSER_AST_INTSPEC_HPP
#define SPIDER_TDL_PARSER_AST_INTSPEC_HPP

#include <cstdint>

namespace spider::tdl::parser::ast {
// Integer type specifications used in the AST.
enum class IntSpec : uint8_t {
    Int8,
    Int16,
    Int32,
    Int64,
};
}  // namespace spider::tdl::parser::ast

#endif  // SPIDER_TDL_PARSER_AST_INTSPEC_HPP
