#ifndef SPIDER_TDL_PARSER_AST_FLOATSPEC_HPP
#define SPIDER_TDL_PARSER_AST_FLOATSPEC_HPP

#include <cstdint>

namespace spider::tdl::parser::ast {
// Float type specifications used in the AST.
enum class FloatSpec : uint8_t {
    Float,
    Double,
};
}  // namespace spider::tdl::parser::ast

#endif  // SPIDER_TDL_PARSER_AST_FLOATSPEC_HPP
