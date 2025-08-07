#ifndef SPIDER_TDL_PARSER_AST_UTILS_HPP
#define SPIDER_TDL_PARSER_AST_UTILS_HPP

#include <cstddef>
#include <string>

namespace spider::tdl::parser::ast {
/**
 * Creates a string with the specified number of indentation levels.
 * Each level of indentation is represented by 2 spaces.
 * @param indentation_level The number of indentation levels to create.
 * @return A string containing the specified number of indentation levels.
 */
[[nodiscard]] auto create_indentation(size_t indentation_level) -> std::string;
}  // namespace spider::tdl::parser::ast

#endif  // SPIDER_TDL_PARSER_AST_UTILS_HPP
