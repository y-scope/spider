#ifndef SPIDER_TDL_PARSER_PARSE_HPP
#define SPIDER_TDL_PARSER_PARSE_HPP

#include <istream>

namespace spider::tdl::parser {
/**
 * Parses a TDL file as a translation unit from an input stream.
 * @param input The input stream containing the TDL content. The entire content will be considered
 * as a single translation unit.
 * @return Whether the parser returned on success.
 * NOTE: We will update this function to return an actual AST root once we add Antlr actions.
 */
auto parse_translation_unit_from_istream(std::istream& input) -> bool;
}  // namespace spider::tdl::parser

#endif  // SPIDER_TDL_PARSER_PARSE_HPP
