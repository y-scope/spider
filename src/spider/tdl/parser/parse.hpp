#ifndef SPIDER_TDL_PARSER_PARSE_HPP
#define SPIDER_TDL_PARSER_PARSE_HPP

#include <istream>

#include <boost/outcome/std_result.hpp>

#include <spider/tdl/Error.hpp>

namespace spider::tdl::parser {
/**
 * Parses a TDL file as a translation unit from an input stream.
 * @param input The input stream containing the TDL content. The entire content will be considered
 * as a single translation unit.
 * @return A void result on success, or an error specified by an `Error` instance on failure.
 * NOTE: We will update this function to return an actual AST root on success once we add Antlr
 * actions.
 */
[[nodiscard]] auto parse_translation_unit_from_istream(std::istream& input)
        -> boost::outcome_v2::std_checked<void, Error>;
}  // namespace spider::tdl::parser

#endif  // SPIDER_TDL_PARSER_PARSE_HPP
