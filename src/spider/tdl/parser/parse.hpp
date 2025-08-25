#ifndef SPIDER_TDL_PARSER_PARSE_HPP
#define SPIDER_TDL_PARSER_PARSE_HPP

#include <istream>
#include <memory>

#include <boost/outcome/std_result.hpp>

#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>

namespace spider::tdl::parser {
/**
 * Parses a TDL file as a translation unit from an input stream.
 * @param input The input stream containing the TDL content. The entire content will be considered
 * as a single translation unit.
 * @return A result containing a unique ptr to the parsed translation unit on success, or an error
 * specified by an `Error` instance on failure.
 */
[[nodiscard]] auto parse_translation_unit_from_istream(std::istream& input)
        -> boost::outcome_v2::std_checked<std::unique_ptr<ast::TranslationUnit>, Error>;
}  // namespace spider::tdl::parser

#endif  // SPIDER_TDL_PARSER_PARSE_HPP
