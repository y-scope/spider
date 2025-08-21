#include "parse.hpp"

#include <istream>
#include <tuple>

#include <antlr4-runtime.h>
#include <boost/outcome/std_result.hpp>

#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/antlr_generated/TaskDefLangLexer.h>
#include <spider/tdl/parser/antlr_generated/TaskDefLangParser.h>
#include <spider/tdl/parser/ErrorListener.hpp>

namespace spider::tdl::parser {
auto parse_translation_unit_from_istream(std::istream& input)
        -> boost::outcome_v2::std_checked<void, Error> {
    // Setup lexer
    antlr4::ANTLRInputStream input_stream{input};
    antlr_generated::TaskDefLangLexer lexer{&input_stream};
    ErrorListener lexer_error_listener{"Lexer"};
    lexer.removeErrorListeners();
    lexer.addErrorListener(&lexer_error_listener);

    // Setup parser
    antlr4::CommonTokenStream token_stream{&lexer};
    antlr_generated::TaskDefLangParser parser{&token_stream};
    ErrorListener parser_error_listener{"Parser"};
    parser.removeErrorListeners();
    parser.addErrorListener(&parser_error_listener);

    // Parse the translation unit
    std::ignore = parser.translationUnit();

    if (lexer_error_listener.has_error()) {
        return lexer_error_listener.error();
    }

    if (parser_error_listener.has_error()) {
        return parser_error_listener.error();
    }

    return boost::outcome_v2::success();
}
}  // namespace spider::tdl::parser
