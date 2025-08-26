#include "parse.hpp"

#include <istream>
#include <memory>
#include <utility>

#include <antlr4-runtime.h>
#include <boost/outcome/std_result.hpp>

#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/antlr_generated/TaskDefLangLexer.h>
#include <spider/tdl/parser/antlr_generated/TaskDefLangParser.h>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/parser/ErrorListener.hpp>
#include <spider/tdl/parser/Exception.hpp>

namespace spider::tdl::parser {
auto parse_translation_unit_from_istream(std::istream& input)
        -> boost::outcome_v2::std_checked<std::unique_ptr<ast::TranslationUnit>, Error> {
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
    try {
        auto* context{parser.translationUnit()};

        if (lexer_error_listener.has_error()) {
            return lexer_error_listener.error();
        }

        if (parser_error_listener.has_error()) {
            return parser_error_listener.error();
        }

        return std::move(context->tu);
    } catch (Exception const& e) {
        // When an exception is caught, we still prioritize the parser and lexer errors since they
        // are the root cause of the exceptions.

        if (lexer_error_listener.has_error()) {
            return lexer_error_listener.error();
        }

        if (parser_error_listener.has_error()) {
            return parser_error_listener.error();
        }

        return e.to_error();
    }
}
}  // namespace spider::tdl::parser
