#include "parse.hpp"

#include <antlr4-runtime.h>

#include <istream>

#include <spider/tdl/parser/antlr_generated/TaskDefLangLexer.h>
#include <spider/tdl/parser/antlr_generated/TaskDefLangParser.h>

namespace spider::tdl::parser {
auto parse_translation_unit_from_istream(std::istream& input) -> bool {
    antlr4::ANTLRInputStream input_stream{input};
    antlr_generated::TaskDefLangLexer lexer{&input_stream};
    antlr4::CommonTokenStream token_stream{&lexer};
    antlr_generated::TaskDefLangParser parser{&token_stream};

    return nullptr != parser.translationUnit();
}
}  // namespace spider::tdl::parser
