
// Generated from tdl/parser/TaskDefLang.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "TaskDefLangParser.h"


namespace spider::tdl::parser::antlr_generated {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by TaskDefLangParser.
 */
class  TaskDefLangVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by TaskDefLangParser.
   */
    virtual std::any visitStart(TaskDefLangParser::StartContext *context) = 0;


};

}  // namespace spider::tdl::parser::antlr_generated
