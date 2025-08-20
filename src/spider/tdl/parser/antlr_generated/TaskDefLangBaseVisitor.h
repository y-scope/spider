
// Generated from tdl/parser/TaskDefLang.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "TaskDefLangVisitor.h"


namespace spider::tdl::parser::antlr_generated {

/**
 * This class provides an empty implementation of TaskDefLangVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  TaskDefLangBaseVisitor : public TaskDefLangVisitor {
public:

  virtual std::any visitStart(TaskDefLangParser::StartContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace spider::tdl::parser::antlr_generated
