
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

  virtual std::any visitTranslationUnit(TaskDefLangParser::TranslationUnitContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamespace(TaskDefLangParser::NamespaceContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFuncDef(TaskDefLangParser::FuncDefContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRet(TaskDefLangParser::RetContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParams(TaskDefLangParser::ParamsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedVar(TaskDefLangParser::NamedVarContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedVarList(TaskDefLangParser::NamedVarListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStructDef(TaskDefLangParser::StructDefContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitId(TaskDefLangParser::IdContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVarType(TaskDefLangParser::VarTypeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRetType(TaskDefLangParser::RetTypeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVarTypeList(TaskDefLangParser::VarTypeListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitListType(TaskDefLangParser::ListTypeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMapType(TaskDefLangParser::MapTypeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTupleType(TaskDefLangParser::TupleTypeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBuiltinType(TaskDefLangParser::BuiltinTypeContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace spider::tdl::parser::antlr_generated
