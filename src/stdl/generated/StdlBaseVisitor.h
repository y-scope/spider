
// Generated from /data/sitao/code/spider/src/stdl/parser/Stdl.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "StdlVisitor.h"


namespace spider::dsl::parser {

/**
 * This class provides an empty implementation of StdlVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  StdlBaseVisitor : public StdlVisitor {
public:

  virtual std::any visitStart(StdlParser::StartContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitService(StdlParser::ServiceContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunction(StdlParser::FunctionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParameter(StdlParser::ParameterContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStruct(StdlParser::StructContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitField(StdlParser::FieldContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReturn_type(StdlParser::Return_typeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitType(StdlParser::TypeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBuiltin_type(StdlParser::Builtin_typeContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace spider::dsl::parser
