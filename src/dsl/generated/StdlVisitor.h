
// Generated from /data/sitao/code/spider/src/dsl/parser/Stdl.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "StdlParser.h"


namespace spider::dsl::parser {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by StdlParser.
 */
class  StdlVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by StdlParser.
   */
    virtual std::any visitStart(StdlParser::StartContext *context) = 0;

    virtual std::any visitService(StdlParser::ServiceContext *context) = 0;

    virtual std::any visitFunction(StdlParser::FunctionContext *context) = 0;

    virtual std::any visitParameter(StdlParser::ParameterContext *context) = 0;

    virtual std::any visitStruct(StdlParser::StructContext *context) = 0;

    virtual std::any visitField(StdlParser::FieldContext *context) = 0;

    virtual std::any visitReturn_type(StdlParser::Return_typeContext *context) = 0;

    virtual std::any visitType(StdlParser::TypeContext *context) = 0;

    virtual std::any visitBuiltin_type(StdlParser::Builtin_typeContext *context) = 0;


};

}  // namespace spider::dsl::parser
