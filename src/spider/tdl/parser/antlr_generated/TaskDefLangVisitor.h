
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
    virtual std::any visitTranslationUnit(TaskDefLangParser::TranslationUnitContext *context) = 0;

    virtual std::any visitNamespace(TaskDefLangParser::NamespaceContext *context) = 0;

    virtual std::any visitFuncDef(TaskDefLangParser::FuncDefContext *context) = 0;

    virtual std::any visitRet(TaskDefLangParser::RetContext *context) = 0;

    virtual std::any visitParams(TaskDefLangParser::ParamsContext *context) = 0;

    virtual std::any visitNamedVar(TaskDefLangParser::NamedVarContext *context) = 0;

    virtual std::any visitNamedVarList(TaskDefLangParser::NamedVarListContext *context) = 0;

    virtual std::any visitStructDef(TaskDefLangParser::StructDefContext *context) = 0;

    virtual std::any visitId(TaskDefLangParser::IdContext *context) = 0;

    virtual std::any visitVarType(TaskDefLangParser::VarTypeContext *context) = 0;

    virtual std::any visitRetType(TaskDefLangParser::RetTypeContext *context) = 0;

    virtual std::any visitVarTypeList(TaskDefLangParser::VarTypeListContext *context) = 0;

    virtual std::any visitListType(TaskDefLangParser::ListTypeContext *context) = 0;

    virtual std::any visitMapType(TaskDefLangParser::MapTypeContext *context) = 0;

    virtual std::any visitTupleType(TaskDefLangParser::TupleTypeContext *context) = 0;

    virtual std::any visitBuiltinType(TaskDefLangParser::BuiltinTypeContext *context) = 0;


};

}  // namespace spider::tdl::parser::antlr_generated
