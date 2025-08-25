
#include <memory>
#include <utility>
#include <vector>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/parser/Exception.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>


// Generated from tdl/parser/TaskDefLang.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


namespace spider::tdl::parser::antlr_generated {


class  TaskDefLangParser : public antlr4::Parser {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, ID = 24, SPACE = 25, COMMENT = 26
  };

  enum {
    RuleTranslationUnit = 0, RuleNamespace = 1, RuleFuncDefs = 2, RuleFuncDef = 3, 
    RuleRet = 4, RuleParams = 5, RuleNamedVar = 6, RuleNamedVarList = 7, 
    RuleStructDef = 8, RuleId = 9, RuleVarType = 10, RuleRetType = 11, RuleVarTypeList = 12, 
    RuleListType = 13, RuleMapType = 14, RuleTupleType = 15, RuleBuiltinType = 16
  };

  explicit TaskDefLangParser(antlr4::TokenStream *input);

  TaskDefLangParser(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~TaskDefLangParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


  class TranslationUnitContext;
  class NamespaceContext;
  class FuncDefsContext;
  class FuncDefContext;
  class RetContext;
  class ParamsContext;
  class NamedVarContext;
  class NamedVarListContext;
  class StructDefContext;
  class IdContext;
  class VarTypeContext;
  class RetTypeContext;
  class VarTypeListContext;
  class ListTypeContext;
  class MapTypeContext;
  class TupleTypeContext;
  class BuiltinTypeContext; 

  class  TranslationUnitContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::TranslationUnit> tu;
    TaskDefLangParser::NamespaceContext *namespaceContext = nullptr;
    TaskDefLangParser::StructDefContext *structDefContext = nullptr;
    TranslationUnitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EOF();
    std::vector<NamespaceContext *> namespace_();
    NamespaceContext* namespace_(size_t i);
    std::vector<StructDefContext *> structDef();
    StructDefContext* structDef(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TranslationUnitContext* translationUnit();

  class  NamespaceContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::IdContext *idContext = nullptr;
    TaskDefLangParser::FuncDefsContext *funcDefsContext = nullptr;
    NamespaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdContext *id();
    FuncDefsContext *funcDefs();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NamespaceContext* namespace_();

  class  FuncDefsContext : public antlr4::ParserRuleContext {
  public:
    std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval;
    TaskDefLangParser::FuncDefContext *funcDefContext = nullptr;
    FuncDefsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<FuncDefContext *> funcDef();
    FuncDefContext* funcDef(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FuncDefsContext* funcDefs();

  class  FuncDefContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::IdContext *idContext = nullptr;
    TaskDefLangParser::ParamsContext *paramsContext = nullptr;
    TaskDefLangParser::RetContext *retContext = nullptr;
    FuncDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdContext *id();
    ParamsContext *params();
    RetContext *ret();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FuncDefContext* funcDef();

  class  RetContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::RetTypeContext *retTypeContext = nullptr;
    RetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RetTypeContext *retType();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RetContext* ret();

  class  ParamsContext : public antlr4::ParserRuleContext {
  public:
    std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval;
    TaskDefLangParser::NamedVarListContext *namedVarListContext = nullptr;
    ParamsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NamedVarListContext *namedVarList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParamsContext* params();

  class  NamedVarContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::IdContext *idContext = nullptr;
    TaskDefLangParser::VarTypeContext *varTypeContext = nullptr;
    NamedVarContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdContext *id();
    VarTypeContext *varType();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NamedVarContext* namedVar();

  class  NamedVarListContext : public antlr4::ParserRuleContext {
  public:
    std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval;
    TaskDefLangParser::NamedVarContext *first_named_var = nullptr;
    TaskDefLangParser::NamedVarContext *subsequent_named_var = nullptr;
    NamedVarListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NamedVarContext *> namedVar();
    NamedVarContext* namedVar(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NamedVarListContext* namedVarList();

  class  StructDefContext : public antlr4::ParserRuleContext {
  public:
    std::shared_ptr<spider::tdl::parser::ast::StructSpec> retval;
    TaskDefLangParser::IdContext *idContext = nullptr;
    TaskDefLangParser::NamedVarListContext *namedVarListContext = nullptr;
    StructDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdContext *id();
    NamedVarListContext *namedVarList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StructDefContext* structDef();

  class  IdContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    antlr4::Token *idToken = nullptr;
    IdContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ID();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IdContext* id();

  class  VarTypeContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::BuiltinTypeContext *builtinTypeContext = nullptr;
    TaskDefLangParser::IdContext *idContext = nullptr;
    VarTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BuiltinTypeContext *builtinType();
    IdContext *id();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VarTypeContext* varType();

  class  RetTypeContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::VarTypeContext *varTypeContext = nullptr;
    TaskDefLangParser::TupleTypeContext *tupleTypeContext = nullptr;
    RetTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarTypeContext *varType();
    TupleTypeContext *tupleType();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RetTypeContext* retType();

  class  VarTypeListContext : public antlr4::ParserRuleContext {
  public:
    std::vector<std::unique_ptr<spider::tdl::parser::ast::Node>> retval;
    TaskDefLangParser::VarTypeContext *first_var_type = nullptr;
    TaskDefLangParser::VarTypeContext *subsequent_var_type = nullptr;
    VarTypeListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<VarTypeContext *> varType();
    VarTypeContext* varType(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VarTypeListContext* varTypeList();

  class  ListTypeContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::VarTypeContext *varTypeContext = nullptr;
    ListTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarTypeContext *varType();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ListTypeContext* listType();

  class  MapTypeContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::VarTypeContext *key_type = nullptr;
    TaskDefLangParser::VarTypeContext *val_type = nullptr;
    MapTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<VarTypeContext *> varType();
    VarTypeContext* varType(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MapTypeContext* mapType();

  class  TupleTypeContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::VarTypeListContext *varTypeListContext = nullptr;
    TupleTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarTypeListContext *varTypeList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TupleTypeContext* tupleType();

  class  BuiltinTypeContext : public antlr4::ParserRuleContext {
  public:
    std::unique_ptr<spider::tdl::parser::ast::Node> retval;
    TaskDefLangParser::ListTypeContext *listTypeContext = nullptr;
    TaskDefLangParser::MapTypeContext *mapTypeContext = nullptr;
    BuiltinTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ListTypeContext *listType();
    MapTypeContext *mapType();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  BuiltinTypeContext* builtinType();


  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};

}  // namespace spider::tdl::parser::antlr_generated
