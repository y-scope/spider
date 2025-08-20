
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
    T__20 = 21, T__21 = 22, ID = 23, SPACE = 24, COMMENT = 25
  };

  enum {
    RuleTranslationUnit = 0, RuleNamespace = 1, RuleFuncDef = 2, RuleRet = 3, 
    RuleParams = 4, RuleNamedVar = 5, RuleNamedVarList = 6, RuleStructDef = 7, 
    RuleId = 8, RuleType = 9, RuleTypeList = 10, RuleBuiltinType = 11
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
  class FuncDefContext;
  class RetContext;
  class ParamsContext;
  class NamedVarContext;
  class NamedVarListContext;
  class StructDefContext;
  class IdContext;
  class TypeContext;
  class TypeListContext;
  class BuiltinTypeContext; 

  class  TranslationUnitContext : public antlr4::ParserRuleContext {
  public:
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
    NamespaceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdContext *id();
    std::vector<FuncDefContext *> funcDef();
    FuncDefContext* funcDef(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NamespaceContext* namespace_();

  class  FuncDefContext : public antlr4::ParserRuleContext {
  public:
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
    RetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TypeContext *type();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RetContext* ret();

  class  ParamsContext : public antlr4::ParserRuleContext {
  public:
    ParamsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NamedVarListContext *namedVarList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParamsContext* params();

  class  NamedVarContext : public antlr4::ParserRuleContext {
  public:
    NamedVarContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdContext *id();
    TypeContext *type();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NamedVarContext* namedVar();

  class  NamedVarListContext : public antlr4::ParserRuleContext {
  public:
    NamedVarListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NamedVarContext *namedVar();
    NamedVarListContext *namedVarList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NamedVarListContext* namedVarList();
  NamedVarListContext* namedVarList(int precedence);
  class  StructDefContext : public antlr4::ParserRuleContext {
  public:
    StructDefContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdContext *id();
    NamedVarListContext *namedVarList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StructDefContext* structDef();

  class  IdContext : public antlr4::ParserRuleContext {
  public:
    IdContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ID();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IdContext* id();

  class  TypeContext : public antlr4::ParserRuleContext {
  public:
    TypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BuiltinTypeContext *builtinType();
    IdContext *id();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TypeContext* type();

  class  TypeListContext : public antlr4::ParserRuleContext {
  public:
    TypeListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TypeContext *type();
    TypeListContext *typeList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TypeListContext* typeList();
  TypeListContext* typeList(int precedence);
  class  BuiltinTypeContext : public antlr4::ParserRuleContext {
  public:
    BuiltinTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TypeContext *> type();
    TypeContext* type(size_t i);
    TypeListContext *typeList();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  BuiltinTypeContext* builtinType();


  bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;

  bool namedVarListSempred(NamedVarListContext *_localctx, size_t predicateIndex);
  bool typeListSempred(TypeListContext *_localctx, size_t predicateIndex);

  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};

}  // namespace spider::tdl::parser::antlr_generated
