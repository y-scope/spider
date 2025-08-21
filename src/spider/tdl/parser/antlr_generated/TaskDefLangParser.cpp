
// Generated from tdl/parser/TaskDefLang.g4 by ANTLR 4.13.2


#include "TaskDefLangVisitor.h"

#include "TaskDefLangParser.h"


using namespace antlrcpp;
using namespace spider::tdl::parser::antlr_generated;

using namespace antlr4;

namespace {

struct TaskDefLangParserStaticData final {
  TaskDefLangParserStaticData(std::vector<std::string> ruleNames,
                        std::vector<std::string> literalNames,
                        std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  TaskDefLangParserStaticData(const TaskDefLangParserStaticData&) = delete;
  TaskDefLangParserStaticData(TaskDefLangParserStaticData&&) = delete;
  TaskDefLangParserStaticData& operator=(const TaskDefLangParserStaticData&) = delete;
  TaskDefLangParserStaticData& operator=(TaskDefLangParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag taskdeflangParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
std::unique_ptr<TaskDefLangParserStaticData> taskdeflangParserStaticData = nullptr;

void taskdeflangParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (taskdeflangParserStaticData != nullptr) {
    return;
  }
#else
  assert(taskdeflangParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<TaskDefLangParserStaticData>(
    std::vector<std::string>{
      "translationUnit", "namespace", "funcDef", "ret", "params", "namedVar", 
      "namedVarList", "structDef", "id", "varType", "retType", "varTypeList", 
      "listType", "mapType", "tupleType", "builtinType"
    },
    std::vector<std::string>{
      "", "'namespace'", "'{'", "'}'", "'fn'", "'('", "')'", "';'", "'->'", 
      "':'", "','", "'struct'", "'List'", "'<'", "'>'", "'Map'", "'Tuple'", 
      "'int8'", "'int16'", "'int32'", "'int64'", "'float'", "'double'", 
      "'bool'"
    },
    std::vector<std::string>{
      "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
      "", "", "", "", "", "", "", "ID", "SPACE", "COMMENT"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,26,146,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,
  	14,2,15,7,15,1,0,1,0,5,0,35,8,0,10,0,12,0,38,9,0,1,0,1,0,1,1,1,1,1,1,
  	1,1,5,1,46,8,1,10,1,12,1,49,9,1,1,1,1,1,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,
  	2,1,3,1,3,1,3,3,3,64,8,3,1,4,1,4,3,4,68,8,4,1,5,1,5,1,5,1,5,1,6,1,6,1,
  	6,1,6,1,6,1,6,5,6,80,8,6,10,6,12,6,83,9,6,1,7,1,7,1,7,1,7,1,7,3,7,90,
  	8,7,1,7,1,7,1,7,1,8,1,8,1,9,1,9,3,9,99,8,9,1,10,1,10,3,10,103,8,10,1,
  	11,1,11,1,11,3,11,108,8,11,1,11,1,11,1,11,5,11,113,8,11,10,11,12,11,116,
  	9,11,1,12,1,12,1,12,1,12,1,12,1,13,1,13,1,13,1,13,1,13,1,13,1,13,1,14,
  	1,14,1,14,1,14,1,14,1,15,1,15,1,15,1,15,1,15,1,15,1,15,1,15,1,15,3,15,
  	144,8,15,1,15,0,2,12,22,16,0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,
  	0,0,148,0,36,1,0,0,0,2,41,1,0,0,0,4,52,1,0,0,0,6,63,1,0,0,0,8,67,1,0,
  	0,0,10,69,1,0,0,0,12,73,1,0,0,0,14,84,1,0,0,0,16,94,1,0,0,0,18,98,1,0,
  	0,0,20,102,1,0,0,0,22,107,1,0,0,0,24,117,1,0,0,0,26,122,1,0,0,0,28,129,
  	1,0,0,0,30,143,1,0,0,0,32,35,3,2,1,0,33,35,3,14,7,0,34,32,1,0,0,0,34,
  	33,1,0,0,0,35,38,1,0,0,0,36,34,1,0,0,0,36,37,1,0,0,0,37,39,1,0,0,0,38,
  	36,1,0,0,0,39,40,5,0,0,1,40,1,1,0,0,0,41,42,5,1,0,0,42,43,3,16,8,0,43,
  	47,5,2,0,0,44,46,3,4,2,0,45,44,1,0,0,0,46,49,1,0,0,0,47,45,1,0,0,0,47,
  	48,1,0,0,0,48,50,1,0,0,0,49,47,1,0,0,0,50,51,5,3,0,0,51,3,1,0,0,0,52,
  	53,5,4,0,0,53,54,3,16,8,0,54,55,5,5,0,0,55,56,3,8,4,0,56,57,5,6,0,0,57,
  	58,3,6,3,0,58,59,5,7,0,0,59,5,1,0,0,0,60,61,5,8,0,0,61,64,3,20,10,0,62,
  	64,1,0,0,0,63,60,1,0,0,0,63,62,1,0,0,0,64,7,1,0,0,0,65,68,3,12,6,0,66,
  	68,1,0,0,0,67,65,1,0,0,0,67,66,1,0,0,0,68,9,1,0,0,0,69,70,3,16,8,0,70,
  	71,5,9,0,0,71,72,3,18,9,0,72,11,1,0,0,0,73,74,6,6,-1,0,74,75,3,10,5,0,
  	75,81,1,0,0,0,76,77,10,1,0,0,77,78,5,10,0,0,78,80,3,10,5,0,79,76,1,0,
  	0,0,80,83,1,0,0,0,81,79,1,0,0,0,81,82,1,0,0,0,82,13,1,0,0,0,83,81,1,0,
  	0,0,84,85,5,11,0,0,85,86,3,16,8,0,86,87,5,2,0,0,87,89,3,12,6,0,88,90,
  	5,10,0,0,89,88,1,0,0,0,89,90,1,0,0,0,90,91,1,0,0,0,91,92,5,3,0,0,92,93,
  	5,7,0,0,93,15,1,0,0,0,94,95,5,24,0,0,95,17,1,0,0,0,96,99,3,30,15,0,97,
  	99,3,16,8,0,98,96,1,0,0,0,98,97,1,0,0,0,99,19,1,0,0,0,100,103,3,18,9,
  	0,101,103,3,28,14,0,102,100,1,0,0,0,102,101,1,0,0,0,103,21,1,0,0,0,104,
  	105,6,11,-1,0,105,108,3,18,9,0,106,108,1,0,0,0,107,104,1,0,0,0,107,106,
  	1,0,0,0,108,114,1,0,0,0,109,110,10,2,0,0,110,111,5,10,0,0,111,113,3,18,
  	9,0,112,109,1,0,0,0,113,116,1,0,0,0,114,112,1,0,0,0,114,115,1,0,0,0,115,
  	23,1,0,0,0,116,114,1,0,0,0,117,118,5,12,0,0,118,119,5,13,0,0,119,120,
  	3,18,9,0,120,121,5,14,0,0,121,25,1,0,0,0,122,123,5,15,0,0,123,124,5,13,
  	0,0,124,125,3,18,9,0,125,126,5,10,0,0,126,127,3,18,9,0,127,128,5,14,0,
  	0,128,27,1,0,0,0,129,130,5,16,0,0,130,131,5,13,0,0,131,132,3,22,11,0,
  	132,133,5,14,0,0,133,29,1,0,0,0,134,144,5,17,0,0,135,144,5,18,0,0,136,
  	144,5,19,0,0,137,144,5,20,0,0,138,144,5,21,0,0,139,144,5,22,0,0,140,144,
  	5,23,0,0,141,144,3,24,12,0,142,144,3,26,13,0,143,134,1,0,0,0,143,135,
  	1,0,0,0,143,136,1,0,0,0,143,137,1,0,0,0,143,138,1,0,0,0,143,139,1,0,0,
  	0,143,140,1,0,0,0,143,141,1,0,0,0,143,142,1,0,0,0,144,31,1,0,0,0,12,34,
  	36,47,63,67,81,89,98,102,107,114,143
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  taskdeflangParserStaticData = std::move(staticData);
}

}

TaskDefLangParser::TaskDefLangParser(TokenStream *input) : TaskDefLangParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

TaskDefLangParser::TaskDefLangParser(TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options) : Parser(input) {
  TaskDefLangParser::initialize();
  _interpreter = new atn::ParserATNSimulator(this, *taskdeflangParserStaticData->atn, taskdeflangParserStaticData->decisionToDFA, taskdeflangParserStaticData->sharedContextCache, options);
}

TaskDefLangParser::~TaskDefLangParser() {
  delete _interpreter;
}

const atn::ATN& TaskDefLangParser::getATN() const {
  return *taskdeflangParserStaticData->atn;
}

std::string TaskDefLangParser::getGrammarFileName() const {
  return "TaskDefLang.g4";
}

const std::vector<std::string>& TaskDefLangParser::getRuleNames() const {
  return taskdeflangParserStaticData->ruleNames;
}

const dfa::Vocabulary& TaskDefLangParser::getVocabulary() const {
  return taskdeflangParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView TaskDefLangParser::getSerializedATN() const {
  return taskdeflangParserStaticData->serializedATN;
}


//----------------- TranslationUnitContext ------------------------------------------------------------------

TaskDefLangParser::TranslationUnitContext::TranslationUnitContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* TaskDefLangParser::TranslationUnitContext::EOF() {
  return getToken(TaskDefLangParser::EOF, 0);
}

std::vector<TaskDefLangParser::NamespaceContext *> TaskDefLangParser::TranslationUnitContext::namespace_() {
  return getRuleContexts<TaskDefLangParser::NamespaceContext>();
}

TaskDefLangParser::NamespaceContext* TaskDefLangParser::TranslationUnitContext::namespace_(size_t i) {
  return getRuleContext<TaskDefLangParser::NamespaceContext>(i);
}

std::vector<TaskDefLangParser::StructDefContext *> TaskDefLangParser::TranslationUnitContext::structDef() {
  return getRuleContexts<TaskDefLangParser::StructDefContext>();
}

TaskDefLangParser::StructDefContext* TaskDefLangParser::TranslationUnitContext::structDef(size_t i) {
  return getRuleContext<TaskDefLangParser::StructDefContext>(i);
}


size_t TaskDefLangParser::TranslationUnitContext::getRuleIndex() const {
  return TaskDefLangParser::RuleTranslationUnit;
}


std::any TaskDefLangParser::TranslationUnitContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitTranslationUnit(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::TranslationUnitContext* TaskDefLangParser::translationUnit() {
  TranslationUnitContext *_localctx = _tracker.createInstance<TranslationUnitContext>(_ctx, getState());
  enterRule(_localctx, 0, TaskDefLangParser::RuleTranslationUnit);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(36);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TaskDefLangParser::T__0

    || _la == TaskDefLangParser::T__10) {
      setState(34);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case TaskDefLangParser::T__0: {
          setState(32);
          namespace_();
          break;
        }

        case TaskDefLangParser::T__10: {
          setState(33);
          structDef();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(38);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(39);
    match(TaskDefLangParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamespaceContext ------------------------------------------------------------------

TaskDefLangParser::NamespaceContext::NamespaceContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::IdContext* TaskDefLangParser::NamespaceContext::id() {
  return getRuleContext<TaskDefLangParser::IdContext>(0);
}

std::vector<TaskDefLangParser::FuncDefContext *> TaskDefLangParser::NamespaceContext::funcDef() {
  return getRuleContexts<TaskDefLangParser::FuncDefContext>();
}

TaskDefLangParser::FuncDefContext* TaskDefLangParser::NamespaceContext::funcDef(size_t i) {
  return getRuleContext<TaskDefLangParser::FuncDefContext>(i);
}


size_t TaskDefLangParser::NamespaceContext::getRuleIndex() const {
  return TaskDefLangParser::RuleNamespace;
}


std::any TaskDefLangParser::NamespaceContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitNamespace(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::NamespaceContext* TaskDefLangParser::namespace_() {
  NamespaceContext *_localctx = _tracker.createInstance<NamespaceContext>(_ctx, getState());
  enterRule(_localctx, 2, TaskDefLangParser::RuleNamespace);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(41);
    match(TaskDefLangParser::T__0);
    setState(42);
    id();
    setState(43);
    match(TaskDefLangParser::T__1);
    setState(47);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TaskDefLangParser::T__3) {
      setState(44);
      funcDef();
      setState(49);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(50);
    match(TaskDefLangParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FuncDefContext ------------------------------------------------------------------

TaskDefLangParser::FuncDefContext::FuncDefContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::IdContext* TaskDefLangParser::FuncDefContext::id() {
  return getRuleContext<TaskDefLangParser::IdContext>(0);
}

TaskDefLangParser::ParamsContext* TaskDefLangParser::FuncDefContext::params() {
  return getRuleContext<TaskDefLangParser::ParamsContext>(0);
}

TaskDefLangParser::RetContext* TaskDefLangParser::FuncDefContext::ret() {
  return getRuleContext<TaskDefLangParser::RetContext>(0);
}


size_t TaskDefLangParser::FuncDefContext::getRuleIndex() const {
  return TaskDefLangParser::RuleFuncDef;
}


std::any TaskDefLangParser::FuncDefContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitFuncDef(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::FuncDefContext* TaskDefLangParser::funcDef() {
  FuncDefContext *_localctx = _tracker.createInstance<FuncDefContext>(_ctx, getState());
  enterRule(_localctx, 4, TaskDefLangParser::RuleFuncDef);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(52);
    match(TaskDefLangParser::T__3);
    setState(53);
    id();
    setState(54);
    match(TaskDefLangParser::T__4);
    setState(55);
    params();
    setState(56);
    match(TaskDefLangParser::T__5);
    setState(57);
    ret();
    setState(58);
    match(TaskDefLangParser::T__6);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RetContext ------------------------------------------------------------------

TaskDefLangParser::RetContext::RetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::RetTypeContext* TaskDefLangParser::RetContext::retType() {
  return getRuleContext<TaskDefLangParser::RetTypeContext>(0);
}


size_t TaskDefLangParser::RetContext::getRuleIndex() const {
  return TaskDefLangParser::RuleRet;
}


std::any TaskDefLangParser::RetContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitRet(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::RetContext* TaskDefLangParser::ret() {
  RetContext *_localctx = _tracker.createInstance<RetContext>(_ctx, getState());
  enterRule(_localctx, 6, TaskDefLangParser::RuleRet);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(63);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__7: {
        enterOuterAlt(_localctx, 1);
        setState(60);
        match(TaskDefLangParser::T__7);
        setState(61);
        retType();
        break;
      }

      case TaskDefLangParser::T__6: {
        enterOuterAlt(_localctx, 2);

        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParamsContext ------------------------------------------------------------------

TaskDefLangParser::ParamsContext::ParamsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::NamedVarListContext* TaskDefLangParser::ParamsContext::namedVarList() {
  return getRuleContext<TaskDefLangParser::NamedVarListContext>(0);
}


size_t TaskDefLangParser::ParamsContext::getRuleIndex() const {
  return TaskDefLangParser::RuleParams;
}


std::any TaskDefLangParser::ParamsContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitParams(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::ParamsContext* TaskDefLangParser::params() {
  ParamsContext *_localctx = _tracker.createInstance<ParamsContext>(_ctx, getState());
  enterRule(_localctx, 8, TaskDefLangParser::RuleParams);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(67);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::ID: {
        enterOuterAlt(_localctx, 1);
        setState(65);
        namedVarList(0);
        break;
      }

      case TaskDefLangParser::T__5: {
        enterOuterAlt(_localctx, 2);

        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedVarContext ------------------------------------------------------------------

TaskDefLangParser::NamedVarContext::NamedVarContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::IdContext* TaskDefLangParser::NamedVarContext::id() {
  return getRuleContext<TaskDefLangParser::IdContext>(0);
}

TaskDefLangParser::VarTypeContext* TaskDefLangParser::NamedVarContext::varType() {
  return getRuleContext<TaskDefLangParser::VarTypeContext>(0);
}


size_t TaskDefLangParser::NamedVarContext::getRuleIndex() const {
  return TaskDefLangParser::RuleNamedVar;
}


std::any TaskDefLangParser::NamedVarContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitNamedVar(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::NamedVarContext* TaskDefLangParser::namedVar() {
  NamedVarContext *_localctx = _tracker.createInstance<NamedVarContext>(_ctx, getState());
  enterRule(_localctx, 10, TaskDefLangParser::RuleNamedVar);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(69);
    id();
    setState(70);
    match(TaskDefLangParser::T__8);
    setState(71);
    varType();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedVarListContext ------------------------------------------------------------------

TaskDefLangParser::NamedVarListContext::NamedVarListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::NamedVarContext* TaskDefLangParser::NamedVarListContext::namedVar() {
  return getRuleContext<TaskDefLangParser::NamedVarContext>(0);
}

TaskDefLangParser::NamedVarListContext* TaskDefLangParser::NamedVarListContext::namedVarList() {
  return getRuleContext<TaskDefLangParser::NamedVarListContext>(0);
}


size_t TaskDefLangParser::NamedVarListContext::getRuleIndex() const {
  return TaskDefLangParser::RuleNamedVarList;
}


std::any TaskDefLangParser::NamedVarListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitNamedVarList(this);
  else
    return visitor->visitChildren(this);
}


TaskDefLangParser::NamedVarListContext* TaskDefLangParser::namedVarList() {
   return namedVarList(0);
}

TaskDefLangParser::NamedVarListContext* TaskDefLangParser::namedVarList(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  TaskDefLangParser::NamedVarListContext *_localctx = _tracker.createInstance<NamedVarListContext>(_ctx, parentState);
  TaskDefLangParser::NamedVarListContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 12;
  enterRecursionRule(_localctx, 12, TaskDefLangParser::RuleNamedVarList, precedence);

    

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(74);
    namedVar();
    _ctx->stop = _input->LT(-1);
    setState(81);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        _localctx = _tracker.createInstance<NamedVarListContext>(parentContext, parentState);
        pushNewRecursionContext(_localctx, startState, RuleNamedVarList);
        setState(76);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(77);
        match(TaskDefLangParser::T__9);
        setState(78);
        namedVar(); 
      }
      setState(83);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- StructDefContext ------------------------------------------------------------------

TaskDefLangParser::StructDefContext::StructDefContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::IdContext* TaskDefLangParser::StructDefContext::id() {
  return getRuleContext<TaskDefLangParser::IdContext>(0);
}

TaskDefLangParser::NamedVarListContext* TaskDefLangParser::StructDefContext::namedVarList() {
  return getRuleContext<TaskDefLangParser::NamedVarListContext>(0);
}


size_t TaskDefLangParser::StructDefContext::getRuleIndex() const {
  return TaskDefLangParser::RuleStructDef;
}


std::any TaskDefLangParser::StructDefContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitStructDef(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::StructDefContext* TaskDefLangParser::structDef() {
  StructDefContext *_localctx = _tracker.createInstance<StructDefContext>(_ctx, getState());
  enterRule(_localctx, 14, TaskDefLangParser::RuleStructDef);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(84);
    match(TaskDefLangParser::T__10);
    setState(85);
    id();
    setState(86);
    match(TaskDefLangParser::T__1);
    setState(87);
    namedVarList(0);
    setState(89);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == TaskDefLangParser::T__9) {
      setState(88);
      match(TaskDefLangParser::T__9);
    }
    setState(91);
    match(TaskDefLangParser::T__2);
    setState(92);
    match(TaskDefLangParser::T__6);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdContext ------------------------------------------------------------------

TaskDefLangParser::IdContext::IdContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* TaskDefLangParser::IdContext::ID() {
  return getToken(TaskDefLangParser::ID, 0);
}


size_t TaskDefLangParser::IdContext::getRuleIndex() const {
  return TaskDefLangParser::RuleId;
}


std::any TaskDefLangParser::IdContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitId(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::IdContext* TaskDefLangParser::id() {
  IdContext *_localctx = _tracker.createInstance<IdContext>(_ctx, getState());
  enterRule(_localctx, 16, TaskDefLangParser::RuleId);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(94);
    match(TaskDefLangParser::ID);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarTypeContext ------------------------------------------------------------------

TaskDefLangParser::VarTypeContext::VarTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::BuiltinTypeContext* TaskDefLangParser::VarTypeContext::builtinType() {
  return getRuleContext<TaskDefLangParser::BuiltinTypeContext>(0);
}

TaskDefLangParser::IdContext* TaskDefLangParser::VarTypeContext::id() {
  return getRuleContext<TaskDefLangParser::IdContext>(0);
}


size_t TaskDefLangParser::VarTypeContext::getRuleIndex() const {
  return TaskDefLangParser::RuleVarType;
}


std::any TaskDefLangParser::VarTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitVarType(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::VarTypeContext* TaskDefLangParser::varType() {
  VarTypeContext *_localctx = _tracker.createInstance<VarTypeContext>(_ctx, getState());
  enterRule(_localctx, 18, TaskDefLangParser::RuleVarType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(98);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__11:
      case TaskDefLangParser::T__14:
      case TaskDefLangParser::T__16:
      case TaskDefLangParser::T__17:
      case TaskDefLangParser::T__18:
      case TaskDefLangParser::T__19:
      case TaskDefLangParser::T__20:
      case TaskDefLangParser::T__21:
      case TaskDefLangParser::T__22: {
        enterOuterAlt(_localctx, 1);
        setState(96);
        builtinType();
        break;
      }

      case TaskDefLangParser::ID: {
        enterOuterAlt(_localctx, 2);
        setState(97);
        id();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RetTypeContext ------------------------------------------------------------------

TaskDefLangParser::RetTypeContext::RetTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::VarTypeContext* TaskDefLangParser::RetTypeContext::varType() {
  return getRuleContext<TaskDefLangParser::VarTypeContext>(0);
}

TaskDefLangParser::TupleTypeContext* TaskDefLangParser::RetTypeContext::tupleType() {
  return getRuleContext<TaskDefLangParser::TupleTypeContext>(0);
}


size_t TaskDefLangParser::RetTypeContext::getRuleIndex() const {
  return TaskDefLangParser::RuleRetType;
}


std::any TaskDefLangParser::RetTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitRetType(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::RetTypeContext* TaskDefLangParser::retType() {
  RetTypeContext *_localctx = _tracker.createInstance<RetTypeContext>(_ctx, getState());
  enterRule(_localctx, 20, TaskDefLangParser::RuleRetType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(102);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__11:
      case TaskDefLangParser::T__14:
      case TaskDefLangParser::T__16:
      case TaskDefLangParser::T__17:
      case TaskDefLangParser::T__18:
      case TaskDefLangParser::T__19:
      case TaskDefLangParser::T__20:
      case TaskDefLangParser::T__21:
      case TaskDefLangParser::T__22:
      case TaskDefLangParser::ID: {
        enterOuterAlt(_localctx, 1);
        setState(100);
        varType();
        break;
      }

      case TaskDefLangParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(101);
        tupleType();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarTypeListContext ------------------------------------------------------------------

TaskDefLangParser::VarTypeListContext::VarTypeListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::VarTypeContext* TaskDefLangParser::VarTypeListContext::varType() {
  return getRuleContext<TaskDefLangParser::VarTypeContext>(0);
}

TaskDefLangParser::VarTypeListContext* TaskDefLangParser::VarTypeListContext::varTypeList() {
  return getRuleContext<TaskDefLangParser::VarTypeListContext>(0);
}


size_t TaskDefLangParser::VarTypeListContext::getRuleIndex() const {
  return TaskDefLangParser::RuleVarTypeList;
}


std::any TaskDefLangParser::VarTypeListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitVarTypeList(this);
  else
    return visitor->visitChildren(this);
}


TaskDefLangParser::VarTypeListContext* TaskDefLangParser::varTypeList() {
   return varTypeList(0);
}

TaskDefLangParser::VarTypeListContext* TaskDefLangParser::varTypeList(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  TaskDefLangParser::VarTypeListContext *_localctx = _tracker.createInstance<VarTypeListContext>(_ctx, parentState);
  TaskDefLangParser::VarTypeListContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 22;
  enterRecursionRule(_localctx, 22, TaskDefLangParser::RuleVarTypeList, precedence);

    

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(107);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      setState(105);
      varType();
      break;
    }

    case 2: {
      break;
    }

    default:
      break;
    }
    _ctx->stop = _input->LT(-1);
    setState(114);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        _localctx = _tracker.createInstance<VarTypeListContext>(parentContext, parentState);
        pushNewRecursionContext(_localctx, startState, RuleVarTypeList);
        setState(109);

        if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
        setState(110);
        match(TaskDefLangParser::T__9);
        setState(111);
        varType(); 
      }
      setState(116);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- ListTypeContext ------------------------------------------------------------------

TaskDefLangParser::ListTypeContext::ListTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::VarTypeContext* TaskDefLangParser::ListTypeContext::varType() {
  return getRuleContext<TaskDefLangParser::VarTypeContext>(0);
}


size_t TaskDefLangParser::ListTypeContext::getRuleIndex() const {
  return TaskDefLangParser::RuleListType;
}


std::any TaskDefLangParser::ListTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitListType(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::ListTypeContext* TaskDefLangParser::listType() {
  ListTypeContext *_localctx = _tracker.createInstance<ListTypeContext>(_ctx, getState());
  enterRule(_localctx, 24, TaskDefLangParser::RuleListType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(117);
    match(TaskDefLangParser::T__11);
    setState(118);
    match(TaskDefLangParser::T__12);
    setState(119);
    varType();
    setState(120);
    match(TaskDefLangParser::T__13);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MapTypeContext ------------------------------------------------------------------

TaskDefLangParser::MapTypeContext::MapTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<TaskDefLangParser::VarTypeContext *> TaskDefLangParser::MapTypeContext::varType() {
  return getRuleContexts<TaskDefLangParser::VarTypeContext>();
}

TaskDefLangParser::VarTypeContext* TaskDefLangParser::MapTypeContext::varType(size_t i) {
  return getRuleContext<TaskDefLangParser::VarTypeContext>(i);
}


size_t TaskDefLangParser::MapTypeContext::getRuleIndex() const {
  return TaskDefLangParser::RuleMapType;
}


std::any TaskDefLangParser::MapTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitMapType(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::MapTypeContext* TaskDefLangParser::mapType() {
  MapTypeContext *_localctx = _tracker.createInstance<MapTypeContext>(_ctx, getState());
  enterRule(_localctx, 26, TaskDefLangParser::RuleMapType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(122);
    match(TaskDefLangParser::T__14);
    setState(123);
    match(TaskDefLangParser::T__12);
    setState(124);
    varType();
    setState(125);
    match(TaskDefLangParser::T__9);
    setState(126);
    varType();
    setState(127);
    match(TaskDefLangParser::T__13);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TupleTypeContext ------------------------------------------------------------------

TaskDefLangParser::TupleTypeContext::TupleTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::VarTypeListContext* TaskDefLangParser::TupleTypeContext::varTypeList() {
  return getRuleContext<TaskDefLangParser::VarTypeListContext>(0);
}


size_t TaskDefLangParser::TupleTypeContext::getRuleIndex() const {
  return TaskDefLangParser::RuleTupleType;
}


std::any TaskDefLangParser::TupleTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitTupleType(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::TupleTypeContext* TaskDefLangParser::tupleType() {
  TupleTypeContext *_localctx = _tracker.createInstance<TupleTypeContext>(_ctx, getState());
  enterRule(_localctx, 28, TaskDefLangParser::RuleTupleType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(129);
    match(TaskDefLangParser::T__15);
    setState(130);
    match(TaskDefLangParser::T__12);
    setState(131);
    varTypeList(0);
    setState(132);
    match(TaskDefLangParser::T__13);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BuiltinTypeContext ------------------------------------------------------------------

TaskDefLangParser::BuiltinTypeContext::BuiltinTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::ListTypeContext* TaskDefLangParser::BuiltinTypeContext::listType() {
  return getRuleContext<TaskDefLangParser::ListTypeContext>(0);
}

TaskDefLangParser::MapTypeContext* TaskDefLangParser::BuiltinTypeContext::mapType() {
  return getRuleContext<TaskDefLangParser::MapTypeContext>(0);
}


size_t TaskDefLangParser::BuiltinTypeContext::getRuleIndex() const {
  return TaskDefLangParser::RuleBuiltinType;
}


std::any TaskDefLangParser::BuiltinTypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitBuiltinType(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::BuiltinTypeContext* TaskDefLangParser::builtinType() {
  BuiltinTypeContext *_localctx = _tracker.createInstance<BuiltinTypeContext>(_ctx, getState());
  enterRule(_localctx, 30, TaskDefLangParser::RuleBuiltinType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(143);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__16: {
        enterOuterAlt(_localctx, 1);
        setState(134);
        match(TaskDefLangParser::T__16);
        break;
      }

      case TaskDefLangParser::T__17: {
        enterOuterAlt(_localctx, 2);
        setState(135);
        match(TaskDefLangParser::T__17);
        break;
      }

      case TaskDefLangParser::T__18: {
        enterOuterAlt(_localctx, 3);
        setState(136);
        match(TaskDefLangParser::T__18);
        break;
      }

      case TaskDefLangParser::T__19: {
        enterOuterAlt(_localctx, 4);
        setState(137);
        match(TaskDefLangParser::T__19);
        break;
      }

      case TaskDefLangParser::T__20: {
        enterOuterAlt(_localctx, 5);
        setState(138);
        match(TaskDefLangParser::T__20);
        break;
      }

      case TaskDefLangParser::T__21: {
        enterOuterAlt(_localctx, 6);
        setState(139);
        match(TaskDefLangParser::T__21);
        break;
      }

      case TaskDefLangParser::T__22: {
        enterOuterAlt(_localctx, 7);
        setState(140);
        match(TaskDefLangParser::T__22);
        break;
      }

      case TaskDefLangParser::T__11: {
        enterOuterAlt(_localctx, 8);
        setState(141);
        listType();
        break;
      }

      case TaskDefLangParser::T__14: {
        enterOuterAlt(_localctx, 9);
        setState(142);
        mapType();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool TaskDefLangParser::sempred(RuleContext *context, size_t ruleIndex, size_t predicateIndex) {
  switch (ruleIndex) {
    case 6: return namedVarListSempred(antlrcpp::downCast<NamedVarListContext *>(context), predicateIndex);
    case 11: return varTypeListSempred(antlrcpp::downCast<VarTypeListContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool TaskDefLangParser::namedVarListSempred(NamedVarListContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return precpred(_ctx, 1);

  default:
    break;
  }
  return true;
}

bool TaskDefLangParser::varTypeListSempred(VarTypeListContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 1: return precpred(_ctx, 2);

  default:
    break;
  }
  return true;
}

void TaskDefLangParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  taskdeflangParserInitialize();
#else
  ::antlr4::internal::call_once(taskdeflangParserOnceFlag, taskdeflangParserInitialize);
#endif
}
