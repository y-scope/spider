
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
      "namedVarList", "structDef", "id", "type", "typeList", "builtinType"
    },
    std::vector<std::string>{
      "", "'namespace'", "'{'", "'}'", "'fn'", "'('", "')'", "';'", "'->'", 
      "':'", "','", "'struct'", "'int8'", "'int16'", "'int32'", "'int64'", 
      "'float'", "'double'", "'bool'", "'List<'", "'>'", "'Map<'", "'Tuple<'"
    },
    std::vector<std::string>{
      "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
      "", "", "", "", "", "", "ID", "SPACE", "COMMENT"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,25,129,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,1,0,1,0,5,0,27,8,0,10,0,12,
  	0,30,9,0,1,0,1,0,1,1,1,1,1,1,1,1,5,1,38,8,1,10,1,12,1,41,9,1,1,1,1,1,
  	1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,3,1,3,1,3,3,3,56,8,3,1,4,1,4,3,4,60,
  	8,4,1,5,1,5,1,5,1,5,1,6,1,6,1,6,1,6,1,6,1,6,5,6,72,8,6,10,6,12,6,75,9,
  	6,1,7,1,7,1,7,1,7,1,7,3,7,82,8,7,1,7,1,7,1,7,1,8,1,8,1,9,1,9,3,9,91,8,
  	9,1,10,1,10,1,10,3,10,96,8,10,1,10,1,10,1,10,5,10,101,8,10,10,10,12,10,
  	104,9,10,1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,11,
  	1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,11,1,11,3,11,127,8,11,1,11,0,2,12,
  	20,12,0,2,4,6,8,10,12,14,16,18,20,22,0,0,135,0,28,1,0,0,0,2,33,1,0,0,
  	0,4,44,1,0,0,0,6,55,1,0,0,0,8,59,1,0,0,0,10,61,1,0,0,0,12,65,1,0,0,0,
  	14,76,1,0,0,0,16,86,1,0,0,0,18,90,1,0,0,0,20,95,1,0,0,0,22,126,1,0,0,
  	0,24,27,3,2,1,0,25,27,3,14,7,0,26,24,1,0,0,0,26,25,1,0,0,0,27,30,1,0,
  	0,0,28,26,1,0,0,0,28,29,1,0,0,0,29,31,1,0,0,0,30,28,1,0,0,0,31,32,5,0,
  	0,1,32,1,1,0,0,0,33,34,5,1,0,0,34,35,3,16,8,0,35,39,5,2,0,0,36,38,3,4,
  	2,0,37,36,1,0,0,0,38,41,1,0,0,0,39,37,1,0,0,0,39,40,1,0,0,0,40,42,1,0,
  	0,0,41,39,1,0,0,0,42,43,5,3,0,0,43,3,1,0,0,0,44,45,5,4,0,0,45,46,3,16,
  	8,0,46,47,5,5,0,0,47,48,3,8,4,0,48,49,5,6,0,0,49,50,3,6,3,0,50,51,5,7,
  	0,0,51,5,1,0,0,0,52,53,5,8,0,0,53,56,3,18,9,0,54,56,1,0,0,0,55,52,1,0,
  	0,0,55,54,1,0,0,0,56,7,1,0,0,0,57,60,3,12,6,0,58,60,1,0,0,0,59,57,1,0,
  	0,0,59,58,1,0,0,0,60,9,1,0,0,0,61,62,3,16,8,0,62,63,5,9,0,0,63,64,3,18,
  	9,0,64,11,1,0,0,0,65,66,6,6,-1,0,66,67,3,10,5,0,67,73,1,0,0,0,68,69,10,
  	1,0,0,69,70,5,10,0,0,70,72,3,10,5,0,71,68,1,0,0,0,72,75,1,0,0,0,73,71,
  	1,0,0,0,73,74,1,0,0,0,74,13,1,0,0,0,75,73,1,0,0,0,76,77,5,11,0,0,77,78,
  	3,16,8,0,78,79,5,2,0,0,79,81,3,12,6,0,80,82,5,10,0,0,81,80,1,0,0,0,81,
  	82,1,0,0,0,82,83,1,0,0,0,83,84,5,3,0,0,84,85,5,7,0,0,85,15,1,0,0,0,86,
  	87,5,23,0,0,87,17,1,0,0,0,88,91,3,22,11,0,89,91,3,16,8,0,90,88,1,0,0,
  	0,90,89,1,0,0,0,91,19,1,0,0,0,92,93,6,10,-1,0,93,96,3,18,9,0,94,96,1,
  	0,0,0,95,92,1,0,0,0,95,94,1,0,0,0,96,102,1,0,0,0,97,98,10,2,0,0,98,99,
  	5,10,0,0,99,101,3,18,9,0,100,97,1,0,0,0,101,104,1,0,0,0,102,100,1,0,0,
  	0,102,103,1,0,0,0,103,21,1,0,0,0,104,102,1,0,0,0,105,127,5,12,0,0,106,
  	127,5,13,0,0,107,127,5,14,0,0,108,127,5,15,0,0,109,127,5,16,0,0,110,127,
  	5,17,0,0,111,127,5,18,0,0,112,113,5,19,0,0,113,114,3,18,9,0,114,115,5,
  	20,0,0,115,127,1,0,0,0,116,117,5,21,0,0,117,118,3,18,9,0,118,119,5,10,
  	0,0,119,120,3,18,9,0,120,121,5,20,0,0,121,127,1,0,0,0,122,123,5,22,0,
  	0,123,124,3,20,10,0,124,125,5,20,0,0,125,127,1,0,0,0,126,105,1,0,0,0,
  	126,106,1,0,0,0,126,107,1,0,0,0,126,108,1,0,0,0,126,109,1,0,0,0,126,110,
  	1,0,0,0,126,111,1,0,0,0,126,112,1,0,0,0,126,116,1,0,0,0,126,122,1,0,0,
  	0,127,23,1,0,0,0,11,26,28,39,55,59,73,81,90,95,102,126
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
    setState(28);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TaskDefLangParser::T__0

    || _la == TaskDefLangParser::T__10) {
      setState(26);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case TaskDefLangParser::T__0: {
          setState(24);
          namespace_();
          break;
        }

        case TaskDefLangParser::T__10: {
          setState(25);
          structDef();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(30);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(31);
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
    setState(33);
    match(TaskDefLangParser::T__0);
    setState(34);
    id();
    setState(35);
    match(TaskDefLangParser::T__1);
    setState(39);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TaskDefLangParser::T__3) {
      setState(36);
      funcDef();
      setState(41);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(42);
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
    setState(44);
    match(TaskDefLangParser::T__3);
    setState(45);
    id();
    setState(46);
    match(TaskDefLangParser::T__4);
    setState(47);
    params();
    setState(48);
    match(TaskDefLangParser::T__5);
    setState(49);
    ret();
    setState(50);
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

TaskDefLangParser::TypeContext* TaskDefLangParser::RetContext::type() {
  return getRuleContext<TaskDefLangParser::TypeContext>(0);
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
    setState(55);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__7: {
        enterOuterAlt(_localctx, 1);
        setState(52);
        match(TaskDefLangParser::T__7);
        setState(53);
        type();
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
    setState(59);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::ID: {
        enterOuterAlt(_localctx, 1);
        setState(57);
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

TaskDefLangParser::TypeContext* TaskDefLangParser::NamedVarContext::type() {
  return getRuleContext<TaskDefLangParser::TypeContext>(0);
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
    setState(61);
    id();
    setState(62);
    match(TaskDefLangParser::T__8);
    setState(63);
    type();
   
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
    setState(66);
    namedVar();
    _ctx->stop = _input->LT(-1);
    setState(73);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        _localctx = _tracker.createInstance<NamedVarListContext>(parentContext, parentState);
        pushNewRecursionContext(_localctx, startState, RuleNamedVarList);
        setState(68);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(69);
        match(TaskDefLangParser::T__9);
        setState(70);
        namedVar(); 
      }
      setState(75);
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
    setState(76);
    match(TaskDefLangParser::T__10);
    setState(77);
    id();
    setState(78);
    match(TaskDefLangParser::T__1);
    setState(79);
    namedVarList(0);
    setState(81);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == TaskDefLangParser::T__9) {
      setState(80);
      match(TaskDefLangParser::T__9);
    }
    setState(83);
    match(TaskDefLangParser::T__2);
    setState(84);
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
    setState(86);
    match(TaskDefLangParser::ID);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TypeContext ------------------------------------------------------------------

TaskDefLangParser::TypeContext::TypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::BuiltinTypeContext* TaskDefLangParser::TypeContext::builtinType() {
  return getRuleContext<TaskDefLangParser::BuiltinTypeContext>(0);
}

TaskDefLangParser::IdContext* TaskDefLangParser::TypeContext::id() {
  return getRuleContext<TaskDefLangParser::IdContext>(0);
}


size_t TaskDefLangParser::TypeContext::getRuleIndex() const {
  return TaskDefLangParser::RuleType;
}


std::any TaskDefLangParser::TypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitType(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::TypeContext* TaskDefLangParser::type() {
  TypeContext *_localctx = _tracker.createInstance<TypeContext>(_ctx, getState());
  enterRule(_localctx, 18, TaskDefLangParser::RuleType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(90);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__11:
      case TaskDefLangParser::T__12:
      case TaskDefLangParser::T__13:
      case TaskDefLangParser::T__14:
      case TaskDefLangParser::T__15:
      case TaskDefLangParser::T__16:
      case TaskDefLangParser::T__17:
      case TaskDefLangParser::T__18:
      case TaskDefLangParser::T__20:
      case TaskDefLangParser::T__21: {
        enterOuterAlt(_localctx, 1);
        setState(88);
        builtinType();
        break;
      }

      case TaskDefLangParser::ID: {
        enterOuterAlt(_localctx, 2);
        setState(89);
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

//----------------- TypeListContext ------------------------------------------------------------------

TaskDefLangParser::TypeListContext::TypeListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

TaskDefLangParser::TypeContext* TaskDefLangParser::TypeListContext::type() {
  return getRuleContext<TaskDefLangParser::TypeContext>(0);
}

TaskDefLangParser::TypeListContext* TaskDefLangParser::TypeListContext::typeList() {
  return getRuleContext<TaskDefLangParser::TypeListContext>(0);
}


size_t TaskDefLangParser::TypeListContext::getRuleIndex() const {
  return TaskDefLangParser::RuleTypeList;
}


std::any TaskDefLangParser::TypeListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitTypeList(this);
  else
    return visitor->visitChildren(this);
}


TaskDefLangParser::TypeListContext* TaskDefLangParser::typeList() {
   return typeList(0);
}

TaskDefLangParser::TypeListContext* TaskDefLangParser::typeList(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  TaskDefLangParser::TypeListContext *_localctx = _tracker.createInstance<TypeListContext>(_ctx, parentState);
  TaskDefLangParser::TypeListContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 20;
  enterRecursionRule(_localctx, 20, TaskDefLangParser::RuleTypeList, precedence);

    

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
    setState(95);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
    case 1: {
      setState(93);
      type();
      break;
    }

    case 2: {
      break;
    }

    default:
      break;
    }
    _ctx->stop = _input->LT(-1);
    setState(102);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        _localctx = _tracker.createInstance<TypeListContext>(parentContext, parentState);
        pushNewRecursionContext(_localctx, startState, RuleTypeList);
        setState(97);

        if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
        setState(98);
        match(TaskDefLangParser::T__9);
        setState(99);
        type(); 
      }
      setState(104);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx);
    }
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

std::vector<TaskDefLangParser::TypeContext *> TaskDefLangParser::BuiltinTypeContext::type() {
  return getRuleContexts<TaskDefLangParser::TypeContext>();
}

TaskDefLangParser::TypeContext* TaskDefLangParser::BuiltinTypeContext::type(size_t i) {
  return getRuleContext<TaskDefLangParser::TypeContext>(i);
}

TaskDefLangParser::TypeListContext* TaskDefLangParser::BuiltinTypeContext::typeList() {
  return getRuleContext<TaskDefLangParser::TypeListContext>(0);
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
  enterRule(_localctx, 22, TaskDefLangParser::RuleBuiltinType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(126);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__11: {
        enterOuterAlt(_localctx, 1);
        setState(105);
        match(TaskDefLangParser::T__11);
        break;
      }

      case TaskDefLangParser::T__12: {
        enterOuterAlt(_localctx, 2);
        setState(106);
        match(TaskDefLangParser::T__12);
        break;
      }

      case TaskDefLangParser::T__13: {
        enterOuterAlt(_localctx, 3);
        setState(107);
        match(TaskDefLangParser::T__13);
        break;
      }

      case TaskDefLangParser::T__14: {
        enterOuterAlt(_localctx, 4);
        setState(108);
        match(TaskDefLangParser::T__14);
        break;
      }

      case TaskDefLangParser::T__15: {
        enterOuterAlt(_localctx, 5);
        setState(109);
        match(TaskDefLangParser::T__15);
        break;
      }

      case TaskDefLangParser::T__16: {
        enterOuterAlt(_localctx, 6);
        setState(110);
        match(TaskDefLangParser::T__16);
        break;
      }

      case TaskDefLangParser::T__17: {
        enterOuterAlt(_localctx, 7);
        setState(111);
        match(TaskDefLangParser::T__17);
        break;
      }

      case TaskDefLangParser::T__18: {
        enterOuterAlt(_localctx, 8);
        setState(112);
        match(TaskDefLangParser::T__18);
        setState(113);
        type();
        setState(114);
        match(TaskDefLangParser::T__19);
        break;
      }

      case TaskDefLangParser::T__20: {
        enterOuterAlt(_localctx, 9);
        setState(116);
        match(TaskDefLangParser::T__20);
        setState(117);
        type();
        setState(118);
        match(TaskDefLangParser::T__9);
        setState(119);
        type();
        setState(120);
        match(TaskDefLangParser::T__19);
        break;
      }

      case TaskDefLangParser::T__21: {
        enterOuterAlt(_localctx, 10);
        setState(122);
        match(TaskDefLangParser::T__21);
        setState(123);
        typeList(0);
        setState(124);
        match(TaskDefLangParser::T__19);
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
    case 10: return typeListSempred(antlrcpp::downCast<TypeListContext *>(context), predicateIndex);

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

bool TaskDefLangParser::typeListSempred(TypeListContext *_localctx, size_t predicateIndex) {
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
