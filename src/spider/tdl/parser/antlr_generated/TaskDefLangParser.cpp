
#include <memory>
#include <utility>
#include <vector>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/parser/Exception.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>


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
      "translationUnit", "namespace", "funcDefs", "funcDef", "ret", "params", 
      "namedVar", "namedVarList", "structDef", "id", "varType", "retType", 
      "varTypeList", "listType", "mapType", "tupleType", "builtinType"
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
  	4,1,26,186,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,7,
  	14,2,15,7,15,2,16,7,16,1,0,1,0,1,0,1,0,1,0,1,0,5,0,41,8,0,10,0,12,0,44,
  	9,0,1,0,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,2,1,2,4,2,58,8,2,11,2,12,
  	2,59,1,3,1,3,1,3,1,3,1,3,1,3,1,3,1,3,1,3,1,4,1,4,1,4,1,4,1,4,3,4,76,8,
  	4,1,5,1,5,1,5,1,5,3,5,82,8,5,1,6,1,6,1,6,1,6,1,6,1,7,1,7,1,7,1,7,1,7,
  	1,7,5,7,95,8,7,10,7,12,7,98,9,7,1,8,1,8,1,8,1,8,1,8,3,8,105,8,8,1,8,1,
  	8,1,8,1,8,1,9,1,9,1,9,1,10,1,10,1,10,1,10,1,10,1,10,3,10,120,8,10,1,11,
  	1,11,1,11,1,11,1,11,1,11,3,11,128,8,11,1,12,1,12,1,12,1,12,1,12,1,12,
  	5,12,136,8,12,10,12,12,12,139,9,12,1,12,3,12,142,8,12,1,13,1,13,1,13,
  	1,13,1,13,1,13,1,14,1,14,1,14,1,14,1,14,1,14,1,14,1,14,1,15,1,15,1,15,
  	1,15,1,15,1,15,1,16,1,16,1,16,1,16,1,16,1,16,1,16,1,16,1,16,1,16,1,16,
  	1,16,1,16,1,16,1,16,1,16,1,16,1,16,1,16,1,16,3,16,184,8,16,1,16,0,0,17,
  	0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,0,0,187,0,42,1,0,0,0,2,
  	47,1,0,0,0,4,57,1,0,0,0,6,61,1,0,0,0,8,75,1,0,0,0,10,81,1,0,0,0,12,83,
  	1,0,0,0,14,88,1,0,0,0,16,99,1,0,0,0,18,110,1,0,0,0,20,119,1,0,0,0,22,
  	127,1,0,0,0,24,141,1,0,0,0,26,143,1,0,0,0,28,149,1,0,0,0,30,157,1,0,0,
  	0,32,183,1,0,0,0,34,35,3,2,1,0,35,36,6,0,-1,0,36,41,1,0,0,0,37,38,3,16,
  	8,0,38,39,6,0,-1,0,39,41,1,0,0,0,40,34,1,0,0,0,40,37,1,0,0,0,41,44,1,
  	0,0,0,42,40,1,0,0,0,42,43,1,0,0,0,43,45,1,0,0,0,44,42,1,0,0,0,45,46,5,
  	0,0,1,46,1,1,0,0,0,47,48,5,1,0,0,48,49,3,18,9,0,49,50,5,2,0,0,50,51,3,
  	4,2,0,51,52,5,3,0,0,52,53,6,1,-1,0,53,3,1,0,0,0,54,55,3,6,3,0,55,56,6,
  	2,-1,0,56,58,1,0,0,0,57,54,1,0,0,0,58,59,1,0,0,0,59,57,1,0,0,0,59,60,
  	1,0,0,0,60,5,1,0,0,0,61,62,5,4,0,0,62,63,3,18,9,0,63,64,5,5,0,0,64,65,
  	3,10,5,0,65,66,5,6,0,0,66,67,3,8,4,0,67,68,5,7,0,0,68,69,6,3,-1,0,69,
  	7,1,0,0,0,70,71,5,8,0,0,71,72,3,22,11,0,72,73,6,4,-1,0,73,76,1,0,0,0,
  	74,76,6,4,-1,0,75,70,1,0,0,0,75,74,1,0,0,0,76,9,1,0,0,0,77,78,3,14,7,
  	0,78,79,6,5,-1,0,79,82,1,0,0,0,80,82,6,5,-1,0,81,77,1,0,0,0,81,80,1,0,
  	0,0,82,11,1,0,0,0,83,84,3,18,9,0,84,85,5,9,0,0,85,86,3,20,10,0,86,87,
  	6,6,-1,0,87,13,1,0,0,0,88,89,3,12,6,0,89,96,6,7,-1,0,90,91,5,10,0,0,91,
  	92,3,12,6,0,92,93,6,7,-1,0,93,95,1,0,0,0,94,90,1,0,0,0,95,98,1,0,0,0,
  	96,94,1,0,0,0,96,97,1,0,0,0,97,15,1,0,0,0,98,96,1,0,0,0,99,100,5,11,0,
  	0,100,101,3,18,9,0,101,102,5,2,0,0,102,104,3,14,7,0,103,105,5,10,0,0,
  	104,103,1,0,0,0,104,105,1,0,0,0,105,106,1,0,0,0,106,107,5,3,0,0,107,108,
  	5,7,0,0,108,109,6,8,-1,0,109,17,1,0,0,0,110,111,5,24,0,0,111,112,6,9,
  	-1,0,112,19,1,0,0,0,113,114,3,32,16,0,114,115,6,10,-1,0,115,120,1,0,0,
  	0,116,117,3,18,9,0,117,118,6,10,-1,0,118,120,1,0,0,0,119,113,1,0,0,0,
  	119,116,1,0,0,0,120,21,1,0,0,0,121,122,3,20,10,0,122,123,6,11,-1,0,123,
  	128,1,0,0,0,124,125,3,30,15,0,125,126,6,11,-1,0,126,128,1,0,0,0,127,121,
  	1,0,0,0,127,124,1,0,0,0,128,23,1,0,0,0,129,130,3,20,10,0,130,137,6,12,
  	-1,0,131,132,5,10,0,0,132,133,3,20,10,0,133,134,6,12,-1,0,134,136,1,0,
  	0,0,135,131,1,0,0,0,136,139,1,0,0,0,137,135,1,0,0,0,137,138,1,0,0,0,138,
  	142,1,0,0,0,139,137,1,0,0,0,140,142,6,12,-1,0,141,129,1,0,0,0,141,140,
  	1,0,0,0,142,25,1,0,0,0,143,144,5,12,0,0,144,145,5,13,0,0,145,146,3,20,
  	10,0,146,147,5,14,0,0,147,148,6,13,-1,0,148,27,1,0,0,0,149,150,5,15,0,
  	0,150,151,5,13,0,0,151,152,3,20,10,0,152,153,5,10,0,0,153,154,3,20,10,
  	0,154,155,5,14,0,0,155,156,6,14,-1,0,156,29,1,0,0,0,157,158,5,16,0,0,
  	158,159,5,13,0,0,159,160,3,24,12,0,160,161,5,14,0,0,161,162,6,15,-1,0,
  	162,31,1,0,0,0,163,164,5,17,0,0,164,184,6,16,-1,0,165,166,5,18,0,0,166,
  	184,6,16,-1,0,167,168,5,19,0,0,168,184,6,16,-1,0,169,170,5,20,0,0,170,
  	184,6,16,-1,0,171,172,5,21,0,0,172,184,6,16,-1,0,173,174,5,22,0,0,174,
  	184,6,16,-1,0,175,176,5,23,0,0,176,184,6,16,-1,0,177,178,3,26,13,0,178,
  	179,6,16,-1,0,179,184,1,0,0,0,180,181,3,28,14,0,181,182,6,16,-1,0,182,
  	184,1,0,0,0,183,163,1,0,0,0,183,165,1,0,0,0,183,167,1,0,0,0,183,169,1,
  	0,0,0,183,171,1,0,0,0,183,173,1,0,0,0,183,175,1,0,0,0,183,177,1,0,0,0,
  	183,180,1,0,0,0,184,33,1,0,0,0,12,40,42,59,75,81,96,104,119,127,137,141,
  	183
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

      antlrcpp::downCast<TranslationUnitContext *>(_localctx)->tu =  spider::tdl::parser::ast::TranslationUnit::create({
              _localctx->start->getLine(),
              _localctx->start->getCharPositionInLine()
      });

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
    setState(42);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == TaskDefLangParser::T__0

    || _la == TaskDefLangParser::T__10) {
      setState(40);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case TaskDefLangParser::T__0: {
          setState(34);
          antlrcpp::downCast<TranslationUnitContext *>(_localctx)->namespaceContext = namespace_();

              auto const ns_loc{antlrcpp::downCast<TranslationUnitContext *>(_localctx)->namespaceContext->retval->get_source_location()};
              spider::tdl::parser::Exception::throw_tryv(
                  _localctx->tu->add_namespace(std::move(antlrcpp::downCast<TranslationUnitContext *>(_localctx)->namespaceContext->retval)),
                  ns_loc
              );

          break;
        }

        case TaskDefLangParser::T__10: {
          setState(37);
          antlrcpp::downCast<TranslationUnitContext *>(_localctx)->structDefContext = structDef();

              auto const struct_loc{antlrcpp::downCast<TranslationUnitContext *>(_localctx)->structDefContext->retval->get_source_location()};
              spider::tdl::parser::Exception::throw_tryv(
                  _localctx->tu->add_struct_spec(std::move(antlrcpp::downCast<TranslationUnitContext *>(_localctx)->structDefContext->retval)),
                  struct_loc
              );

          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(44);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(45);
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

TaskDefLangParser::FuncDefsContext* TaskDefLangParser::NamespaceContext::funcDefs() {
  return getRuleContext<TaskDefLangParser::FuncDefsContext>(0);
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

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(47);
    match(TaskDefLangParser::T__0);
    setState(48);
    antlrcpp::downCast<NamespaceContext *>(_localctx)->idContext = id();
    setState(49);
    match(TaskDefLangParser::T__1);
    setState(50);
    antlrcpp::downCast<NamespaceContext *>(_localctx)->funcDefsContext = funcDefs();
    setState(51);
    match(TaskDefLangParser::T__2);

        SourceLocation const loc{
            _localctx->start->getLine(),
            _localctx->start->getCharPositionInLine()
        };
        antlrcpp::downCast<NamespaceContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
            spider::tdl::parser::ast::Namespace::create(
                std::move(antlrcpp::downCast<NamespaceContext *>(_localctx)->idContext->retval),
                std::move(antlrcpp::downCast<NamespaceContext *>(_localctx)->funcDefsContext->retval),
                loc
            ),
            loc
        );

   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FuncDefsContext ------------------------------------------------------------------

TaskDefLangParser::FuncDefsContext::FuncDefsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<TaskDefLangParser::FuncDefContext *> TaskDefLangParser::FuncDefsContext::funcDef() {
  return getRuleContexts<TaskDefLangParser::FuncDefContext>();
}

TaskDefLangParser::FuncDefContext* TaskDefLangParser::FuncDefsContext::funcDef(size_t i) {
  return getRuleContext<TaskDefLangParser::FuncDefContext>(i);
}


size_t TaskDefLangParser::FuncDefsContext::getRuleIndex() const {
  return TaskDefLangParser::RuleFuncDefs;
}


std::any TaskDefLangParser::FuncDefsContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitFuncDefs(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::FuncDefsContext* TaskDefLangParser::funcDefs() {
  FuncDefsContext *_localctx = _tracker.createInstance<FuncDefsContext>(_ctx, getState());
  enterRule(_localctx, 4, TaskDefLangParser::RuleFuncDefs);

      _localctx->retval.clear();

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
    setState(57); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(54);
      antlrcpp::downCast<FuncDefsContext *>(_localctx)->funcDefContext = funcDef();

          _localctx->retval.emplace_back(std::move(antlrcpp::downCast<FuncDefsContext *>(_localctx)->funcDefContext->retval));

      setState(59); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (_la == TaskDefLangParser::T__3);
   
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
  enterRule(_localctx, 6, TaskDefLangParser::RuleFuncDef);

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
    match(TaskDefLangParser::T__3);
    setState(62);
    antlrcpp::downCast<FuncDefContext *>(_localctx)->idContext = id();
    setState(63);
    match(TaskDefLangParser::T__4);
    setState(64);
    antlrcpp::downCast<FuncDefContext *>(_localctx)->paramsContext = params();
    setState(65);
    match(TaskDefLangParser::T__5);
    setState(66);
    antlrcpp::downCast<FuncDefContext *>(_localctx)->retContext = ret();
    setState(67);
    match(TaskDefLangParser::T__6);

        SourceLocation const loc{
            _localctx->start->getLine(),
            _localctx->start->getCharPositionInLine()
        };
        antlrcpp::downCast<FuncDefContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
            spider::tdl::parser::ast::Function::create(
                std::move(antlrcpp::downCast<FuncDefContext *>(_localctx)->idContext->retval),
                std::move(antlrcpp::downCast<FuncDefContext *>(_localctx)->retContext->retval),
                std::move(antlrcpp::downCast<FuncDefContext *>(_localctx)->paramsContext->retval),
                loc
            ),
            loc
        );

   
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
  enterRule(_localctx, 8, TaskDefLangParser::RuleRet);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(75);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__7: {
        enterOuterAlt(_localctx, 1);
        setState(70);
        match(TaskDefLangParser::T__7);
        setState(71);
        antlrcpp::downCast<RetContext *>(_localctx)->retTypeContext = retType();

            antlrcpp::downCast<RetContext *>(_localctx)->retval =  std::move(antlrcpp::downCast<RetContext *>(_localctx)->retTypeContext->retval);

        break;
      }

      case TaskDefLangParser::T__6: {
        enterOuterAlt(_localctx, 2);

            antlrcpp::downCast<RetContext *>(_localctx)->retval =  nullptr;

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
  enterRule(_localctx, 10, TaskDefLangParser::RuleParams);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(81);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::ID: {
        enterOuterAlt(_localctx, 1);
        setState(77);
        antlrcpp::downCast<ParamsContext *>(_localctx)->namedVarListContext = namedVarList();

            antlrcpp::downCast<ParamsContext *>(_localctx)->retval =  std::move(antlrcpp::downCast<ParamsContext *>(_localctx)->namedVarListContext->retval);

        break;
      }

      case TaskDefLangParser::T__5: {
        enterOuterAlt(_localctx, 2);

            _localctx->retval.clear();

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
  enterRule(_localctx, 12, TaskDefLangParser::RuleNamedVar);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(83);
    antlrcpp::downCast<NamedVarContext *>(_localctx)->idContext = id();
    setState(84);
    match(TaskDefLangParser::T__8);
    setState(85);
    antlrcpp::downCast<NamedVarContext *>(_localctx)->varTypeContext = varType();

        SourceLocation const loc{
            _localctx->start->getLine(),
            _localctx->start->getCharPositionInLine()
        };
        antlrcpp::downCast<NamedVarContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
            spider::tdl::parser::ast::NamedVar::create(
                std::move(antlrcpp::downCast<NamedVarContext *>(_localctx)->idContext->retval),
                std::move(antlrcpp::downCast<NamedVarContext *>(_localctx)->varTypeContext->retval),
                loc
            ),
            loc
        );

   
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

std::vector<TaskDefLangParser::NamedVarContext *> TaskDefLangParser::NamedVarListContext::namedVar() {
  return getRuleContexts<TaskDefLangParser::NamedVarContext>();
}

TaskDefLangParser::NamedVarContext* TaskDefLangParser::NamedVarListContext::namedVar(size_t i) {
  return getRuleContext<TaskDefLangParser::NamedVarContext>(i);
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
  NamedVarListContext *_localctx = _tracker.createInstance<NamedVarListContext>(_ctx, getState());
  enterRule(_localctx, 14, TaskDefLangParser::RuleNamedVarList);

      _localctx->retval.clear();


#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(88);
    antlrcpp::downCast<NamedVarListContext *>(_localctx)->first_named_var = namedVar();

        _localctx->retval.emplace_back(std::move(antlrcpp::downCast<NamedVarListContext *>(_localctx)->first_named_var->retval));

    setState(96);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(90);
        match(TaskDefLangParser::T__9);
        setState(91);
        antlrcpp::downCast<NamedVarListContext *>(_localctx)->subsequent_named_var = namedVar();

            _localctx->retval.emplace_back(std::move(antlrcpp::downCast<NamedVarListContext *>(_localctx)->subsequent_named_var->retval));
     
      }
      setState(98);
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
  enterRule(_localctx, 16, TaskDefLangParser::RuleStructDef);
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
    setState(99);
    match(TaskDefLangParser::T__10);
    setState(100);
    antlrcpp::downCast<StructDefContext *>(_localctx)->idContext = id();
    setState(101);
    match(TaskDefLangParser::T__1);
    setState(102);
    antlrcpp::downCast<StructDefContext *>(_localctx)->namedVarListContext = namedVarList();
    setState(104);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == TaskDefLangParser::T__9) {
      setState(103);
      match(TaskDefLangParser::T__9);
    }
    setState(106);
    match(TaskDefLangParser::T__2);
    setState(107);
    match(TaskDefLangParser::T__6);

        SourceLocation const loc{
            _localctx->start->getLine(),
            _localctx->start->getCharPositionInLine()
        };
        antlrcpp::downCast<StructDefContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
            spider::tdl::parser::ast::StructSpec::create(
                std::move(antlrcpp::downCast<StructDefContext *>(_localctx)->idContext->retval),
                std::move(antlrcpp::downCast<StructDefContext *>(_localctx)->namedVarListContext->retval),
                loc
            ),
            loc
        );

   
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
  enterRule(_localctx, 18, TaskDefLangParser::RuleId);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(110);
    antlrcpp::downCast<IdContext *>(_localctx)->idToken = match(TaskDefLangParser::ID);

        antlrcpp::downCast<IdContext *>(_localctx)->retval =  spider::tdl::parser::ast::Identifier::create(
            (antlrcpp::downCast<IdContext *>(_localctx)->idToken != nullptr ? antlrcpp::downCast<IdContext *>(_localctx)->idToken->getText() : ""),
            spider::tdl::parser::SourceLocation{
                    _localctx->start->getLine(),
                    _localctx->start->getCharPositionInLine()
            }
        );

   
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
  enterRule(_localctx, 20, TaskDefLangParser::RuleVarType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(119);
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
        setState(113);
        antlrcpp::downCast<VarTypeContext *>(_localctx)->builtinTypeContext = builtinType();

            antlrcpp::downCast<VarTypeContext *>(_localctx)->retval =  std::move(antlrcpp::downCast<VarTypeContext *>(_localctx)->builtinTypeContext->retval);

        break;
      }

      case TaskDefLangParser::ID: {
        enterOuterAlt(_localctx, 2);
        setState(116);
        antlrcpp::downCast<VarTypeContext *>(_localctx)->idContext = id();

            SourceLocation const loc{
                _localctx->start->getLine(),
                _localctx->start->getCharPositionInLine()
            };
            antlrcpp::downCast<VarTypeContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
                spider::tdl::parser::ast::Struct::create(std::move(antlrcpp::downCast<VarTypeContext *>(_localctx)->idContext->retval), loc),
                loc
            );

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
  enterRule(_localctx, 22, TaskDefLangParser::RuleRetType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(127);
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
        setState(121);
        antlrcpp::downCast<RetTypeContext *>(_localctx)->varTypeContext = varType();

            antlrcpp::downCast<RetTypeContext *>(_localctx)->retval =  std::move(antlrcpp::downCast<RetTypeContext *>(_localctx)->varTypeContext->retval);

        break;
      }

      case TaskDefLangParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(124);
        antlrcpp::downCast<RetTypeContext *>(_localctx)->tupleTypeContext = tupleType();

            antlrcpp::downCast<RetTypeContext *>(_localctx)->retval =  std::move(antlrcpp::downCast<RetTypeContext *>(_localctx)->tupleTypeContext->retval);

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

std::vector<TaskDefLangParser::VarTypeContext *> TaskDefLangParser::VarTypeListContext::varType() {
  return getRuleContexts<TaskDefLangParser::VarTypeContext>();
}

TaskDefLangParser::VarTypeContext* TaskDefLangParser::VarTypeListContext::varType(size_t i) {
  return getRuleContext<TaskDefLangParser::VarTypeContext>(i);
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
  VarTypeListContext *_localctx = _tracker.createInstance<VarTypeListContext>(_ctx, getState());
  enterRule(_localctx, 24, TaskDefLangParser::RuleVarTypeList);

      _localctx->retval.clear();

  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(141);
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
        setState(129);
        antlrcpp::downCast<VarTypeListContext *>(_localctx)->first_var_type = varType();

            _localctx->retval.emplace_back(std::move(antlrcpp::downCast<VarTypeListContext *>(_localctx)->first_var_type->retval));

        setState(137);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == TaskDefLangParser::T__9) {
          setState(131);
          match(TaskDefLangParser::T__9);
          setState(132);
          antlrcpp::downCast<VarTypeListContext *>(_localctx)->subsequent_var_type = varType();

              _localctx->retval.emplace_back(std::move(antlrcpp::downCast<VarTypeListContext *>(_localctx)->subsequent_var_type->retval));

          setState(139);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      case TaskDefLangParser::T__13: {
        enterOuterAlt(_localctx, 2);

            _localctx->retval.clear();

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
  enterRule(_localctx, 26, TaskDefLangParser::RuleListType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(143);
    match(TaskDefLangParser::T__11);
    setState(144);
    match(TaskDefLangParser::T__12);
    setState(145);
    antlrcpp::downCast<ListTypeContext *>(_localctx)->varTypeContext = varType();
    setState(146);
    match(TaskDefLangParser::T__13);

        SourceLocation const loc{
            _localctx->start->getLine(),
            _localctx->start->getCharPositionInLine()
        };
        antlrcpp::downCast<ListTypeContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
            spider::tdl::parser::ast::List::create(std::move(antlrcpp::downCast<ListTypeContext *>(_localctx)->varTypeContext->retval), loc),
            loc
        );

   
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
  enterRule(_localctx, 28, TaskDefLangParser::RuleMapType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(149);
    match(TaskDefLangParser::T__14);
    setState(150);
    match(TaskDefLangParser::T__12);
    setState(151);
    antlrcpp::downCast<MapTypeContext *>(_localctx)->key_type = varType();
    setState(152);
    match(TaskDefLangParser::T__9);
    setState(153);
    antlrcpp::downCast<MapTypeContext *>(_localctx)->val_type = varType();
    setState(154);
    match(TaskDefLangParser::T__13);

        SourceLocation const loc{
            _localctx->start->getLine(),
            _localctx->start->getCharPositionInLine()
        };
        antlrcpp::downCast<MapTypeContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
            spider::tdl::parser::ast::Map::create(
                    std::move(antlrcpp::downCast<MapTypeContext *>(_localctx)->key_type->retval),
                    std::move(antlrcpp::downCast<MapTypeContext *>(_localctx)->val_type->retval),
                    loc
            ),
            loc
        );

   
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
  enterRule(_localctx, 30, TaskDefLangParser::RuleTupleType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(157);
    match(TaskDefLangParser::T__15);
    setState(158);
    match(TaskDefLangParser::T__12);
    setState(159);
    antlrcpp::downCast<TupleTypeContext *>(_localctx)->varTypeListContext = varTypeList();
    setState(160);
    match(TaskDefLangParser::T__13);

        SourceLocation const loc{
            _localctx->start->getLine(),
            _localctx->start->getCharPositionInLine()
        };
        antlrcpp::downCast<TupleTypeContext *>(_localctx)->retval =  spider::tdl::parser::Exception::throw_tryx(
            spider::tdl::parser::ast::Tuple::create(std::move(antlrcpp::downCast<TupleTypeContext *>(_localctx)->varTypeListContext->retval), loc),
            loc
        );

   
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
  enterRule(_localctx, 32, TaskDefLangParser::RuleBuiltinType);

      SourceLocation const loc{
          _localctx->start->getLine(),
          _localctx->start->getCharPositionInLine()
      };


#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(183);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case TaskDefLangParser::T__16: {
        enterOuterAlt(_localctx, 1);
        setState(163);
        match(TaskDefLangParser::T__16);

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int8, loc);

        break;
      }

      case TaskDefLangParser::T__17: {
        enterOuterAlt(_localctx, 2);
        setState(165);
        match(TaskDefLangParser::T__17);

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int16, loc);

        break;
      }

      case TaskDefLangParser::T__18: {
        enterOuterAlt(_localctx, 3);
        setState(167);
        match(TaskDefLangParser::T__18);

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int32, loc);

        break;
      }

      case TaskDefLangParser::T__19: {
        enterOuterAlt(_localctx, 4);
        setState(169);
        match(TaskDefLangParser::T__19);

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  spider::tdl::parser::ast::Int::create(spider::tdl::parser::ast::IntSpec::Int64, loc);

        break;
      }

      case TaskDefLangParser::T__20: {
        enterOuterAlt(_localctx, 5);
        setState(171);
        match(TaskDefLangParser::T__20);

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  spider::tdl::parser::ast::Float::create(
                    spider::tdl::parser::ast::FloatSpec::Float,
                    loc
            );

        break;
      }

      case TaskDefLangParser::T__21: {
        enterOuterAlt(_localctx, 6);
        setState(173);
        match(TaskDefLangParser::T__21);

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  spider::tdl::parser::ast::Float::create(
                    spider::tdl::parser::ast::FloatSpec::Double,
                    loc
            );

        break;
      }

      case TaskDefLangParser::T__22: {
        enterOuterAlt(_localctx, 7);
        setState(175);
        match(TaskDefLangParser::T__22);

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  spider::tdl::parser::ast::Bool::create(loc);

        break;
      }

      case TaskDefLangParser::T__11: {
        enterOuterAlt(_localctx, 8);
        setState(177);
        antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->listTypeContext = listType();

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  std::move(antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->listTypeContext->retval);

        break;
      }

      case TaskDefLangParser::T__14: {
        enterOuterAlt(_localctx, 9);
        setState(180);
        antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->mapTypeContext = mapType();

            antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->retval =  std::move(antlrcpp::downCast<BuiltinTypeContext *>(_localctx)->mapTypeContext->retval);

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

void TaskDefLangParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  taskdeflangParserInitialize();
#else
  ::antlr4::internal::call_once(taskdeflangParserOnceFlag, taskdeflangParserInitialize);
#endif
}
