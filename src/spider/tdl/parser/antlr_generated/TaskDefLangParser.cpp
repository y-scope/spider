
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
      "start"
    },
    std::vector<std::string>{
    },
    std::vector<std::string>{
      "", "SPACE"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,1,5,2,0,7,0,1,0,1,0,1,0,0,0,1,0,0,0,3,0,2,1,0,0,0,2,3,5,0,0,1,3,1,
  	1,0,0,0,0
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


//----------------- StartContext ------------------------------------------------------------------

TaskDefLangParser::StartContext::StartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* TaskDefLangParser::StartContext::EOF() {
  return getToken(TaskDefLangParser::EOF, 0);
}


size_t TaskDefLangParser::StartContext::getRuleIndex() const {
  return TaskDefLangParser::RuleStart;
}


std::any TaskDefLangParser::StartContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<TaskDefLangVisitor*>(visitor))
    return parserVisitor->visitStart(this);
  else
    return visitor->visitChildren(this);
}

TaskDefLangParser::StartContext* TaskDefLangParser::start() {
  StartContext *_localctx = _tracker.createInstance<StartContext>(_ctx, getState());
  enterRule(_localctx, 0, TaskDefLangParser::RuleStart);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2);
    match(TaskDefLangParser::EOF);
   
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
