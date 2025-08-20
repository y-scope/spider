
// Generated from tdl/parser/TaskDefLang.g4 by ANTLR 4.13.2


#include "TaskDefLangLexer.h"


using namespace antlr4;

using namespace spider::tdl::parser::antlr_generated;


using namespace antlr4;

namespace {

struct TaskDefLangLexerStaticData final {
  TaskDefLangLexerStaticData(std::vector<std::string> ruleNames,
                          std::vector<std::string> channelNames,
                          std::vector<std::string> modeNames,
                          std::vector<std::string> literalNames,
                          std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), channelNames(std::move(channelNames)),
        modeNames(std::move(modeNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  TaskDefLangLexerStaticData(const TaskDefLangLexerStaticData&) = delete;
  TaskDefLangLexerStaticData(TaskDefLangLexerStaticData&&) = delete;
  TaskDefLangLexerStaticData& operator=(const TaskDefLangLexerStaticData&) = delete;
  TaskDefLangLexerStaticData& operator=(TaskDefLangLexerStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> channelNames;
  const std::vector<std::string> modeNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag taskdeflanglexerLexerOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
std::unique_ptr<TaskDefLangLexerStaticData> taskdeflanglexerLexerStaticData = nullptr;

void taskdeflanglexerLexerInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (taskdeflanglexerLexerStaticData != nullptr) {
    return;
  }
#else
  assert(taskdeflanglexerLexerStaticData == nullptr);
#endif
  auto staticData = std::make_unique<TaskDefLangLexerStaticData>(
    std::vector<std::string>{
      "SPACE"
    },
    std::vector<std::string>{
      "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    },
    std::vector<std::string>{
      "DEFAULT_MODE"
    },
    std::vector<std::string>{
    },
    std::vector<std::string>{
      "", "SPACE"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,0,1,7,6,-1,2,0,7,0,1,0,1,0,1,0,1,0,0,0,1,1,1,1,0,1,3,0,9,10,13,13,32,
  	32,6,0,1,1,0,0,0,1,3,1,0,0,0,3,4,7,0,0,0,4,5,1,0,0,0,5,6,6,0,0,0,6,2,
  	1,0,0,0,1,0,1,6,0,0
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  taskdeflanglexerLexerStaticData = std::move(staticData);
}

}

TaskDefLangLexer::TaskDefLangLexer(CharStream *input) : Lexer(input) {
  TaskDefLangLexer::initialize();
  _interpreter = new atn::LexerATNSimulator(this, *taskdeflanglexerLexerStaticData->atn, taskdeflanglexerLexerStaticData->decisionToDFA, taskdeflanglexerLexerStaticData->sharedContextCache);
}

TaskDefLangLexer::~TaskDefLangLexer() {
  delete _interpreter;
}

std::string TaskDefLangLexer::getGrammarFileName() const {
  return "TaskDefLang.g4";
}

const std::vector<std::string>& TaskDefLangLexer::getRuleNames() const {
  return taskdeflanglexerLexerStaticData->ruleNames;
}

const std::vector<std::string>& TaskDefLangLexer::getChannelNames() const {
  return taskdeflanglexerLexerStaticData->channelNames;
}

const std::vector<std::string>& TaskDefLangLexer::getModeNames() const {
  return taskdeflanglexerLexerStaticData->modeNames;
}

const dfa::Vocabulary& TaskDefLangLexer::getVocabulary() const {
  return taskdeflanglexerLexerStaticData->vocabulary;
}

antlr4::atn::SerializedATNView TaskDefLangLexer::getSerializedATN() const {
  return taskdeflanglexerLexerStaticData->serializedATN;
}

const atn::ATN& TaskDefLangLexer::getATN() const {
  return *taskdeflanglexerLexerStaticData->atn;
}




void TaskDefLangLexer::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  taskdeflanglexerLexerInitialize();
#else
  ::antlr4::internal::call_once(taskdeflanglexerLexerOnceFlag, taskdeflanglexerLexerInitialize);
#endif
}
