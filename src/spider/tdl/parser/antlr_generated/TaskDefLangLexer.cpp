
#include <memory>
#include <utility>
#include <vector>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/parser/Exception.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>


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
      "T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
      "T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
      "T__17", "T__18", "T__19", "T__20", "T__21", "T__22", "ID", "SPACE", 
      "COMMENT"
    },
    std::vector<std::string>{
      "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    },
    std::vector<std::string>{
      "DEFAULT_MODE"
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
  	4,0,26,172,6,-1,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
  	6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,
  	7,14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,
  	7,21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,1,0,1,0,1,0,1,0,1,0,1,0,
  	1,0,1,0,1,0,1,0,1,1,1,1,1,2,1,2,1,3,1,3,1,3,1,4,1,4,1,5,1,5,1,6,1,6,1,
  	7,1,7,1,7,1,8,1,8,1,9,1,9,1,10,1,10,1,10,1,10,1,10,1,10,1,10,1,11,1,11,
  	1,11,1,11,1,11,1,12,1,12,1,13,1,13,1,14,1,14,1,14,1,14,1,15,1,15,1,15,
  	1,15,1,15,1,15,1,16,1,16,1,16,1,16,1,16,1,17,1,17,1,17,1,17,1,17,1,17,
  	1,18,1,18,1,18,1,18,1,18,1,18,1,19,1,19,1,19,1,19,1,19,1,19,1,20,1,20,
  	1,20,1,20,1,20,1,20,1,21,1,21,1,21,1,21,1,21,1,21,1,21,1,22,1,22,1,22,
  	1,22,1,22,1,23,1,23,5,23,153,8,23,10,23,12,23,156,9,23,1,24,1,24,1,24,
  	1,24,1,25,1,25,1,25,1,25,5,25,166,8,25,10,25,12,25,169,9,25,1,25,1,25,
  	0,0,26,1,1,3,2,5,3,7,4,9,5,11,6,13,7,15,8,17,9,19,10,21,11,23,12,25,13,
  	27,14,29,15,31,16,33,17,35,18,37,19,39,20,41,21,43,22,45,23,47,24,49,
  	25,51,26,1,0,4,3,0,65,90,95,95,97,122,4,0,48,57,65,90,95,95,97,122,3,
  	0,9,10,13,13,32,32,2,0,10,10,13,13,173,0,1,1,0,0,0,0,3,1,0,0,0,0,5,1,
  	0,0,0,0,7,1,0,0,0,0,9,1,0,0,0,0,11,1,0,0,0,0,13,1,0,0,0,0,15,1,0,0,0,
  	0,17,1,0,0,0,0,19,1,0,0,0,0,21,1,0,0,0,0,23,1,0,0,0,0,25,1,0,0,0,0,27,
  	1,0,0,0,0,29,1,0,0,0,0,31,1,0,0,0,0,33,1,0,0,0,0,35,1,0,0,0,0,37,1,0,
  	0,0,0,39,1,0,0,0,0,41,1,0,0,0,0,43,1,0,0,0,0,45,1,0,0,0,0,47,1,0,0,0,
  	0,49,1,0,0,0,0,51,1,0,0,0,1,53,1,0,0,0,3,63,1,0,0,0,5,65,1,0,0,0,7,67,
  	1,0,0,0,9,70,1,0,0,0,11,72,1,0,0,0,13,74,1,0,0,0,15,76,1,0,0,0,17,79,
  	1,0,0,0,19,81,1,0,0,0,21,83,1,0,0,0,23,90,1,0,0,0,25,95,1,0,0,0,27,97,
  	1,0,0,0,29,99,1,0,0,0,31,103,1,0,0,0,33,109,1,0,0,0,35,114,1,0,0,0,37,
  	120,1,0,0,0,39,126,1,0,0,0,41,132,1,0,0,0,43,138,1,0,0,0,45,145,1,0,0,
  	0,47,150,1,0,0,0,49,157,1,0,0,0,51,161,1,0,0,0,53,54,5,110,0,0,54,55,
  	5,97,0,0,55,56,5,109,0,0,56,57,5,101,0,0,57,58,5,115,0,0,58,59,5,112,
  	0,0,59,60,5,97,0,0,60,61,5,99,0,0,61,62,5,101,0,0,62,2,1,0,0,0,63,64,
  	5,123,0,0,64,4,1,0,0,0,65,66,5,125,0,0,66,6,1,0,0,0,67,68,5,102,0,0,68,
  	69,5,110,0,0,69,8,1,0,0,0,70,71,5,40,0,0,71,10,1,0,0,0,72,73,5,41,0,0,
  	73,12,1,0,0,0,74,75,5,59,0,0,75,14,1,0,0,0,76,77,5,45,0,0,77,78,5,62,
  	0,0,78,16,1,0,0,0,79,80,5,58,0,0,80,18,1,0,0,0,81,82,5,44,0,0,82,20,1,
  	0,0,0,83,84,5,115,0,0,84,85,5,116,0,0,85,86,5,114,0,0,86,87,5,117,0,0,
  	87,88,5,99,0,0,88,89,5,116,0,0,89,22,1,0,0,0,90,91,5,76,0,0,91,92,5,105,
  	0,0,92,93,5,115,0,0,93,94,5,116,0,0,94,24,1,0,0,0,95,96,5,60,0,0,96,26,
  	1,0,0,0,97,98,5,62,0,0,98,28,1,0,0,0,99,100,5,77,0,0,100,101,5,97,0,0,
  	101,102,5,112,0,0,102,30,1,0,0,0,103,104,5,84,0,0,104,105,5,117,0,0,105,
  	106,5,112,0,0,106,107,5,108,0,0,107,108,5,101,0,0,108,32,1,0,0,0,109,
  	110,5,105,0,0,110,111,5,110,0,0,111,112,5,116,0,0,112,113,5,56,0,0,113,
  	34,1,0,0,0,114,115,5,105,0,0,115,116,5,110,0,0,116,117,5,116,0,0,117,
  	118,5,49,0,0,118,119,5,54,0,0,119,36,1,0,0,0,120,121,5,105,0,0,121,122,
  	5,110,0,0,122,123,5,116,0,0,123,124,5,51,0,0,124,125,5,50,0,0,125,38,
  	1,0,0,0,126,127,5,105,0,0,127,128,5,110,0,0,128,129,5,116,0,0,129,130,
  	5,54,0,0,130,131,5,52,0,0,131,40,1,0,0,0,132,133,5,102,0,0,133,134,5,
  	108,0,0,134,135,5,111,0,0,135,136,5,97,0,0,136,137,5,116,0,0,137,42,1,
  	0,0,0,138,139,5,100,0,0,139,140,5,111,0,0,140,141,5,117,0,0,141,142,5,
  	98,0,0,142,143,5,108,0,0,143,144,5,101,0,0,144,44,1,0,0,0,145,146,5,98,
  	0,0,146,147,5,111,0,0,147,148,5,111,0,0,148,149,5,108,0,0,149,46,1,0,
  	0,0,150,154,7,0,0,0,151,153,7,1,0,0,152,151,1,0,0,0,153,156,1,0,0,0,154,
  	152,1,0,0,0,154,155,1,0,0,0,155,48,1,0,0,0,156,154,1,0,0,0,157,158,7,
  	2,0,0,158,159,1,0,0,0,159,160,6,24,0,0,160,50,1,0,0,0,161,162,5,47,0,
  	0,162,163,5,47,0,0,163,167,1,0,0,0,164,166,8,3,0,0,165,164,1,0,0,0,166,
  	169,1,0,0,0,167,165,1,0,0,0,167,168,1,0,0,0,168,170,1,0,0,0,169,167,1,
  	0,0,0,170,171,6,25,0,0,171,52,1,0,0,0,3,0,154,167,1,6,0,0
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
