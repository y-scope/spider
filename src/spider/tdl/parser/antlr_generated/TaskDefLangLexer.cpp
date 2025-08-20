
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
      "T__17", "T__18", "T__19", "T__20", "T__21", "ID", "SPACE", "COMMENT"
    },
    std::vector<std::string>{
      "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    },
    std::vector<std::string>{
      "DEFAULT_MODE"
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
  	4,0,25,171,6,-1,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
  	6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,
  	7,14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,
  	7,21,2,22,7,22,2,23,7,23,2,24,7,24,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,
  	0,1,0,1,1,1,1,1,2,1,2,1,3,1,3,1,3,1,4,1,4,1,5,1,5,1,6,1,6,1,7,1,7,1,7,
  	1,8,1,8,1,9,1,9,1,10,1,10,1,10,1,10,1,10,1,10,1,10,1,11,1,11,1,11,1,11,
  	1,11,1,12,1,12,1,12,1,12,1,12,1,12,1,13,1,13,1,13,1,13,1,13,1,13,1,14,
  	1,14,1,14,1,14,1,14,1,14,1,15,1,15,1,15,1,15,1,15,1,15,1,16,1,16,1,16,
  	1,16,1,16,1,16,1,16,1,17,1,17,1,17,1,17,1,17,1,18,1,18,1,18,1,18,1,18,
  	1,18,1,19,1,19,1,20,1,20,1,20,1,20,1,20,1,21,1,21,1,21,1,21,1,21,1,21,
  	1,21,1,22,1,22,5,22,152,8,22,10,22,12,22,155,9,22,1,23,1,23,1,23,1,23,
  	1,24,1,24,1,24,1,24,5,24,165,8,24,10,24,12,24,168,9,24,1,24,1,24,0,0,
  	25,1,1,3,2,5,3,7,4,9,5,11,6,13,7,15,8,17,9,19,10,21,11,23,12,25,13,27,
  	14,29,15,31,16,33,17,35,18,37,19,39,20,41,21,43,22,45,23,47,24,49,25,
  	1,0,4,3,0,65,90,95,95,97,122,4,0,48,57,65,90,95,95,97,122,3,0,9,10,13,
  	13,32,32,2,0,10,10,13,13,172,0,1,1,0,0,0,0,3,1,0,0,0,0,5,1,0,0,0,0,7,
  	1,0,0,0,0,9,1,0,0,0,0,11,1,0,0,0,0,13,1,0,0,0,0,15,1,0,0,0,0,17,1,0,0,
  	0,0,19,1,0,0,0,0,21,1,0,0,0,0,23,1,0,0,0,0,25,1,0,0,0,0,27,1,0,0,0,0,
  	29,1,0,0,0,0,31,1,0,0,0,0,33,1,0,0,0,0,35,1,0,0,0,0,37,1,0,0,0,0,39,1,
  	0,0,0,0,41,1,0,0,0,0,43,1,0,0,0,0,45,1,0,0,0,0,47,1,0,0,0,0,49,1,0,0,
  	0,1,51,1,0,0,0,3,61,1,0,0,0,5,63,1,0,0,0,7,65,1,0,0,0,9,68,1,0,0,0,11,
  	70,1,0,0,0,13,72,1,0,0,0,15,74,1,0,0,0,17,77,1,0,0,0,19,79,1,0,0,0,21,
  	81,1,0,0,0,23,88,1,0,0,0,25,93,1,0,0,0,27,99,1,0,0,0,29,105,1,0,0,0,31,
  	111,1,0,0,0,33,117,1,0,0,0,35,124,1,0,0,0,37,129,1,0,0,0,39,135,1,0,0,
  	0,41,137,1,0,0,0,43,142,1,0,0,0,45,149,1,0,0,0,47,156,1,0,0,0,49,160,
  	1,0,0,0,51,52,5,110,0,0,52,53,5,97,0,0,53,54,5,109,0,0,54,55,5,101,0,
  	0,55,56,5,115,0,0,56,57,5,112,0,0,57,58,5,97,0,0,58,59,5,99,0,0,59,60,
  	5,101,0,0,60,2,1,0,0,0,61,62,5,123,0,0,62,4,1,0,0,0,63,64,5,125,0,0,64,
  	6,1,0,0,0,65,66,5,102,0,0,66,67,5,110,0,0,67,8,1,0,0,0,68,69,5,40,0,0,
  	69,10,1,0,0,0,70,71,5,41,0,0,71,12,1,0,0,0,72,73,5,59,0,0,73,14,1,0,0,
  	0,74,75,5,45,0,0,75,76,5,62,0,0,76,16,1,0,0,0,77,78,5,58,0,0,78,18,1,
  	0,0,0,79,80,5,44,0,0,80,20,1,0,0,0,81,82,5,115,0,0,82,83,5,116,0,0,83,
  	84,5,114,0,0,84,85,5,117,0,0,85,86,5,99,0,0,86,87,5,116,0,0,87,22,1,0,
  	0,0,88,89,5,105,0,0,89,90,5,110,0,0,90,91,5,116,0,0,91,92,5,56,0,0,92,
  	24,1,0,0,0,93,94,5,105,0,0,94,95,5,110,0,0,95,96,5,116,0,0,96,97,5,49,
  	0,0,97,98,5,54,0,0,98,26,1,0,0,0,99,100,5,105,0,0,100,101,5,110,0,0,101,
  	102,5,116,0,0,102,103,5,51,0,0,103,104,5,50,0,0,104,28,1,0,0,0,105,106,
  	5,105,0,0,106,107,5,110,0,0,107,108,5,116,0,0,108,109,5,54,0,0,109,110,
  	5,52,0,0,110,30,1,0,0,0,111,112,5,102,0,0,112,113,5,108,0,0,113,114,5,
  	111,0,0,114,115,5,97,0,0,115,116,5,116,0,0,116,32,1,0,0,0,117,118,5,100,
  	0,0,118,119,5,111,0,0,119,120,5,117,0,0,120,121,5,98,0,0,121,122,5,108,
  	0,0,122,123,5,101,0,0,123,34,1,0,0,0,124,125,5,98,0,0,125,126,5,111,0,
  	0,126,127,5,111,0,0,127,128,5,108,0,0,128,36,1,0,0,0,129,130,5,76,0,0,
  	130,131,5,105,0,0,131,132,5,115,0,0,132,133,5,116,0,0,133,134,5,60,0,
  	0,134,38,1,0,0,0,135,136,5,62,0,0,136,40,1,0,0,0,137,138,5,77,0,0,138,
  	139,5,97,0,0,139,140,5,112,0,0,140,141,5,60,0,0,141,42,1,0,0,0,142,143,
  	5,84,0,0,143,144,5,117,0,0,144,145,5,112,0,0,145,146,5,108,0,0,146,147,
  	5,101,0,0,147,148,5,60,0,0,148,44,1,0,0,0,149,153,7,0,0,0,150,152,7,1,
  	0,0,151,150,1,0,0,0,152,155,1,0,0,0,153,151,1,0,0,0,153,154,1,0,0,0,154,
  	46,1,0,0,0,155,153,1,0,0,0,156,157,7,2,0,0,157,158,1,0,0,0,158,159,6,
  	23,0,0,159,48,1,0,0,0,160,161,5,47,0,0,161,162,5,47,0,0,162,166,1,0,0,
  	0,163,165,8,3,0,0,164,163,1,0,0,0,165,168,1,0,0,0,166,164,1,0,0,0,166,
  	167,1,0,0,0,167,169,1,0,0,0,168,166,1,0,0,0,169,170,6,24,0,0,170,50,1,
  	0,0,0,3,0,153,166,1,6,0,0
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
