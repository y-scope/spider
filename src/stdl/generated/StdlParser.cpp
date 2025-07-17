
// Generated from /data/sitao/code/spider/src/stdl/parser/Stdl.g4 by ANTLR 4.13.2


#include "StdlVisitor.h"

#include "StdlParser.h"


using namespace antlrcpp;
using namespace spider::dsl::parser;

using namespace antlr4;

namespace {

struct StdlParserStaticData final {
  StdlParserStaticData(std::vector<std::string> ruleNames,
                        std::vector<std::string> literalNames,
                        std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  StdlParserStaticData(const StdlParserStaticData&) = delete;
  StdlParserStaticData(StdlParserStaticData&&) = delete;
  StdlParserStaticData& operator=(const StdlParserStaticData&) = delete;
  StdlParserStaticData& operator=(StdlParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag stdlParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
std::unique_ptr<StdlParserStaticData> stdlParserStaticData = nullptr;

void stdlParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (stdlParserStaticData != nullptr) {
    return;
  }
#else
  assert(stdlParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<StdlParserStaticData>(
    std::vector<std::string>{
      "start", "service", "function", "parameter", "struct", "field", "return_type", 
      "type", "builtin_type"
    },
    std::vector<std::string>{
      "", "'service'", "'{'", "'}'", "'fn'", "'('", "','", "')'", "'->'", 
      "';'", "':'", "'struct'", "'List'", "'<'", "'>'", "'Map'", "'int8'", 
      "'int16'", "'int32'", "'int64'", "'float'", "'double'", "'bool'", 
      "'char'", "'string'"
    },
    std::vector<std::string>{
      "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
      "", "", "", "", "", "", "", "", "ID", "WS"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,26,121,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,2,
  	7,7,7,2,8,7,8,1,0,1,0,5,0,21,8,0,10,0,12,0,24,9,0,1,0,1,0,1,1,1,1,1,1,
  	1,1,5,1,32,8,1,10,1,12,1,35,9,1,1,1,1,1,1,2,1,2,1,2,1,2,1,2,1,2,5,2,45,
  	8,2,10,2,12,2,48,9,2,3,2,50,8,2,1,2,1,2,1,2,1,2,1,2,1,3,1,3,1,3,1,3,1,
  	4,1,4,1,4,1,4,1,4,1,4,5,4,67,8,4,10,4,12,4,70,9,4,1,4,3,4,73,8,4,1,4,
  	1,4,1,5,1,5,1,5,1,6,1,6,1,6,1,6,5,6,84,8,6,10,6,12,6,87,9,6,1,6,1,6,1,
  	6,3,6,92,8,6,1,7,1,7,3,7,96,8,7,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,
  	8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,3,8,119,8,8,1,8,0,0,9,0,
  	2,4,6,8,10,12,14,16,0,0,131,0,22,1,0,0,0,2,27,1,0,0,0,4,38,1,0,0,0,6,
  	56,1,0,0,0,8,60,1,0,0,0,10,76,1,0,0,0,12,91,1,0,0,0,14,95,1,0,0,0,16,
  	118,1,0,0,0,18,21,3,2,1,0,19,21,3,8,4,0,20,18,1,0,0,0,20,19,1,0,0,0,21,
  	24,1,0,0,0,22,20,1,0,0,0,22,23,1,0,0,0,23,25,1,0,0,0,24,22,1,0,0,0,25,
  	26,5,0,0,1,26,1,1,0,0,0,27,28,5,1,0,0,28,29,5,25,0,0,29,33,5,2,0,0,30,
  	32,3,4,2,0,31,30,1,0,0,0,32,35,1,0,0,0,33,31,1,0,0,0,33,34,1,0,0,0,34,
  	36,1,0,0,0,35,33,1,0,0,0,36,37,5,3,0,0,37,3,1,0,0,0,38,39,5,4,0,0,39,
  	40,5,25,0,0,40,49,5,5,0,0,41,46,3,6,3,0,42,43,5,6,0,0,43,45,3,6,3,0,44,
  	42,1,0,0,0,45,48,1,0,0,0,46,44,1,0,0,0,46,47,1,0,0,0,47,50,1,0,0,0,48,
  	46,1,0,0,0,49,41,1,0,0,0,49,50,1,0,0,0,50,51,1,0,0,0,51,52,5,7,0,0,52,
  	53,5,8,0,0,53,54,3,12,6,0,54,55,5,9,0,0,55,5,1,0,0,0,56,57,5,25,0,0,57,
  	58,5,10,0,0,58,59,3,14,7,0,59,7,1,0,0,0,60,61,5,11,0,0,61,62,5,25,0,0,
  	62,63,5,2,0,0,63,68,3,10,5,0,64,65,5,6,0,0,65,67,3,10,5,0,66,64,1,0,0,
  	0,67,70,1,0,0,0,68,66,1,0,0,0,68,69,1,0,0,0,69,72,1,0,0,0,70,68,1,0,0,
  	0,71,73,5,6,0,0,72,71,1,0,0,0,72,73,1,0,0,0,73,74,1,0,0,0,74,75,5,3,0,
  	0,75,9,1,0,0,0,76,77,3,14,7,0,77,78,5,25,0,0,78,11,1,0,0,0,79,80,5,5,
  	0,0,80,85,3,14,7,0,81,82,5,6,0,0,82,84,3,14,7,0,83,81,1,0,0,0,84,87,1,
  	0,0,0,85,83,1,0,0,0,85,86,1,0,0,0,86,88,1,0,0,0,87,85,1,0,0,0,88,89,5,
  	7,0,0,89,92,1,0,0,0,90,92,3,14,7,0,91,79,1,0,0,0,91,90,1,0,0,0,92,13,
  	1,0,0,0,93,96,5,25,0,0,94,96,3,16,8,0,95,93,1,0,0,0,95,94,1,0,0,0,96,
  	15,1,0,0,0,97,98,5,12,0,0,98,99,5,13,0,0,99,100,3,14,7,0,100,101,5,14,
  	0,0,101,119,1,0,0,0,102,103,5,15,0,0,103,104,5,13,0,0,104,105,3,14,7,
  	0,105,106,5,6,0,0,106,107,3,14,7,0,107,108,5,14,0,0,108,119,1,0,0,0,109,
  	119,5,16,0,0,110,119,5,17,0,0,111,119,5,18,0,0,112,119,5,19,0,0,113,119,
  	5,20,0,0,114,119,5,21,0,0,115,119,5,22,0,0,116,119,5,23,0,0,117,119,5,
  	24,0,0,118,97,1,0,0,0,118,102,1,0,0,0,118,109,1,0,0,0,118,110,1,0,0,0,
  	118,111,1,0,0,0,118,112,1,0,0,0,118,113,1,0,0,0,118,114,1,0,0,0,118,115,
  	1,0,0,0,118,116,1,0,0,0,118,117,1,0,0,0,119,17,1,0,0,0,11,20,22,33,46,
  	49,68,72,85,91,95,118
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  stdlParserStaticData = std::move(staticData);
}

}

StdlParser::StdlParser(TokenStream *input) : StdlParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

StdlParser::StdlParser(TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options) : Parser(input) {
  StdlParser::initialize();
  _interpreter = new atn::ParserATNSimulator(this, *stdlParserStaticData->atn, stdlParserStaticData->decisionToDFA, stdlParserStaticData->sharedContextCache, options);
}

StdlParser::~StdlParser() {
  delete _interpreter;
}

const atn::ATN& StdlParser::getATN() const {
  return *stdlParserStaticData->atn;
}

std::string StdlParser::getGrammarFileName() const {
  return "Stdl.g4";
}

const std::vector<std::string>& StdlParser::getRuleNames() const {
  return stdlParserStaticData->ruleNames;
}

const dfa::Vocabulary& StdlParser::getVocabulary() const {
  return stdlParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView StdlParser::getSerializedATN() const {
  return stdlParserStaticData->serializedATN;
}


//----------------- StartContext ------------------------------------------------------------------

StdlParser::StartContext::StartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* StdlParser::StartContext::EOF() {
  return getToken(StdlParser::EOF, 0);
}

std::vector<StdlParser::ServiceContext *> StdlParser::StartContext::service() {
  return getRuleContexts<StdlParser::ServiceContext>();
}

StdlParser::ServiceContext* StdlParser::StartContext::service(size_t i) {
  return getRuleContext<StdlParser::ServiceContext>(i);
}

std::vector<StdlParser::StructContext *> StdlParser::StartContext::struct_() {
  return getRuleContexts<StdlParser::StructContext>();
}

StdlParser::StructContext* StdlParser::StartContext::struct_(size_t i) {
  return getRuleContext<StdlParser::StructContext>(i);
}


size_t StdlParser::StartContext::getRuleIndex() const {
  return StdlParser::RuleStart;
}


std::any StdlParser::StartContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitStart(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::StartContext* StdlParser::start() {
  StartContext *_localctx = _tracker.createInstance<StartContext>(_ctx, getState());
  enterRule(_localctx, 0, StdlParser::RuleStart);
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
    setState(22);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == StdlParser::T__0

    || _la == StdlParser::T__10) {
      setState(20);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case StdlParser::T__0: {
          setState(18);
          service();
          break;
        }

        case StdlParser::T__10: {
          setState(19);
          struct_();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(24);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(25);
    match(StdlParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ServiceContext ------------------------------------------------------------------

StdlParser::ServiceContext::ServiceContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* StdlParser::ServiceContext::ID() {
  return getToken(StdlParser::ID, 0);
}

std::vector<StdlParser::FunctionContext *> StdlParser::ServiceContext::function() {
  return getRuleContexts<StdlParser::FunctionContext>();
}

StdlParser::FunctionContext* StdlParser::ServiceContext::function(size_t i) {
  return getRuleContext<StdlParser::FunctionContext>(i);
}


size_t StdlParser::ServiceContext::getRuleIndex() const {
  return StdlParser::RuleService;
}


std::any StdlParser::ServiceContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitService(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::ServiceContext* StdlParser::service() {
  ServiceContext *_localctx = _tracker.createInstance<ServiceContext>(_ctx, getState());
  enterRule(_localctx, 2, StdlParser::RuleService);
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
    setState(27);
    match(StdlParser::T__0);
    setState(28);
    match(StdlParser::ID);
    setState(29);
    match(StdlParser::T__1);
    setState(33);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == StdlParser::T__3) {
      setState(30);
      function();
      setState(35);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(36);
    match(StdlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FunctionContext ------------------------------------------------------------------

StdlParser::FunctionContext::FunctionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* StdlParser::FunctionContext::ID() {
  return getToken(StdlParser::ID, 0);
}

StdlParser::Return_typeContext* StdlParser::FunctionContext::return_type() {
  return getRuleContext<StdlParser::Return_typeContext>(0);
}

std::vector<StdlParser::ParameterContext *> StdlParser::FunctionContext::parameter() {
  return getRuleContexts<StdlParser::ParameterContext>();
}

StdlParser::ParameterContext* StdlParser::FunctionContext::parameter(size_t i) {
  return getRuleContext<StdlParser::ParameterContext>(i);
}


size_t StdlParser::FunctionContext::getRuleIndex() const {
  return StdlParser::RuleFunction;
}


std::any StdlParser::FunctionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitFunction(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::FunctionContext* StdlParser::function() {
  FunctionContext *_localctx = _tracker.createInstance<FunctionContext>(_ctx, getState());
  enterRule(_localctx, 4, StdlParser::RuleFunction);
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
    setState(38);
    match(StdlParser::T__3);
    setState(39);
    match(StdlParser::ID);
    setState(40);
    match(StdlParser::T__4);
    setState(49);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == StdlParser::ID) {
      setState(41);
      parameter();
      setState(46);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == StdlParser::T__5) {
        setState(42);
        match(StdlParser::T__5);
        setState(43);
        parameter();
        setState(48);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(51);
    match(StdlParser::T__6);
    setState(52);
    match(StdlParser::T__7);
    setState(53);
    return_type();
    setState(54);
    match(StdlParser::T__8);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParameterContext ------------------------------------------------------------------

StdlParser::ParameterContext::ParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* StdlParser::ParameterContext::ID() {
  return getToken(StdlParser::ID, 0);
}

StdlParser::TypeContext* StdlParser::ParameterContext::type() {
  return getRuleContext<StdlParser::TypeContext>(0);
}


size_t StdlParser::ParameterContext::getRuleIndex() const {
  return StdlParser::RuleParameter;
}


std::any StdlParser::ParameterContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitParameter(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::ParameterContext* StdlParser::parameter() {
  ParameterContext *_localctx = _tracker.createInstance<ParameterContext>(_ctx, getState());
  enterRule(_localctx, 6, StdlParser::RuleParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(56);
    match(StdlParser::ID);
    setState(57);
    match(StdlParser::T__9);
    setState(58);
    type();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StructContext ------------------------------------------------------------------

StdlParser::StructContext::StructContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* StdlParser::StructContext::ID() {
  return getToken(StdlParser::ID, 0);
}

std::vector<StdlParser::FieldContext *> StdlParser::StructContext::field() {
  return getRuleContexts<StdlParser::FieldContext>();
}

StdlParser::FieldContext* StdlParser::StructContext::field(size_t i) {
  return getRuleContext<StdlParser::FieldContext>(i);
}


size_t StdlParser::StructContext::getRuleIndex() const {
  return StdlParser::RuleStruct;
}


std::any StdlParser::StructContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitStruct(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::StructContext* StdlParser::struct_() {
  StructContext *_localctx = _tracker.createInstance<StructContext>(_ctx, getState());
  enterRule(_localctx, 8, StdlParser::RuleStruct);
  size_t _la = 0;

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
    setState(60);
    match(StdlParser::T__10);
    setState(61);
    match(StdlParser::ID);
    setState(62);
    match(StdlParser::T__1);
    setState(63);
    field();
    setState(68);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(64);
        match(StdlParser::T__5);
        setState(65);
        field(); 
      }
      setState(70);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    }
    setState(72);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == StdlParser::T__5) {
      setState(71);
      match(StdlParser::T__5);
    }
    setState(74);
    match(StdlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FieldContext ------------------------------------------------------------------

StdlParser::FieldContext::FieldContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

StdlParser::TypeContext* StdlParser::FieldContext::type() {
  return getRuleContext<StdlParser::TypeContext>(0);
}

tree::TerminalNode* StdlParser::FieldContext::ID() {
  return getToken(StdlParser::ID, 0);
}


size_t StdlParser::FieldContext::getRuleIndex() const {
  return StdlParser::RuleField;
}


std::any StdlParser::FieldContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitField(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::FieldContext* StdlParser::field() {
  FieldContext *_localctx = _tracker.createInstance<FieldContext>(_ctx, getState());
  enterRule(_localctx, 10, StdlParser::RuleField);

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
    type();
    setState(77);
    match(StdlParser::ID);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Return_typeContext ------------------------------------------------------------------

StdlParser::Return_typeContext::Return_typeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<StdlParser::TypeContext *> StdlParser::Return_typeContext::type() {
  return getRuleContexts<StdlParser::TypeContext>();
}

StdlParser::TypeContext* StdlParser::Return_typeContext::type(size_t i) {
  return getRuleContext<StdlParser::TypeContext>(i);
}


size_t StdlParser::Return_typeContext::getRuleIndex() const {
  return StdlParser::RuleReturn_type;
}


std::any StdlParser::Return_typeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitReturn_type(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::Return_typeContext* StdlParser::return_type() {
  Return_typeContext *_localctx = _tracker.createInstance<Return_typeContext>(_ctx, getState());
  enterRule(_localctx, 12, StdlParser::RuleReturn_type);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(91);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case StdlParser::T__4: {
        enterOuterAlt(_localctx, 1);
        setState(79);
        match(StdlParser::T__4);
        setState(80);
        type();
        setState(85);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == StdlParser::T__5) {
          setState(81);
          match(StdlParser::T__5);
          setState(82);
          type();
          setState(87);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(88);
        match(StdlParser::T__6);
        break;
      }

      case StdlParser::T__11:
      case StdlParser::T__14:
      case StdlParser::T__15:
      case StdlParser::T__16:
      case StdlParser::T__17:
      case StdlParser::T__18:
      case StdlParser::T__19:
      case StdlParser::T__20:
      case StdlParser::T__21:
      case StdlParser::T__22:
      case StdlParser::T__23:
      case StdlParser::ID: {
        enterOuterAlt(_localctx, 2);
        setState(90);
        type();
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

//----------------- TypeContext ------------------------------------------------------------------

StdlParser::TypeContext::TypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* StdlParser::TypeContext::ID() {
  return getToken(StdlParser::ID, 0);
}

StdlParser::Builtin_typeContext* StdlParser::TypeContext::builtin_type() {
  return getRuleContext<StdlParser::Builtin_typeContext>(0);
}


size_t StdlParser::TypeContext::getRuleIndex() const {
  return StdlParser::RuleType;
}


std::any StdlParser::TypeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitType(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::TypeContext* StdlParser::type() {
  TypeContext *_localctx = _tracker.createInstance<TypeContext>(_ctx, getState());
  enterRule(_localctx, 14, StdlParser::RuleType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(95);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case StdlParser::ID: {
        enterOuterAlt(_localctx, 1);
        setState(93);
        match(StdlParser::ID);
        break;
      }

      case StdlParser::T__11:
      case StdlParser::T__14:
      case StdlParser::T__15:
      case StdlParser::T__16:
      case StdlParser::T__17:
      case StdlParser::T__18:
      case StdlParser::T__19:
      case StdlParser::T__20:
      case StdlParser::T__21:
      case StdlParser::T__22:
      case StdlParser::T__23: {
        enterOuterAlt(_localctx, 2);
        setState(94);
        builtin_type();
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

//----------------- Builtin_typeContext ------------------------------------------------------------------

StdlParser::Builtin_typeContext::Builtin_typeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<StdlParser::TypeContext *> StdlParser::Builtin_typeContext::type() {
  return getRuleContexts<StdlParser::TypeContext>();
}

StdlParser::TypeContext* StdlParser::Builtin_typeContext::type(size_t i) {
  return getRuleContext<StdlParser::TypeContext>(i);
}


size_t StdlParser::Builtin_typeContext::getRuleIndex() const {
  return StdlParser::RuleBuiltin_type;
}


std::any StdlParser::Builtin_typeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<StdlVisitor*>(visitor))
    return parserVisitor->visitBuiltin_type(this);
  else
    return visitor->visitChildren(this);
}

StdlParser::Builtin_typeContext* StdlParser::builtin_type() {
  Builtin_typeContext *_localctx = _tracker.createInstance<Builtin_typeContext>(_ctx, getState());
  enterRule(_localctx, 16, StdlParser::RuleBuiltin_type);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(118);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case StdlParser::T__11: {
        enterOuterAlt(_localctx, 1);
        setState(97);
        match(StdlParser::T__11);
        setState(98);
        match(StdlParser::T__12);
        setState(99);
        type();
        setState(100);
        match(StdlParser::T__13);
        break;
      }

      case StdlParser::T__14: {
        enterOuterAlt(_localctx, 2);
        setState(102);
        match(StdlParser::T__14);
        setState(103);
        match(StdlParser::T__12);
        setState(104);
        type();
        setState(105);
        match(StdlParser::T__5);
        setState(106);
        type();
        setState(107);
        match(StdlParser::T__13);
        break;
      }

      case StdlParser::T__15: {
        enterOuterAlt(_localctx, 3);
        setState(109);
        match(StdlParser::T__15);
        break;
      }

      case StdlParser::T__16: {
        enterOuterAlt(_localctx, 4);
        setState(110);
        match(StdlParser::T__16);
        break;
      }

      case StdlParser::T__17: {
        enterOuterAlt(_localctx, 5);
        setState(111);
        match(StdlParser::T__17);
        break;
      }

      case StdlParser::T__18: {
        enterOuterAlt(_localctx, 6);
        setState(112);
        match(StdlParser::T__18);
        break;
      }

      case StdlParser::T__19: {
        enterOuterAlt(_localctx, 7);
        setState(113);
        match(StdlParser::T__19);
        break;
      }

      case StdlParser::T__20: {
        enterOuterAlt(_localctx, 8);
        setState(114);
        match(StdlParser::T__20);
        break;
      }

      case StdlParser::T__21: {
        enterOuterAlt(_localctx, 9);
        setState(115);
        match(StdlParser::T__21);
        break;
      }

      case StdlParser::T__22: {
        enterOuterAlt(_localctx, 10);
        setState(116);
        match(StdlParser::T__22);
        break;
      }

      case StdlParser::T__23: {
        enterOuterAlt(_localctx, 11);
        setState(117);
        match(StdlParser::T__23);
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

void StdlParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  stdlParserInitialize();
#else
  ::antlr4::internal::call_once(stdlParserOnceFlag, stdlParserInitialize);
#endif
}
