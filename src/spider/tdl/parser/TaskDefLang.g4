grammar TaskDefLang;

translationUnit: (namespace | structDef)* EOF ;

namespace
: 'namespace' id '{' (funcDef)* '}'
;

funcDef
: 'fn' id '(' params ')' ret ';'
;

ret
: '->' type
|
;

params
: namedVarList
|
;

namedVar
: id ':' type
;

namedVarList
: namedVar
| namedVarList ',' namedVar
;

structDef
: 'struct' id '{' namedVarList (',')? '}' ';'
;

id
: ID
;

type
: builtinType
| id
;

typeList
: type
| typeList ',' type
|
;

builtinType
: 'int8'
| 'int16'
| 'int32'
| 'int64'
| 'float'
| 'double'
| 'bool'
| 'List<' type '>'
| 'Map<' type ',' type '>'
| 'Tuple<' typeList '>'
;

ID: [a-zA-Z_][a-zA-Z0-9_]* ;
SPACE:  [ \t\r\n] -> skip ;
COMMENT: '//' (~[\r\n])* -> skip;
