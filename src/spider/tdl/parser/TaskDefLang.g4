grammar TaskDefLang;

translationUnit: (namespace | structDef)* EOF ;

namespace
: 'namespace' id '{' (funcDef)* '}'
;

funcDef
: 'fn' id '(' params ')' ret ';'
;

ret
: '->' retType
|
;

params
: namedVarList
|
;

namedVar
: id ':' varType
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

varType
: builtinType
| id
;

retType
: varType
| tupleType
;

varTypeList
: varType
| varTypeList ',' varType
|
;

listType
: 'List' '<' varType '>'
;

mapType
: 'Map' '<' varType ',' varType '>'
;

tupleType
: 'Tuple' '<' varTypeList '>'
;

builtinType
: 'int8'
| 'int16'
| 'int32'
| 'int64'
| 'float'
| 'double'
| 'bool'
| listType
| mapType
;

ID: [a-zA-Z_][a-zA-Z0-9_]* ;
SPACE:  [ \t\r\n] -> skip ;
COMMENT: '//' (~[\r\n])* -> skip;
