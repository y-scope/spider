grammar Stdl;

start: (service | struct)* EOF ;

service: 'service' ID '{' (function)* '}' ;

function: 'fn' ID '(' (parameter (',' parameter)*)? ')' '->' return_type ';' ;

parameter: ID ':' type ;

struct : 'struct' ID '{' field (',' field)* (',')? '}' ;

field: type ID ;

return_type
    : '(' type (',' type)* ')'
    | type
    ;

ID: [a-zA-Z_][a-zA-Z0-9_]* ;

type
    : ID
    | builtin_type
    ;

builtin_type
    : 'List' '<' type '>'
    | 'Map' '<' type ',' type '>'
    | 'int8'
    | 'int16'
    | 'int32'
    | 'int64'
    | 'float'
    | 'double'
    | 'bool'
    | 'char'
    | 'string'
    ;

WS: [ \t\r\n]+ -> skip ;
