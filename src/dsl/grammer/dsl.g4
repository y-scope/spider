grammar dsl;

start: service EOF ;

service: 'service' ID '{' (function)* '}' ;

function: 'fn' ID '(' (parameter (',' parameter)*)? ')' '->' type ';' ;

parameter: ID ':' type ;

ID: [a-zA-Z_][a-zA-Z0-9_]* ;

type
    : 'int8'
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
