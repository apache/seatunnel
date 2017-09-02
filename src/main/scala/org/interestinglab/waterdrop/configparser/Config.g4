grammar Config;

// Filter, Output 允许if else
// STRING lexer rule定义过于简单，不满足需求，包括包含引号的字符串.
// 允许comment
// [done]允许key不包含双引号
// [done] 允许多行配置没有"，"分割
// [done] 允许plugin中不包含任何配置

config
    : input filter output
    ;

input
    : 'input' '{' (STRING obj)* '}'
    ;

filter
    : 'filter' '{' (STRING obj)* '}'
    ;

output
    : 'output' '{' (STRING obj)* '}'
    ;

obj
    : '{' pair (','? pair)* '}'
    | '{' '}'
    ;

pair
   : STRING '=' value
   ;

array
   : '[' value (',' value)* ']'
   | '[' ']'
   ;

value
   : STRING
   | NUMBER
//   | obj
   | array
   | 'true'
   | 'false'
   | 'null'
   ;

STRING
   : UNQUOTE_STRING | SINGLE_QUOTE_STRING | DOUBLE_QUOTE_STRING
   ;

fragment UNQUOTE_STRING
    : [a-zA-Z]+
    ;
fragment SINGLE_QUOTE_STRING
    : '\'' UNQUOTE_STRING '\''
    ;
fragment DOUBLE_QUOTE_STRING
    : '"' UNQUOTE_STRING '"'
    ;

NUMBER
   : '-'? INT '.' [0-9] + EXP? | '-'? INT EXP | '-'? INT
   ;
fragment INT
   : '0' | [1-9] [0-9]*
   ;
// no leading zeros
fragment EXP
   : [Ee] [+\-]? INT
   ;

WS
    : [ \t\n\r]+ -> skip
    ;
