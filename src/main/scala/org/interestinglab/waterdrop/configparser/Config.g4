grammar Config;

// STRING lexer rule定义过于简单，不满足需求，包括包含引号的字符串.
// [done] Filter, Output 允许if else
// [done] if 中的逻辑运算，值引用
// [done]允许key不包含双引号
// [done] 允许多行配置没有"，"分割
// [done] 允许plugin中不包含任何配置

// notes: lexer rule vs parser rule

config
    : COMMENT* input COMMENT* filter COMMENT* output COMMENT*
    ;

input
    : 'input' '{' plugin_list '}'
    ;

filter
    : 'filter' '{' statement '}'
    ;

output
    : 'output' '{' statement '}'
    ;

statement
    : (plugin | if_block | COMMENT)*
    ;

plugin_list
    : (plugin | COMMENT)*
    ;

plugin
    : STRING obj
    ;

if_block
    : 'if' expr '{' plugin_list '}'
     ('else if' expr '{' plugin_list '}')*
     ('else' '{' plugin_list '}')?
    ;

// support nested boolean expression
expr
    : '(' expr ')'         // parens
    | op='~' expr          // NOT
    | expr op='&&' expr     // AND
    | expr op='||' expr     // OR
    | bool                 // bool
    ;

bool
    : STRING
    | STRING OPERATOR STRING
    ;

obj
    : '{' (pair | COMMENT)* '}'
    ;

//obj
//    : '{' pair (','? pair)* '}'
//    | '{' '}'
//    ;

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

COMMENT
    : '#' ~( '\r' | '\n' )*
    ;

STRING
    : UNQUOTE_STRING | SINGLE_QUOTE_STRING | DOUBLE_QUOTE_STRING
    ;

fragment UNQUOTE_STRING
    : [a-zA-Z0-9.,:_]+
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


OPERATOR
    : '>' | '>=' | '<' | '<=' | '=' | '!='
    ;

WS
    : [ \t\n\r]+ -> skip
    ;
