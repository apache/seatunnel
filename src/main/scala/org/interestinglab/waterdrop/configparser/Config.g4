grammar Config;


// nested bool expression中的字段引用
// STRING lexer rule定义过于简单，不满足需求, 包括其中包含Quote(',")的String
// [done] nested bool expression
// [done] Filter, Output 允许if else, 包含nested if..else
// [done]允许key不包含双引号
// [done] 允许多行配置没有"，"分割
// [done] 允许plugin中不包含任何配置

// notes: lexer rule vs parser rule

import BoolExpr;

config
    : COMMENT* 'input' input_block COMMENT* 'filter' filter_block COMMENT* 'output' output_block COMMENT*
    ;

input_block
    : '{' plugin_list '}'
    ;

filter_block
    : '{' statement '}'
    ;

output_block
    : '{' statement '}'
    ;

statement
    : (plugin | if_statement | COMMENT)*
    ;

if_statement
    : IF expression '{' statement '}' (ELSE IF expression '{' statement '}')* (ELSE '{' statement '}')?
    ;

plugin_list
    : (plugin | COMMENT)*
    ;

plugin
    : IDENTIFIER entries
//    : plugin_name entries
    ;

entries
    : '{' (pair | COMMENT)* '}'
    ;

// entries
//    : '{' pair (','? pair)* '}'
//    | '{' '}'
//    ;

pair
    : IDENTIFIER '=' value
    ;

array
    : '[' value (',' value)* ']'
    | '[' ']'
    ;

value
//    : NUMBER
    : DECIMAL
    | QUOTED_STRING
//   | entries
    | array
    | 'true'
    | 'false'
    | 'null'
    ;

COMMENT
    : '#' ~( '\r' | '\n' )*
    ;

QUOTED_STRING
    : '\'' STRING_RAW ? '\''
    | '"' STRING_RAW ? '"'
    ;

fragment STRING_RAW
    : [a-zA-Z0-9,:_]+
    ;

fragment INT
    : '0' | [1-9] [0-9]*
    ;

WS
    : [ \t\n\r]+ -> skip
    ;
