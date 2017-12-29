grammar Config;

// [done] STRING lexer rule定义过于简单，不满足需求, 包括其中包含Quote(',")的String
// [done] nested bool expression中的字段引用
// [done] nested bool expression
// [done] Filter, Output 允许if else, 包含nested if..else
// [done] 允许key不包含双引号
// [done] 允许多行配置没有"，"分割
// [done] 允许plugin中不包含任何配置

// notes: lexer rule vs parser rule
// notes: don't let two lexer rule match the same token

import BoolExpr;

config
    : COMMENT* 'spark' sparkBlock COMMENT* 'input' inputBlock COMMENT* 'filter' filterBlock COMMENT* 'output' outputBlock COMMENT* EOF
    ;

sparkBlock
    : entries
    ;

inputBlock
    : '{' (plugin | COMMENT)* '}'
    ;

filterBlock
    : '{' statement* '}'
    ;

outputBlock
    : '{' statement* '}'
    ;

statement
    : plugin
    | ifStatement
    | COMMENT
    ;

ifStatement
    : IF expression '{' statement* '}' (ELSE IF expression '{' statement* '}')* (ELSE '{' statement* '}')?
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
    : DECIMAL
    | QUOTED_STRING
//   | entries
    | array
    | TRUE
    | FALSE
    | NULL
    ;

COMMENT
    : ('#' | '//') ~( '\r' | '\n' )* -> skip
    ;

// double and single quoted string support
fragment BSLASH : '\\';
fragment DQUOTE : '"';
fragment SQUOTE : '\'';
fragment DQ_STRING_ESC : BSLASH ["\\/bfnrt] ;
fragment SQ_STRING_ESC : BSLASH ['\\/bfnrt] ;
fragment DQ_STRING : DQUOTE (DQ_STRING_ESC | ~["\\])* DQUOTE ;
fragment SQ_STRING : SQUOTE (SQ_STRING_ESC | ~['\\])* SQUOTE ;
QUOTED_STRING : DQ_STRING | SQ_STRING ;

NULL
    : 'null'
    ;

WS
    : [ \t\n\r]+ -> skip
    ;

