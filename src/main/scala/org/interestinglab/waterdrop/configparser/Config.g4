grammar Config;

// nested bool expression
// STRING lexer rule定义过于简单，不满足需求, 包括其中包含Quote(',")的String
// [done] Filter, Output 允许if else
// [done] if 中的逻辑运算，值引用
// [done]允许key不包含双引号
// [done] 允许多行配置没有"，"分割
// [done] 允许plugin中不包含任何配置

// notes: lexer rule vs parser rule

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
    : (plugin | if_block | COMMENT)*
    ;

plugin_list
    : (plugin | COMMENT)*
    ;

plugin
    : KEY entries
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

entries
    : '{' (pair | COMMENT)* '}'
    ;

// entries
//    : '{' pair (','? pair)* '}'
//    | '{' '}'
//    ;

pair
    : KEY '=' value
    ;

array
    : '[' value (',' value)* ']'
    | '[' ']'
    ;

value
    : NUMBER
    | QUOTED_STRING
//   | entries
    | array
    | 'true'
    | 'false'
    | 'null'
    ;

NUMBER
    : '-'? INT // Integer
    | '-'? INT '.' INT // float
    ;

KEY
    : [a-zA-Z0-9_]+
    ;

// if you put NUMBER after COMMENT, number will not be recognized
COMMENT
    : '#' ~( '\r' | '\n' )*
    ;

QUOTED_STRING
    : '\'' STRING_RAW ? '\''
    | '"' STRING_RAW ? '"'
    ;

//STRING
//    : STRING_RAW
//    ;

fragment STRING_RAW
    : [a-zA-Z0-9,:_]+
    ;

fragment INT
    : '0' | [1-9] [0-9]*
    ;

OPERATOR
    : '>' | '>=' | '<' | '<=' | '=' | '!='
    ;

WS
    : [ \t\n\r]+ -> skip
    ;
