grammar BoolExpr;


parse
 : expression EOF
 ;

expression
 : LPAREN expression RPAREN                       #parenExpression
 | NOT expression                                 #notExpression
 | left=expression op=comparator right=expression #comparatorExpression
 | left=expression op=binary right=expression     #binaryExpression
 | bool                                           #boolExpression
 | IDENTIFIER                                     #identifierExpression
 | DECIMAL                                        #decimalExpression
 | FIELD_REFERENCE                                #fieldReferenceExpression
 ;

comparator
 : GT | GE | LT | LE | EQ
 ;

binary
 : AND | OR
 ;

bool
 : TRUE | FALSE
 ;

IF : 'if';
ELSE : 'else';

AND        : 'AND' ;
OR         : 'OR' ;
NOT        : 'NOT';
TRUE       : 'true' ;
FALSE      : 'false' ;
GT         : '>' ;
GE         : '>=' ;
LT         : '<' ;
LE         : '<=' ;
EQ         : '==' ;
LPAREN     : '(' ;
RPAREN     : ')' ;
// integer and float point number
DECIMAL    : '-'? [0-9]+ ( '.' [0-9]+ )? ;
// identifier that represents string starts with [a-zA-Z_] (so we can separate DECIMAL with IDENTIFIER),
// and support such format org.apache.spark
IDENTIFIER : [a-zA-Z_] [a-zA-Z_0-9-]* ('.' [a-zA-Z_0-9-]+)*;
// field reference, example: ${var}
FIELD_REFERENCE : '${' [a-zA-Z_0-9]+ ('.' [a-zA-Z_0-9]+)* '}';

WS         : [ \r\t\u000C\n]+ -> skip;