grammar PluginDoc;

// TODO: udfs
// TOOD: all todos

// 内容类型: string, url, quote_string, code, markdown
// 能够写原始的markdown，能够指向链接，能够quote部分文字
// 或者开发者可以在生成的markdown doc上，修改markdown
// pluginUDF 有可能会有多个

// @waterdropPlugin
// @pluginGroup input | filter | output

// @pluginName
// @pluginDesc

// @pluginAuthor
// @pluginHomepage
// @pluginVersion

// @pluginOption type name(required default_value) description
// @pluginOptionsExample

// @pluginUDF
// @pluginUDFName
// @pluginUDFDesc
// @pluginUDFOptions order name desc type required default_value return_value_type
// @pluginUDFExample

waterdropPlugin
    : WaterdropPlugin pluginBlock (udfBlock)*
    ;

pluginBlock
    : (definition)+
    ;

definition
    : pluginGroup
    | pluginName
    | pluginDesc
    | pluginAuthor
    | pluginHomepage
    | pluginVersion
    | pluginOption
    ;

pluginGroup
    : PluginGroup (INPUT | FILTER | OUTPUT)
    ;

pluginName
    : PluginName IDENTIFIER
    ;

pluginDesc
    : PluginDesc (IDENTIFIER | TEXT)
    ;

pluginAuthor
    : PluginAuthor (IDENTIFIER | TEXT)
    ;

pluginHomepage
    : PluginHomepage URL
    ;

pluginVersion
    : PluginVersion VERSION_NUMBER
    ;

pluginOption
    : PluginOption optionType optionName (optionDesc)?
    ;

optionType
    : NUMBER | STRING | ARRAY | BOOLEAN | NULL
    ;

optionName
    : IDENTIFIER
    ;

optionDesc
    : 'desc' | 'desc2'// TODO
    ;

udfBlock
    : 'tet'
    ;

WaterdropPlugin : '@waterdropPlugin';
PluginGroup : '@pluginGroup';

PluginName : '@pluginName';
PluginDesc : '@pluginDesc';

PluginAuthor : '@pluginAuthor';
PluginHomepage : '@pluginHomepage';
PluginVersion : '@pluginVersion';

PluginOption : '@pluginOption';
PluginOptionsExample : '@pluginOptionsExample';

PluginUDF : '@pluginUDF';
PluginUDFName : '@pluginUDFName';
PluginUDFDesc : '@pluginUDFDesc';
PluginUDFOptions : '@pluginUDFOptions';
PluginUDFExample : '@pluginUDFExample';

// pluginGroup
INPUT : 'input';
FILTER : 'filter';
OUTPUT : 'output';

// optionType
NUMBER : 'number';
STRING : 'string';
ARRAY : 'array';
BOOLEAN : 'boolean';
NULL : 'null';

VERSION_NUMBER : [0-9]+ '.' [0-9]+ '.' + [0-9]+;

URL : ('http' 's'? '://')? URL_VALID_CHARS ('.' URL_VALID_CHARS)+ ('/' | URL_PATH_FRAGMENT)* ('?' URL_PARAMS)?;

fragment URL_PATH_FRAGMENT
    : '/' URL_VALID_CHARS
    ;

fragment URL_PARAMS
    : URL_VALID_CHARS '=' URL_VALID_CHARS ('&' URL_VALID_CHARS '=' URL_VALID_CHARS)*
    ;

fragment URL_VALID_CHARS
    : [0-9a-z_-]+
    ;

// IDENTIFIER should be placed before TEXT to be matched first
IDENTIFIER : [a-zA-Z_] [a-zA-Z_0-9]* ('.' [a-zA-Z_0-9]+)*;

TEXT
    : ~( ' ' | '\n' | '\t' )+
    ;

WS
    : [ \t\n\r]+ -> skip
    ;