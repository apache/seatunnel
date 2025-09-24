# SQL函数

> SQL函数转换插件功能

## 字符串函数

### ASCII

```ASCII(string) -> INT```

返回字符串中第一个字符的ASCII值。

示例:

ASCII('Hi')

### BIT_LENGTH

```BIT_LENGTH(bytes) -> LONG```

返回二进制字符串中的位数。

示例:

BIT_LENGTH(NAME)

### CHAR_LENGTH / LENGTH

```CHAR_LENGTH | LENGTH (string) -> LONG```

这个方法返回一个字符串中字符的数量。

示例:

CHAR_LENGTH(NAME)

### OCTET_LENGTH

```OCTET_LENGTH(bytes) -> LONG```

返回二进制字符串中字节的数量。

示例:

OCTET_LENGTH(NAME)

### CHAR / CHR

```CHAR | CHR (int) -> STRING```

返回表示ASCII值的字符。

示例:

CHAR(65)

### CONCAT

```CONCAT(string, string[, string ...] ) -> STRING```

组合字符串。与运算符 `||` 不同，**NULL** 参数会被忽略，不会导致结果变为 **NULL**。如果所有参数都是 NULL，则结果是一个空字符串。

示例:

CONCAT(NAME, '_')

### CONCAT_WS

```CONCAT_WS(separatorString, string, string[, string ...] ) -> STRING```

使用分隔符组合字符串。如果分隔符为 **NULL**，则会被视为空字符串。其他 **NULL** 参数会被忽略。剩余的 **非NULL** 参数（如果有）将用指定的分隔符连接起来。如果没有剩余参数，则结果是一个空字符串。

示例:

CONCAT_WS(',', NAME, '_')

### HEXTORAW

```HEXTORAW(string) -> STRING```

将字符串的十六进制表示转换为字符串。每个字符串字符使用4个十六进制字符。

示例:

HEXTORAW(DATA)

### RAWTOHEX

```RAWTOHEX(string | bytes) -> STRING```

将字符串或字节转换为十六进制表示。每个字符串字符使用4个十六进制字符。

示例:

RAWTOHEX(DATA)

### INSERT

```INSERT(originalString, startInt, lengthInt, addString) -> STRING```

在原始字符串的指定起始位置插入额外的字符串。长度参数指定在原始字符串的起始位置删除的字符数。

示例:

INSERT(NAME, 1, 1, ' ')

### LOWER / LCASE

```LOWER | LCASE (string) -> STRING```

将字符串转换为小写形式。

示例:

LOWER(NAME)

### UPPER / UCASE

```UPPER | UCASE (string) -> STRING```

将字符串转换为大写形式。

示例:

UPPER(NAME)

### LEFT

```LEFT(string, int) -> STRING```

返回最左边的一定数量的字符。

示例:

LEFT(NAME, 3)

### RIGHT

```RIGHT(string, int) -> STRING```

返回最右边的一定数量的字符。

示例:

RIGHT(NAME, 3)

### LOCATE / INSTR / POSITION

```LOCATE(searchString, string[, startInt]) -> INT```

```INSTR(string, searchString[, startInt]) -> INT```

```POSITION(searchString, string) -> INT```

返回字符串中搜索字符串的位置。如果使用了起始位置参数，则忽略它之前的字符。如果位置参数是负数，则返回最右边的位置。如果未找到搜索字符串，则返回 0。请注意，即使参数不区分大小写，此函数也区分大小写。

示例:

LOCATE('.', NAME)

### LPAD

```LPAD(string ,int[, string]) -> STRING```

将字符串左侧填充到指定的长度。如果长度比字符串短，则字符串将在末尾被截断。如果未设置填充字符串，则使用空格填充。

示例:

LPAD(AMOUNT, 10, '*')

### RPAD

```RPAD(string, int[, string]) -> STRING```

将字符串右侧填充到指定的长度。如果长度比字符串短，则字符串将被截断。如果未设置填充字符串，则使用空格填充。

示例:

RPAD(TEXT, 10, '-')

### LTRIM

```LTRIM(string[, characterToTrimString]) -> STRING```

移除字符串中所有前导空格或其他指定的字符。

示例:

LTRIM(NAME)

### RTRIM

```RTRIM(string[, characterToTrimString]) -> STRING```

移除字符串中所有尾随空格或其他指定的字符。

示例:

RTRIM(NAME)

### TRIM

```TRIM(string[, characterToTrimString]) -> STRING```

移除字符串中所有前导空格和尾随空格或其他指定的字符。

示例:

TRIM(NAME)

### REGEXP_REPLACE

```REGEXP_REPLACE(inputString, regexString, replacementString[, flagsString]) -> STRING```

替换与正则表达式匹配的每个子字符串。详情请参阅 Java String.replaceAll() 方法。如果任何参数为 null（除了可选的 flagsString 参数），则结果为 null。

标志值限于 'i'、'c'、'n'、'm'。其他符号会引发异常。可以在一个 flagsString 参数中使用多个符号（例如 'im'）。后面的标志会覆盖前面的标志，例如 'ic' 等同于区分大小写匹配 'c'。

'i' 启用不区分大小写匹配（Pattern.CASE_INSENSITIVE）

'c' 禁用不区分大小写匹配（Pattern.CASE_INSENSITIVE）

'n' 允许句点匹配换行符（Pattern.DOTALL）

'm' 启用多行模式（Pattern.MULTILINE）

示例:

REGEXP_REPLACE('Hello    World', ' +', ' ')
REGEXP_REPLACE('Hello WWWWorld', 'w+', 'W', 'i')

### REGEXP_LIKE

```REGEXP_LIKE(inputString, regexString[, flagsString]) -> BOOLEAN```

将字符串与正则表达式匹配。详情请参阅 Java Matcher.find() 方法。如果任何参数为 null（除了可选的 flagsString 参数），则结果为 null。

标志值限于 'i'、'c'、'n'、'm'。其他符号会引发异常。可以在一个 flagsString 参数中使用多个符号（例如 'im'）。后面的标志会覆盖前面的标志，例如 'ic' 等同于区分大小写匹配 'c'。

'i' 启用不区分大小写匹配（Pattern.CASE_INSENSITIVE）

'c' 禁用不区分大小写匹配（Pattern.CASE_INSENSITIVE）

'n' 允许句点匹配换行符（Pattern.DOTALL）

'm' 启用多行模式（Pattern.MULTILINE）

示例:

REGEXP_LIKE('Hello    World', '[A-Z ]*', 'i')

### REGEXP_SUBSTR

```REGEXP_SUBSTR(inputString, regexString[, positionInt, occurrenceInt, flagsString, groupInt]) -> STRING```

将字符串与正则表达式匹配，并返回匹配的子字符串。详情请参阅 java.util.regex.Pattern 和相关功能。

参数 position 指定匹配应该从 inputString 的哪里开始。Occurrence 指示在 inputString 中搜索 pattern 的哪个出现。

标志值限于 'i'、'c'、'n'、'm'。其他符号会引发异常。可以在一个 flagsString 参数中使用多个符号（例如 'im'）。后面的标志会覆盖前面的标志，例如 'ic' 等同于区分大小写匹配 'c'。

'i' 启用不区分大小写匹配（Pattern.CASE_INSENSITIVE）

'c' 禁用不区分大小写匹配（Pattern.CASE_INSENSITIVE）

'n' 允许句点匹配换行符（Pattern.DOTALL）

'm' 启用多行模式（Pattern.MULTILINE）

如果模式具有组，则可以使用 group 参数指定要返回的组。

示例:

REGEXP_SUBSTR('2020-10-01', '\d{4}')
REGEXP_SUBSTR('2020-10-01', '(\d{4})-(\d{2})-(\d{2})', 1, 1, NULL, 2)

### REPEAT

```REPEAT(string, int) -> STRING```

将字符串按指定次数重复后返回。

示例:

REPEAT(NAME || ' ', 10)

### REPLACE

```REPLACE(string, searchString[, replacementString]) -> STRING```

在文本中替换所有出现的搜索字符串为另一个字符串。如果没有指定替换字符串，则从原始字符串中移除搜索字符串。如果任何参数为 null，则结果为 null。

示例:

REPLACE(NAME, ' ')


### SPLIT

```SPLIT(string, delimiterString) -> ARRAY<STRING>```

将字符串切分成数组。

示例:

select SPLIT(test,';') as arrays

### MURMUR64

```MURMUR64(string) -> LONG```

计算输入字符串的 MurmurHash 128 哈希值，并返回低 64 位作为长整型值。MurmurHash 是一种非加密哈希函数，适用于一般的基于哈希的查找。此方法返回一个长整型值，如果输入参数为 null，则返回 null。

示例:

MURMUR64('hello world')
MURMUR64(NAME)

### SOUNDEX

```SOUNDEX(string) -> STRING```

表示字符串发音。此方法返回一个字符串，如果参数为 null，则返回 null。有关更多信息，请参阅 https://en.wikipedia.org/wiki/Soundex 。

示例:

SOUNDEX(NAME)

### SPACE

```SPACE(int) -> STRING```

返回由一定数量的空格组成的字符串。

示例:

SPACE(80)

### SUBSTRING / SUBSTR

```SUBSTRING | SUBSTR (string, startInt[, lengthInt ]) -> STRING```

返回从指定位置开始的字符串的子串。如果起始索引为负数，则相对于字符串的末尾计算起始索引。长度是可选的。

示例:

CALL SUBSTRING('[Hello]', 2);
CALL SUBSTRING('hour', 3, 2);

### TO_CHAR

```TO_CHAR(value[, formatString]) -> STRING```

Oracle 兼容的 TO_CHAR 函数可用于格式化时间戳、数字或文本。

示例:

CALL TO_CHAR(SYS_TIME, 'yyyy-MM-dd HH:mm:ss')

### TRANSLATE

```TRANSLATE(value, searchString, replacementString) -> STRING```

Oracle 兼容的 TRANSLATE 函数用于将字符串中的一系列字符替换为另一组字符。

示例:

CALL TRANSLATE('Hello world', 'eo', 'EO')

## Numeric Functions

### ABS

```ABS(numeric) -> NUMERIC (same type)```

返回指定值的绝对值。返回的值与参数的数据类型相同。

请注意，TINYINT、SMALLINT、INT 和 BIGINT 数据类型无法表示它们的最小负值的绝对值，因为它们的负值比正值多。例如，对于 INT 数据类型，允许的值范围是从 -2147483648 到 2147483647。ABS(-2147483648) 应该是 2147483648，但是这个值对于这个数据类型是不允许的。这会导致异常。为了避免这种情况，请将此函数的参数转换为更高的数据类型。

示例:

ABS(I)

### ACOS

```ACOS(numeric) -> DOUBLE```

计算反余弦值。另请参阅 Java Math.acos。

示例:

ACOS(D)

### ARRAY_MAX

```ARRAY_MAX(ARRAY) -> type(array element)```

MAX 函数返回表达式的最大值。

示例:

ARRAY_MAX(I)

### ARRAY_MIN

```ARRAY_MIN(ARRAY) -> type(array element)```

MIN 函数返回表达式的最小值。

示例:

ARRAY_MIN(I)


### ASIN

```ASIN(numeric) -> DOUBLE```

计算反正弦值。另请参阅 Java Math.asin。

示例:

ASIN(D)

### ATAN

```ATAN(numeric) -> DOUBLE```

计算反正切值。另请参阅 Java Math.atan。

示例:

ATAN(D)

### COS

```COS(numeric) -> DOUBLE```

计算三角余弦值。另请参阅 Java Math.cos。

示例:

COS(ANGLE)

### COSH

```COSH(numeric) -> DOUBLE```

计算双曲余弦值。另请参阅 Java Math.cosh。

示例:

COSH(X)

### COT

```COT(numeric) -> DOUBLE```

计算三角余切值（1/TAN(角度)）。另请参阅 Java Math.* 函数。

示例:

COT(ANGLE)

### SIN

```SIN(numeric) -> DOUBLE```

计算三角正弦值。另请参阅 Java Math.sin。

示例:

SIN(ANGLE)

### SINH

```SINH(numeric) -> DOUBLE```

计算双曲正弦值。另请参阅 Java Math.sinh。

示例:

SINH(ANGLE)

### TAN

```TAN(numeric) -> DOUBLE```

计算三角正切值。另请参阅 Java Math.tan。

示例:

TAN(ANGLE)

### TANH

```TANH(numeric) -> DOUBLE```

计算双曲正切值。另请参阅 Java Math.tanh。

示例:

TANH(X)

### MOD

```MOD(dividendNumeric, divisorNumeric ) -> type(divisorNumeric)```

取模运算表达式。

结果与除数的类型相同。如果任一参数为 NULL，则结果为 NULL。如果除数为 0，则会引发异常。结果与被除数的符号相同，或者等于 0。

通常情况下，参数应具有标度 0，但 H2 并不要求。

示例:

MOD(A, B)

### CEIL / CEILING

```CEIL | CEILING (numeric) -> NUMERIC (same type, scale 0)```

返回大于或等于参数的最小整数值。该方法返回与参数相同类型的值，但标度设置为 0，并且如果适用，则调整精度。

示例:

CEIL(A)

### EXP

```EXP(numeric) -> DOUBLE```

请参阅 Java Math.exp。

示例:

EXP(A)

### FLOOR

```FLOOR(numeric) -> NUMERIC (same type, scale 0)```

返回小于或等于参数的最大整数值。该方法返回与参数相同类型的值，但标度设置为 0，并且如果适用，则调整精度。

示例:

FLOOR(A)

### LN

```LN(numeric) -> DOUBLE```

计算自然对数（以 e 为底）的双精度浮点数值。参数必须是一个正数值。

示例:

LN(A)

### LOG

```LOG(baseNumeric, numeric) -> DOUBLE```

计算以指定底数的对数，返回一个双精度浮点数。参数和底数必须是正数值。底数不能等于1。

默认底数是 e（自然对数），在 PostgreSQL 模式下，默认底数是 10。在 MSSQLServer 模式下，可选的底数在参数之后指定。

LOG 函数的单参数变体已被弃用，请使用 LN 或 LOG10 替代。

示例:

LOG(2, A)

### LOG10

```LOG10(numeric) -> DOUBLE```

计算以 10 为底的对数，返回一个双精度浮点数。参数必须是一个正数值。

示例:

LOG10(A)

### RADIANS

```RADIANS(numeric) -> DOUBLE```

请参阅 Java Math.toRadians。

示例:

RADIANS(A)

### SQRT

```SQRT(numeric) -> DOUBLE```

请参阅 Java Math.sqrt。

示例:

SQRT(A)

### PI

```PI() -> DOUBLE```

请参阅 Java Math.PI。

示例:

PI()

### POWER

```POWER(numeric, numeric) -> DOUBLE```

请参阅 Java Math.pow。

示例:

POWER(A, B)

### RAND / RANDOM

```RAND | RANDOM([ int ]) -> DOUBLE```

如果不带参数调用该函数，则返回下一个伪随机数。如果带有参数调用，则将会给该会话的随机数生成器设定种子。该方法返回一个介于 0（包括）和 1（不包括）之间的双精度浮点数。

示例:

RAND()

### ROUND

```ROUND(numeric[, digitsInt]) -> NUMERIC (same type)```

四舍五入到指定的小数位数。该方法返回与参数相同类型的值，但如果适用，则调整精度和标度。

示例:

ROUND(N, 2)

### SIGN

```SIGN(numeric) -> INT```

如果值小于 0，则返回 -1；如果值为零或 NaN，则返回 0；否则返回 1。

示例:

SIGN(N)

### TRUNC

```TRUNC | TRUNCATE(numeric[, digitsInt]) -> NUMERIC (same type)```

当指定了一个数值参数时，将其截断为指定的数字位数（接近0的下一个值），并返回与参数相同类型的值，但如果适用，则调整精度和标度。

示例:

TRUNC(N, 2)

### TRIM_SCALE

```TRIM_SCALE(numeric) -> NUMERIC (same type)```

通过删除尾数部分的零来降低值的刻度（小数位数），并调整小数位数。

示例:

TRIM_SCALE(N)

## Time and Date Functions

### CURRENT_DATE

```CURRENT_DATE [()] -> DATE```

返回当前日期。

这些函数在事务（默认）或命令内部返回相同的值，具体取决于数据库模式。

示例:

CURRENT_DATE

### CURRENT_TIME

```CURRENT_TIME [()] -> TIME```

返回带有系统时区的当前时间。实际可用的最大精度取决于操作系统和 JVM，可以是 3（毫秒）或更高。在 Java 9 之前不支持更高的精度。

示例:

CURRENT_TIME

### CURRENT_TIMESTAMP / NOW

```CURRENT_TIMESTAMP[()] | NOW() -> TIMESTAMP```

返回带有系统时区的当前时间戳。实际可用的最大精度取决于操作系统和 JVM，可以是 3（毫秒）或更高。在 Java 9 之前不支持更高的精度。

示例:

CURRENT_TIMESTAMP

### DATEADD / TIMESTAMPADD

```DATEADD | TIMESTAMPADD(dateAndTime, addIntLong, datetimeFieldString) -> type(dateAndTime)```

将单位添加到日期时间值中。datetimeFieldString 表示单位。使用负值来减去单位。当操作毫秒、微秒或纳秒时，addIntLong 可能是一个 long 值，否则其范围被限制为 int。如果单位与指定值兼容，则此方法返回与指定值相同类型的值。如果指定的字段是 HOUR、MINUTE、SECOND、MILLISECOND 等，而值是 DATE 值，DATEADD 返回组合的 TIMESTAMP。对于 TIME 值，不允许使用 DAY、MONTH、YEAR、WEEK 等字段。

示例:

DATEADD(CREATED, 1, 'MONTH')

### DATEDIFF

```DATEDIFF(aDateAndTime, bDateAndTime, datetimeFieldString) -> LONG```

返回两个日期时间值之间跨越的单位边界数。datetimeField 表示单位。

示例:

DATEDIFF(T1.CREATED, T2.CREATED, 'MONTH')

### DATE_TRUNC

```DATE_TRUNC (dateAndTime, datetimeFieldString) -> dateAndTime (same type)```

将指定的日期时间值截断到指定的字段。

示例:

DATE_TRUNC(CREATED, 'DAY');

### DAYNAME

```DAYNAME(dateAndTime) -> STRING```

返回星期几的名称（英文）。

示例:

DAYNAME(CREATED)

### DAY_OF_MONTH

```DAY_OF_MONTH(dateAndTime) -> INT```

返回月份中的日期（1-31）。

示例:

DAY_OF_MONTH(CREATED)

### DAY_OF_WEEK

```DAY_OF_WEEK(dateAndTime) -> INT```

返回星期几的数值（1-7）（星期一至星期日），根据本地化设置。

示例:

DAY_OF_WEEK(CREATED)

### DAY_OF_YEAR

```DAY_OF_YEAR(dateAndTime) -> INT```

返回一年中的日期（1-366）。

示例:

DAY_OF_YEAR(CREATED)

### EXTRACT

```EXTRACT ( datetimeField FROM dateAndTime) -> INT | NUMERIC```

从日期/时间值中返回特定时间单位的值。该方法对于 EPOCH 字段返回一个数值，对于其他字段返回一个整数。

EXTRACT函数支持以下字段名：

- `CENTURY`：世纪；对于interval值，年份字段除以100
- `DAY`：月份中的日期（1-31）；对于interval值，表示天数
- `DECADE`：年份字段除以10
- `DOW` 或 `DAYOFWEEK`：星期几，从周日（0）到周六（6）
- `DOY`：一年中的第几天（1-365/366）
- `EPOCH`：对于timestamp值，表示自1970-01-01 00:00:00以来的秒数；对于interval值，表示总秒数
- `HOUR`：小时字段（0-23）
- `ISODOW`：星期几，从周一（1）到周日（7），符合ISO 8601标准
- `ISOYEAR`：ISO 8601周编号年份
- `MICROSECONDS`：秒字段（包括小数部分）乘以1,000,000
- `MILLENNIUM`：千年；对于interval值，年份字段除以1000
- `MILLISECONDS`：秒字段（包括小数部分）乘以1,000
- `MINUTE`：分钟字段（0-59）
- `MONTH`：年份中的月份（1-12）；对于interval值，月份对12取模（0-11）
- `QUARTER`：日期所在的季度（1-4）
- `SECOND`：秒字段，包括任何小数秒
- `WEEK`：ISO 8601周编号年份中的周数（1-53）
- `YEAR`：年份字段

EXTRACT函数支持以下四种DateTime字面量类型：

- `DATE`：用于从日期字面量中提取日期组件
  ```sql
  EXTRACT(YEAR FROM DATE '2025-05-21')
  ```

- `TIME`：用于从时间字面量中提取时间组件
  ```sql
  EXTRACT(HOUR FROM TIME '17:57:40')
  ```

- `TIMESTAMP`：用于从时间戳字面量中提取日期和时间组件
  ```sql
  EXTRACT(YEAR FROM TIMESTAMP '2025-05-21T17:57:40')
  ```

- `TIMESTAMP WITH TIMEZONE`：用于从带时区的时间戳字面量中提取组件
  ```sql
  EXTRACT(HOUR FROM TIMESTAMPTZ '2025-05-21T17:57:40+08:00')
  ```

示例：

```sql
EXTRACT(YEAR FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(HOUR FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(DOW FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(YEAR FROM eventTime)
EXTRACT(HOUR FROM eventTime)
EXTRACT(DOW FROM eventTime)
```

### FORMATDATETIME

```FORMATDATETIME (dateAndTime, formatString) -> STRING```

将日期、时间或时间戳格式化为字符串。最重要的格式字符包括：y（年）、M（月）、d（日）、H（时）、m（分）、s（秒）。有关格式的详细信息，请参阅 java.time.format.DateTimeFormatter。



示例:

CALL FORMATDATETIME(CREATED, 'yyyy-MM-dd HH:mm:ss')

### HOUR

```HOUR(dateAndTime) -> INT```

从日期/时间值中返回小时（0-23）。

示例:

HOUR(CREATED)

### MINUTE

```MINUTE(dateAndTime) -> INT```

从日期/时间值中返回分钟（0-59）。

该函数已经被弃用，请使用 EXTRACT 替代。

示例:

MINUTE(CREATED)

### MONTH

```MONTH(dateAndTime) -> INT```

从日期/时间值中返回月份（1-12）。

该函数已经被弃用，请使用 EXTRACT 替代。

示例:

MONTH(CREATED)

### MONTHNAME

```MONTHNAME(dateAndTime) -> STRING```

返回月份的名称（英文）。

示例:

MONTHNAME(CREATED)

### PARSEDATETIME / TO_DATE

```PARSEDATETIME | TO_DATE(string, formatString) -> TIMESTAMP```

解析一个字符串并返回一个 TIMESTAMP WITH TIME ZONE 值。最重要的格式字符包括：y（年）、M（月）、d（日）、H（时）、m（分）、s（秒）。有关格式的详细信息，请参阅 java.time.format.DateTimeFormatter。

示例:

CALL PARSEDATETIME('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')
CALL TO_DATE('2021-04-08T13:34:45','yyyy-MM-dd''T''HH:mm:ss')
注意SQL函数中的`'`填写时需要转义为`''`。

### QUARTER

```QUARTER(dateAndTime) -> INT```

从日期/时间值中返回季度（1-4）。

示例:

QUARTER(CREATED)

### SECOND

```SECOND(dateAndTime) -> INT```

从日期/时间值中返回秒数（0-59）。

该函数已经被弃用，请使用 EXTRACT 替代。

示例:

SECOND(CREATED)

### WEEK

```WEEK(dateAndTime) -> INT```

返回日期/时间值中的周数（1-53）。

该函数使用当前系统的区域设置。

示例:

WEEK(CREATED)

### YEAR

```YEAR(dateAndTime) -> INT```

返回日期/时间值中的年份。

示例:

YEAR(CREATED)

### FROM_UNIXTIME

```FROM_UNIXTIME (unixtime, formatString,timeZone) -> STRING```

将从 UNIX 纪元（1970-01-01 00:00:00 UTC）开始的秒数转换为表示该时刻时间戳的字符串。

最重要的格式字符包括：y（年）、M（月）、d（日）、H（时）、m（分）、s（秒）。有关格式的详细信息，请参阅 `java.time.format.DateTimeFormatter`。

`timeZone` 是可选的，默认值为系统的时区。`timezone` 的值可以是一个 `UTC+ 时区偏移`，例如，`UTC+8` 表示亚洲/上海时区，请参阅 https://en.wikipedia.org/wiki/List_of_tz_database_time_zones 。

示例:

// 使用默认时区

CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss')

or

// 使用指定时区

CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss','UTC+6')


### AT TIME ZONE

```dateAndTime AT TIME ZONE 'timeZone' -> TIMESTAMP_TZ```

转换一个时间戳值为指定时区的带时区时间戳值。

`timezone` 的值可以是一个 `UTC+ 时区偏移`，例如，`+08:00` 表示亚洲/上海时区，请参阅 https://en.wikipedia.org/wiki/List_of_tz_database_time_zones 。

Example:

local_date_time AT TIME ZONE '+09:00'

offset_date_time AT TIME ZONE 'Pacific/Honolulu'

## System Functions

### CAST

```CAST(value as dataType) -> dataType```

将一个值转换为另一个数据类型。

支持的数据类型有：STRING | VARCHAR，TINYINT，SMALLINT，INT | INTEGER，LONG | BIGINT，BYTE，FLOAT，DOUBLE，DECIMAL(p,s)，TIMESTAMP，DATE，TIME，BYTES

示例:

CAST(NAME AS INT)

CAST(FLAG AS BOOLEAN)

注意：将值转换为布尔数据类型时，遵循以下规则：

1.  如果值可以被解释为布尔字符串（'true' 或 'false'），则返回相应的布尔值。
2.  如果值可以被解释为数值（1 或 0），则对于 1 返回 true，对于 0 返回 false。
3.  如果值无法根据以上规则进行解释，则抛出 TransformException 异常。

### TRY_CAST

```TRY_CAST(value as dataType) -> dataType | NULL```

该函数类似于 CAST，但当转换失败时，它返回 NULL 而不是抛出异常。

支持的数据类型有：STRING | VARCHAR，TINYINT，SMALLINT，INT | INTEGER，LONG | BIGINT，BYTE，FLOAT，DOUBLE，DECIMAL(p,s)，TIMESTAMP，DATE，TIME，BYTES

示例:

TRY_CAST(NAME AS INT)

### COALESCE

```COALESCE(aValue, bValue [,...]) -> type(of first non-null arg)```

返回第一个非空值。如果后续参数与第一个参数的数据类型不同，则会自动转换为第一个参数的类型。

示例:

COALESCE(A, B, C)

类型转换示例:

```
-- 如果A是字符串类型而B是整数类型
-- 当A为空时，B会被转换为字符串类型
SELECT COALESCE(A, B) as result FROM my_table
```

### IFNULL

```IFNULL(aValue, bValue) -> type(common of args)```

返回第一个非空值。如果后续参数与第一个参数的数据类型不同，则会自动转换为第一个参数的类型。

示例:

IFNULL(A, B)

### NULLIF

```NULLIF(aValue, bValue) -> type(aValue) | NULL```

如果 'a' 等于 'b'，则返回 NULL，否则返回 'a'。

示例:

NULLIF(A, B)

### MULTI_IF

```MULTI_IF(condition1, value1, condition2, value2, ... conditionN, valueN, bValue) -> type(of values)```

返回第一个满足相应条件的值。如果所有条件均为假，则返回最后一个值。

示例:

MULTI_IF(A > 1, 'A', B > 1, 'B', C > 1, 'C', 'D')

### CASE WHEN

```CASE WHEN <condition> THEN <expr> [WHEN ...] [ELSE <expr>] END -> type(of result expressions)```

```
select
  case
    when c_string in ('c_string') then 1
    else 0
  end as c_string_1,
  case
    when c_string not in ('c_string') then 1
    else 0
  end as c_string_0,
  case
    when c_tinyint = 117
    and TO_CHAR(c_boolean) = 'true' then 1
    else 0
  end as c_tinyint_boolean_1,
  case
    when c_tinyint != 117
    and TO_CHAR(c_boolean) = 'true' then 1
    else 0
  end as c_tinyint_boolean_0,
  case
    when c_tinyint != 117
    or TO_CHAR(c_boolean) = 'true' then 1
    else 0
  end as c_tinyint_boolean_or_1,
  case
    when c_int > 1
    and c_bigint > 1
    and c_float > 1
    and c_double > 1
    and c_decimal > 1 then 1
    else 0
  end as c_number_1,
  case
    when c_tinyint <> 117 then 1
    else 0
  end as c_number_0
from
  fake
```

用于确定条件是否有效，并根据不同的判断返回不同的值

示例:

case when c_string in ('c_string') then 1 else 0 end

case when c_string in ('c_string') then true else false end

### UUID

```UUID() -> STRING```

通过java函数生成uuid

示例:

select UUID() as seatunnel_uuid


### ARRAY

```ARRAY<T> array(T, ...) -> ARRAY<T>```
创建一个由可变参数元素组成的数组并返回它。这里，T 可以是“列”或“常量”。

示例:

select ARRAY(1,2,3) as arrays
select ARRAY('c_1',2,3.12) as arrays
select ARRAY(column1,column2,column3) as arrays

注意：目前仅支持string、double、long、int几种类型

### LATERAL VIEW
#### EXPLODE
```EXPLODE(array of T) -> rows(value: T)```  
```OUTER EXPLODE(array of T) -> rows(value: T | NULL)```

用于将数组列展开成多行。它通过对数组应用 EXPLODE 函数，为数组中的每个元素生成一个新行。

EXPLODE：将数组列转换为多行。如果数组为 NULL 或为空，则不生成行。

OUTER EXPLODE：当数组为 NULL 或为空时返回 NULL，确保至少生成一行。

EXPLODE(SPLIT(字段名, 分隔符))：使用指定的分隔符将字符串拆分为数组，然后将其展开为多行。

EXPLODE(ARRAY(值1, 值2, ...))：将自定义数组展开为多行。

示例:
```
SELECT * FROM dual
	LATERAL VIEW EXPLODE ( SPLIT ( NAME, ',' ) ) AS NAME
	LATERAL VIEW EXPLODE ( SPLIT ( pk_id, ';' ) ) AS pk_id
	LATERAL VIEW OUTER EXPLODE ( age ) AS age
	LATERAL VIEW OUTER EXPLODE ( ARRAY(1,1) ) AS num
```

## 向量函数

### VECTOR_DIMS

```VECTOR_DIMS(vector) -> INT```

返回一个INT值，表示向量中的维数（元素）。

示例:

VECTOR_DIMS(vector)

### VECTOR_NORM

```VECTOR_NORM(vector) -> DOUBLE```

计算向量的L2范数（欧几里得范数），表示向量的长度或大小。

示例:

VECTOR_NORM(vector)

### INNER_PRODUCT

```INNER_PRODUCT(vector1, vector2) -> DOUBLE```

计算两个向量的内积（点积），用于测量向量之间的相似性和投影。

示例:

INNER_PRODUCT(vector1, vector2)

### COSINE_DISTANCE

```COSINE_DISTANCE(vector1, vector2) -> DOUBLE```

返回介于 0 和 1 之间的 DOUBLE 值：

0：相同的向量（完全相似）

1：正交向量（完全不同）

示例:

COSINE_DISTANCE(vector1, vector2)

### L1_DISTANCE

```L1_DISTANCE(vector1, vector2) -> DOUBLE```

计算两个向量之间的曼哈顿（L1）距离。

示例:

L1_DISTANCE(vector1, vector2)

### L2_DISTANCE

```L2_DISTANCE(vector1, vector2) -> DOUBLE```

计算两个向量之间的欧几里得（L2）距离。

示例:

L2_DISTANCE(vector1, vector2)

### VECTOR_REDUCE

```VECTOR_REDUCE(vector_field, target_dimension, method)```

通用向量降维函数，支持多种降维方法。

**参数:**
- `vector_field`: 要降维的向量字段 (VECTOR 类型)
- `target_dimension`: 目标维度 (INTEGER，必须小于源维度)
- `method`: 降维方法 (STRING)：
  - **'TRUNCATE'**: 截断法，通过保留前N个元素来缩减向量维度。这是最简单、最快速的降维方法，但可能会丢失被截断维度中的重要信息。
  - **'RANDOM_PROJECTION'**: 随机投影法，使用高斯随机投影和正态分布的随机矩阵。该方法在降维的同时保持向量间的相对距离，遵循Johnson-Lindenstrauss引理。
  - **'SPARSE_RANDOM_PROJECTION'**: 稀疏随机投影法，矩阵元素大多为零（±√3, 0）。比常规随机投影在计算上更高效，同时保持相似的距离保持特性。

**返回值:** 降维后的 VECTOR 类型

**示例:**
```sql
SELECT id, VECTOR_REDUCE(embedding, 256, 'TRUNCATE') as reduced_embedding FROM table
SELECT id, VECTOR_REDUCE(embedding, 128, 'RANDOM_PROJECTION') as reduced_embedding FROM table
SELECT id, VECTOR_REDUCE(embedding, 64, 'SPARSE_RANDOM_PROJECTION') as reduced_embedding FROM table
```

### VECTOR_NORMALIZE

```VECTOR_NORMALIZE(vector_field)```

将向量归一化为单位长度（模长 = 1）。这对于计算余弦相似度很有用。

**参数:**
- `vector_field`: 要归一化的向量字段 (VECTOR 类型)

**返回值:** VECTOR 类型 - 归一化后的向量

**示例:**
```sql
SELECT id, VECTOR_NORMALIZE(embedding) as normalized_embedding FROM table
```