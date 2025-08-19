# SQL函数

> SQL函数转换插件功能

## 字符串函数

### ASCII

```ASCII(string) -> INT```

返回字符串中第一个字符的 ASCII 值。

示例:  
ASCII('Hi')

### BIT_LENGTH

```BIT_LENGTH(bytes) -> LONG```

返回二进制字符串中的位数。

示例:  
BIT_LENGTH(NAME)

### CHAR_LENGTH / LENGTH

```CHAR_LENGTH | LENGTH (string) -> LONG```

返回字符串中的字符数量。

示例:  
CHAR_LENGTH(NAME)

### OCTET_LENGTH

```OCTET_LENGTH(bytes) -> LONG```

返回二进制字符串中的字节数量。

示例:  
OCTET_LENGTH(NAME)

### CHAR / CHR

```CHAR | CHR (int) -> STRING```

返回表示该 ASCII 值的字符。

示例:  
CHAR(65)

### CONCAT

```CONCAT(string, string[, string ...]) -> STRING```

连接字符串。与运算符 `||` 不同，**NULL** 参数会被忽略，不会使结果变为 **NULL**。

示例:  
CONCAT(NAME, '_')

### CONCAT_WS

```CONCAT_WS(separatorString, string, string[, string ...]) -> STRING```

使用分隔符连接字符串。分隔符为 **NULL** 时视为空字符串；其他 **NULL** 参数会被忽略。

示例:  
CONCAT_WS(',', NAME, '_')

### HEXTORAW

```HEXTORAW(string) -> STRING```

将字符串的十六进制表示转换为字符串（每个字符 4 个十六进制字符）。

示例:  
HEXTORAW(DATA)

### RAWTOHEX

```RAWTOHEX(string | bytes) -> STRING```

将字符串或字节转换为十六进制表示（每个字符 4 个十六进制字符）。

示例:  
RAWTOHEX(DATA)

### INSERT

```INSERT(originalString, startInt, lengthInt, addString) -> STRING```

在原始字符串的指定起始位置插入额外的字符串。`lengthInt` 指定需在该位置删除的字符数。

示例:  
INSERT(NAME, 1, 1, ' ')

### LOWER / LCASE

```LOWER | LCASE (string) -> STRING```

将字符串转换为小写。

示例:  
LOWER(NAME)

### UPPER / UCASE

```UPPER | UCASE (string) -> STRING```

将字符串转换为大写。

示例:  
UPPER(NAME)

### LEFT

```LEFT(string, int) -> STRING```

返回最左边的若干字符。

示例:  
LEFT(NAME, 3)

### RIGHT

```RIGHT(string, int) -> STRING```

返回最右边的若干字符。

示例:  
RIGHT(NAME, 3)

### LOCATE / INSTR / POSITION

```LOCATE(searchString, string[, startInt]) -> INT```  
```INSTR(string, searchString[, startInt]) -> INT```  
```POSITION(searchString, string) -> INT```

返回 `searchString` 在 `string` 中的位置；若提供 `startInt`，则忽略其之前的字符；`startInt` 为负表示从右侧开始搜索；未找到返回 0。大小写敏感。

示例:  
LOCATE('.', NAME)

### LPAD

```LPAD(string, int[, string]) -> STRING```

在左侧填充到指定长度。若长度更短，则在末尾截断；未提供填充串则使用空格。

示例:  
LPAD(AMOUNT, 10, '*')

### RPAD

```RPAD(string, int[, string]) -> STRING```

在右侧填充到指定长度。若长度更短，则截断；未提供填充串则使用空格。

示例:  
RPAD(TEXT, 10, '-')

### LTRIM

```LTRIM(string[, characterToTrimString]) -> STRING```

去除前导空格或指定字符。

示例:  
LTRIM(NAME)

### RTRIM

```RTRIM(string[, characterToTrimString]) -> STRING```

去除尾随空格或指定字符。

示例:  
RTRIM(NAME)

### TRIM

```TRIM(string[, characterToTrimString]) -> STRING```

去除前后空格或指定字符。

示例:  
TRIM(NAME)

### REGEXP_REPLACE

```REGEXP_REPLACE(inputString, regexString, replacementString[, flagsString]) -> STRING```

将匹配正则的子串替换为指定内容（参见 Java `String.replaceAll()`）。任一必要参数为 NULL 时返回 NULL。`flagsString` 仅支持：  
`i` 不区分大小写；`c` 取消不区分大小写；`n` 点号匹配换行；`m` 多行模式。可组合使用，如 `im`。

示例:  
REGEXP_REPLACE('Hello    World', ' +', ' ')  
REGEXP_REPLACE('Hello WWWWorld', 'w+', 'W', 'i')

### REGEXP_LIKE

```REGEXP_LIKE(inputString, regexString[, flagsString]) -> BOOLEAN```

判断是否匹配正则（参见 Java `Matcher.find()`）。必要参数为 NULL 时返回 NULL。`flagsString` 同上（`i`/`c`/`n`/`m`）。

示例:  
REGEXP_LIKE('Hello    World', '[A-Z ]*', 'i')

### REGEXP_SUBSTR

```REGEXP_SUBSTR(inputString, regexString[, positionInt, occurrenceInt, flagsString, groupInt]) -> STRING```

返回匹配正则的子串；`positionInt` 为起始位置，`occurrenceInt` 为第几次匹配；可用 `groupInt` 指定返回的分组。`flagsString` 同上。

示例:  
REGEXP_SUBSTR('2020-10-01', '\d{4}')  
REGEXP_SUBSTR('2020-10-01', '(\d{4})-(\d{2})-(\d{2})', 1, 1, NULL, 2)

### REPEAT

```REPEAT(string, int) -> STRING```

返回重复若干次后的字符串。

示例:  
REPEAT(NAME || ' ', 10)

### REPLACE

```REPLACE(string, searchString[, replacementString]) -> STRING```

将所有出现的 `searchString` 替换为 `replacementString`；未提供时将其删除。任一参数为 NULL 返回 NULL。

示例:  
REPLACE(NAME, ' ')

### SPLIT

```SPLIT(string, delimiterString) -> ARRAY<STRING>```

将字符串按分隔符切分为数组。

示例:  
SELECT SPLIT(test, ';') AS arrays

### SOUNDEX

```SOUNDEX(string) -> STRING```

返回表示发音的四字符代码（见维基百科 *Soundex*）。

示例:  
SOUNDEX(NAME)

### SPACE

```SPACE(int) -> STRING```

返回由指定数量空格组成的字符串。

示例:  
SPACE(80)

### SUBSTRING / SUBSTR

```SUBSTRING | SUBSTR (string, startInt[, lengthInt]) -> STRING```

返回从 `startInt` 开始的子串；`startInt` 为负时自右向左计数；`lengthInt` 可选。

示例:  
CALL SUBSTRING('[Hello]', 2);  
CALL SUBSTRING('hour', 3, 2);

### TO_CHAR

```TO_CHAR(value[, formatString]) -> STRING```

Oracle 兼容格式化：时间戳、数字或文本。

示例:  
CALL TO_CHAR(SYS_TIME, 'yyyy-MM-dd HH:mm:ss')

### TRANSLATE

```TRANSLATE(value, searchString, replacementString) -> STRING```

将字符串中的一组字符映射替换为另一组字符。

示例:  
CALL TRANSLATE('Hello world', 'eo', 'EO')

---

## 数值函数

### ABS

```ABS(numeric) -> numeric (same type)```

返回绝对值（与参数同类型）。注意整型最小负值的绝对值可能溢出，应先提升类型。

示例:  
ABS(I)

### ACOS

```ACOS(numeric) -> DOUBLE```

反余弦。

示例:  
ACOS(D)

### ARRAY_MAX

```ARRAY_MAX(array) -> type(array element)```

返回数组中的最大值。

示例:  
ARRAY_MAX(I)

### ARRAY_MIN

```ARRAY_MIN(array) -> type(array element)```

返回数组中的最小值。

示例:  
ARRAY_MIN(I)

### ASIN

```ASIN(numeric) -> DOUBLE```

反正弦。

示例:  
ASIN(D)

### ATAN

```ATAN(numeric) -> DOUBLE```

反正切。

示例:  
ATAN(D)

### COS

```COS(numeric) -> DOUBLE```

三角余弦。

示例:  
COS(ANGLE)

### COSH

```COSH(numeric) -> DOUBLE```

双曲余弦。

示例:  
COSH(X)

### COT

```COT(numeric) -> DOUBLE```

三角余切（1 / TAN(ANGLE)）。

示例:  
COT(ANGLE)

### SIN

```SIN(numeric) -> DOUBLE```

三角正弦。

示例:  
SIN(ANGLE)

### SINH

```SINH(numeric) -> DOUBLE```

双曲正弦。

示例:  
SINH(ANGLE)

### TAN

```TAN(numeric) -> DOUBLE```

三角正切。

示例:  
TAN(ANGLE)

### TANH

```TANH(numeric) -> DOUBLE```

双曲正切。

示例:  
TANH(X)

### MOD

```MOD(dividendNumeric, divisorNumeric) -> type(divisorNumeric)```

取模。任一参数为 NULL 返回 NULL；除数为 0 抛异常；结果与被除数同符号或为 0。

示例:  
MOD(A, B)

### CEIL / CEILING

```CEIL | CEILING (numeric) -> numeric (same type, scale 0)```

向上取整（标度置 0，如适用调整精度）。

示例:  
CEIL(A)

### EXP

```EXP(numeric) -> DOUBLE```

指数（`Math.exp`）。

示例:  
EXP(A)

### FLOOR

```FLOOR(numeric) -> numeric (same type, scale 0)```

向下取整（标度置 0）。

示例:  
FLOOR(A)

### LN

```LN(numeric) -> DOUBLE```

自然对数（底 e）。

示例:  
LN(A)

### LOG

```LOG(baseNumeric, numeric) -> DOUBLE```

指定底对数；参数与底均需为正，底不可为 1。单参数形式已弃用，请用 `LN` 或 `LOG10`。

示例:  
LOG(2, A)

### LOG10

```LOG10(numeric) -> DOUBLE```

以 10 为底的对数。

示例:  
LOG10(A)

### RADIANS

```RADIANS(numeric) -> DOUBLE```

角度转弧度（`Math.toRadians`）。

示例:  
RADIANS(A)

### SQRT

```SQRT(numeric) -> DOUBLE```

平方根（`Math.sqrt`）。

示例:  
SQRT(A)

### PI

```PI() -> DOUBLE```

圆周率（`Math.PI`）。

示例:  
PI()

### POWER

```POWER(numeric, numeric) -> DOUBLE```

乘幂（`Math.pow`）。

示例:  
POWER(A, B)

### RAND / RANDOM

```RAND | RANDOM([int]) -> DOUBLE```

不带参返回区间 [0, 1) 的伪随机数；带整型参数时设置会话随机种子。

示例:  
RAND()

### ROUND

```ROUND(numeric[, digitsInt]) -> numeric (same type)```

按指定位数四舍五入（如适用调整精度/标度）。

示例:  
ROUND(N, 2)

### SIGN

```SIGN(numeric) -> INT```

小于 0 返回 -1；等于 0 或 NaN 返回 0；否则返回 1。

示例:  
SIGN(N)

### TRUNC

```TRUNC | TRUNCATE(numeric[, digitsInt]) -> numeric (same type)```

按指定位数截断（趋近 0）。

示例:  
TRUNC(N, 2)

### TRIM_SCALE

```TRIM_SCALE(numeric) -> STRING```

去除小数部分尾随 0 并降低标度，返回字符串。

示例:  
TRIM_SCALE(N)

---

## 时间与日期函数

### CURRENT_DATE

```CURRENT_DATE -> DATE```

返回当前日期（在一次事务或一次命令内值不变，取决于数据库模式）。

示例:  
CURRENT_DATE

### CURRENT_TIME

```CURRENT_TIME -> TIME```

返回当前时间（含系统时区）。

示例:  
CURRENT_TIME

### CURRENT_TIMESTAMP / NOW

```CURRENT_TIMESTAMP | NOW() -> TIMESTAMP```

返回当前时间戳。

示例:  
CURRENT_TIMESTAMP

### DATEADD / TIMESTAMPADD

```DATEADD | TIMESTAMPADD(dateAndTime, addIntLong, datetimeFieldString) -> dateAndTime (same type)```

为日期时间加上指定单位（负值为相减）。`datetimeFieldString` 表示单位。**注意**：当对 `DATE` 值添加 HOUR/MINUTE/SECOND/MILLISECOND/MICROSECOND/NANOSECOND 等时间字段时，可能返回 `TIMESTAMP`。

示例:  
DATEADD(CREATED, 1, 'MONTH')

### DATEDIFF

```DATEDIFF(aDateAndTime, bDateAndTime, datetimeFieldString) -> LONG```

返回两个日期时间值之间跨越的单位边界数。`datetimeFieldString` 表示单位。

示例:  
DATEDIFF(T1.CREATED, T2.CREATED, 'MONTH')

### DATE_TRUNC

```DATE_TRUNC(dateAndTime, datetimeFieldString) -> dateAndTime (same type)```

将日期时间截断到指定字段。

示例:  
DATE_TRUNC(CREATED, 'DAY');

### DAYNAME

```DAYNAME(dateAndTime) -> STRING```

返回星期名称（英文）。

示例:  
DAYNAME(CREATED)

### DAY_OF_MONTH

```DAY_OF_MONTH(dateAndTime) -> INT```

返回当月第几天（1-31）。

示例:  
DAY_OF_MONTH(CREATED)

### DAY_OF_WEEK

```DAY_OF_WEEK(dateAndTime) -> INT```

返回星期几（1-7，周一至周日，随本地化）。

示例:  
DAY_OF_WEEK(CREATED)

### DAY_OF_YEAR

```DAY_OF_YEAR(dateAndTime) -> INT```

返回当年第几天（1-366）。

示例:  
DAY_OF_YEAR(CREATED)

### EXTRACT

```EXTRACT(datetimeField FROM dateAndTime) -> INT```

从日期/时间提取指定字段的值（`EPOCH` 通常表示自 1970-01-01 起的秒数，具体实现可能返回更宽的整数）。

EXTRACT 支持以下四种 DateTime 字面量类型：
- `DATE`：EXTRACT(YEAR FROM DATE '2025-05-21')
- `TIME`：EXTRACT(HOUR FROM TIME '17:57:40')
- `TIMESTAMP`：EXTRACT(YEAR FROM TIMESTAMP '2025-05-21T17:57:40')
- `TIMESTAMP WITH TIMEZONE`：EXTRACT(HOUR FROM TIMESTAMPTZ '2025-05-21T17:57:40+08:00')

示例:  
EXTRACT(YEAR FROM TIMESTAMP '2001-02-16 20:38:40')  
EXTRACT(HOUR FROM TIMESTAMP '2001-02-16 20:38:40')  
EXTRACT(DOW FROM TIMESTAMP '2001-02-16 20:38:40')  
EXTRACT(YEAR FROM eventTime)  
EXTRACT(HOUR FROM eventTime)  
EXTRACT(DOW FROM eventTime)

### FORMATDATETIME

```FORMATDATETIME(dateAndTime, formatString) -> STRING```

按给定模式格式化日期/时间/时间戳（参见 `java.time.format.DateTimeFormatter`）。

示例:  
CALL FORMATDATETIME(CREATED, 'yyyy-MM-dd HH:mm:ss')

### HOUR

```HOUR(dateAndTime) -> INT```

返回小时（0-23）。

示例:  
HOUR(CREATED)

### MINUTE

```MINUTE(dateAndTime) -> INT```

返回分钟（0-59）（已弃用；请用 `EXTRACT`）。

示例:  
MINUTE(CREATED)

### MONTH

```MONTH(dateAndTime) -> INT```

返回月份（1-12）（已弃用；请用 `EXTRACT`）。

示例:  
MONTH(CREATED)

### MONTHNAME

```MONTHNAME(dateAndTime) -> STRING```

返回月份名称（英文）。

示例:  
MONTHNAME(CREATED)

### IS_DATE

```IS_DATE(string, formatString) -> BOOLEAN```

判断字符串是否能按给定格式解析为日期/时间。

示例:  
CALL IS_DATE('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')

### PARSEDATETIME / TO_DATE

```PARSEDATETIME | TO_DATE(string, formatString) -> TIMESTAMP```

按给定格式解析字符串为时间戳（参见 `java.time.format.DateTimeFormatter`）。在 SQL 文本中填写 `'` 需写作 `''` 进行转义。

示例:  
CALL PARSEDATETIME('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')  
CALL TO_DATE('2021-04-08''T''13:34:45','yyyy-MM-dd''T''HH:mm:ss')

### QUARTER

```QUARTER(dateAndTime) -> INT```

返回季度（1-4）。

示例:  
QUARTER(CREATED)

### SECOND

```SECOND(dateAndTime) -> INT```

返回秒（0-59）（已弃用；请用 `EXTRACT`）。

示例:  
SECOND(CREATED)

### WEEK

```WEEK(dateAndTime) -> INT```

返回周数（1-53），依赖系统区域设置。

示例:  
WEEK(CREATED)

### YEAR

```YEAR(dateAndTime) -> INT```

返回年份。

示例:  
YEAR(CREATED)

### FROM_UNIXTIME

```FROM_UNIXTIME(unixtime, formatString[, timeZone]) -> STRING```

将自 UNIX 纪元起的秒数格式化为时间戳字符串；`timeZone` 可选（如 `UTC+8`）。

示例:  
// 使用默认时区  
CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss')  
// 使用指定时区  
CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss','UTC+6')

---

## 系统函数

### CAST

```CAST(value AS dataType) -> dataType```

将值转换为其他数据类型。  
支持：STRING | VARCHAR, TINYINT, SMALLINT, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES, BOOLEAN

示例:  
CAST(NAME AS INT)  
CAST(FLAG AS BOOLEAN)

注意（转换为 BOOLEAN 时）：
1) `'true'` / `'false'` → 对应布尔值；
2) 数值 `1` / `0` → `true` / `false`；
3) 无法解析则抛出 `TransformException`。

### TRY_CAST

```TRY_CAST(value AS dataType) -> dataType | NULL```

与 CAST 类似，但失败时返回 NULL。  
支持：STRING | VARCHAR, TINYINT, SMALLINT, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES

示例:  
TRY_CAST(NAME AS INT)

### COALESCE

```COALESCE(aValue, bValue[, ...]) -> type(of first non-null arg)```

返回第一个非 NULL 的参数。

示例:  
COALESCE(A, B, C)

### IFNULL

```IFNULL(aValue, bValue) -> type(common of args)```

返回第一个非 NULL 的参数。

示例:  
IFNULL(A, B)

### NULLIF

```NULLIF(aValue, bValue) -> type(aValue) | NULL```

若 `aValue = bValue` 返回 NULL，否则返回 `aValue`。

示例:  
NULLIF(A, B)

### MULTI_IF

```MULTI_IF(condition1, value1, condition2, value2, ... conditionN, valueN, bValue) -> type(of values)```

返回第一个条件为真的对应值；若都为假，返回最后一个值。

示例:  
MULTI_IF(A > 1, 'A', B > 1, 'B', C > 1, 'C', 'D')

### CASE WHEN

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

```UUID()```

通过java函数生成uuid

示例:

select UUID() as seatunnel_uuid


### ARRAY

```ARRAY<T> array(T, ...)```
创建一个由可变参数元素组成的数组并返回它。这里，T 可以是“列”或“常量”。。

示例:

select ARRAY(1,2,3) as arrays
select ARRAY('c_1',2,3.12) as arrays
select ARRAY(column1,column2,column3) as arrays

注意：目前仅支持string、double、long、int几种类型

### LATERAL VIEW
#### EXPLODE

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
