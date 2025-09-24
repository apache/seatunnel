# SQL Functions

> The Functions of SQL transform plugin

## String Functions

### ASCII

```ASCII(string) -> INT```

Returns the ```ASCII``` value of the first character in the string.

Example:

ASCII('Hi')

### BIT_LENGTH

```BIT_LENGTH(bytes) -> LONG```

Returns the number of bits in a binary string.

Example:

BIT_LENGTH(NAME)

### CHAR_LENGTH / LENGTH

```CHAR_LENGTH | LENGTH(string) -> LONG```

Returns the number of characters in a character string.

Example:

CHAR_LENGTH(NAME)

### OCTET_LENGTH

```OCTET_LENGTH(bytes) -> LONG```

Returns the number of bytes in a binary string.

Example:

OCTET_LENGTH(NAME)

### CHAR / CHR

```CHAR | CHR (int) -> STRING```

Returns the character that represents the ASCII value.

Example:

CHAR(65)

### CONCAT

```CONCAT(string, string[, string...]) -> STRING```

Combines strings. Unlike with the operator ```||```, **NULL** parameters are ignored, and do not cause the result to become **NULL**. If all parameters are NULL the result is an empty string.

Example:

CONCAT(NAME, '_')

### CONCAT_WS

```CONCAT_WS(separatorString, string, string[, string...]) -> STRING```

Combines strings with separator. If separator is **NULL** it is treated like an empty string. Other **NULL** parameters are ignored. Remaining **non-NULL** parameters, if any, are concatenated with the specified separator. If there are no remaining parameters the result is an empty string.

Example:

CONCAT_WS(',', NAME, '_')

### HEXTORAW

```HEXTORAW(string) -> STRING```

Converts a hex representation of a string to a string. 4 hex characters per string character are used.

Example:

HEXTORAW(DATA)

### RAWTOHEX

```RAWTOHEX(string | bytes) -> STRING```

Converts a string or bytes to the hex representation. 4 hex characters per string character are used.

Example:

RAWTOHEX(DATA)

### INSERT

```INSERT(originalString, startInt, lengthInt, addString) -> STRING```

Inserts an additional string into the original string at a specified start position. The length specifies the number of characters that are removed at the start position in the original string.

Example:

INSERT(NAME, 1, 1, ' ')

### LOWER / LCASE

```LOWER | LCASE(string) -> STRING```

Converts a string to lowercase.

Example:

LOWER(NAME)

### UPPER / UCASE

```UPPER | UCASE(string) -> STRING```

Converts a string to uppercase.

Example:

UPPER(NAME)

### LEFT

```LEFT(string, int) -> STRING```

Returns the leftmost number of characters.

Example:

LEFT(NAME, 3)

### RIGHT

```RIGHT(string, int) -> STRING```

Returns the rightmost number of characters.

Example:

RIGHT(NAME, 3)

### LOCATE / INSTR / POSITION

```LOCATE(searchString, string[, startInt]) -> INT```

```INSTR(string, searchString[, startInt]) -> INT```

```POSITION(searchString, string) -> INT```

Returns the location of a search string in a string. If a start position is used, the characters before it are ignored. If position is negative, the rightmost location is returned. 0 is returned if the search string is not found. Please note this function is case sensitive, even if the parameters are not.

Example:

LOCATE('.', NAME)

### LPAD

```LPAD(string, int[, string]) -> STRING```

Left pad the string to the specified length. If the length is shorter than the string, it will be truncated at the end. If the padding string is not set, spaces will be used.

Example:

LPAD(AMOUNT, 10, '*')

### RPAD

```RPAD(string, int[, string]) -> STRING```

Right pad the string to the specified length. If the length is shorter than the string, it will be truncated. If the padding string is not set, spaces will be used.

Example:

RPAD(TEXT, 10, '-')

### LTRIM

```LTRIM(string[, characterToTrimString]) -> STRING```

Removes all leading spaces or other specified characters from a string.

Example:

LTRIM(NAME)

### RTRIM

```RTRIM(string[, characterToTrimString]) -> STRING```

Removes all trailing spaces or other specified characters from a string.

Example:

RTRIM(NAME)

### TRIM

```TRIM(string[, characterToTrimString]) -> STRING```

Removes all leading spaces and trailing spaces or other specified characters from a string.

Example:

TRIM(NAME)

### REGEXP_REPLACE

```REGEXP_REPLACE(inputString, regexString, replacementString[, flagsString]) -> STRING```

Replaces each substring that matches a regular expression. For details, see the Java String.replaceAll() method. If any parameter is null (except optional flagsString parameter), the result is null.

Flags values are limited to 'i', 'c', 'n', 'm'. Other symbols cause exception. Multiple symbols could be used in one flagsString parameter (like 'im'). Later flags override first ones, for example 'ic' is equivalent to case sensitive matching 'c'.

'i' enables case insensitive matching (Pattern.CASE_INSENSITIVE)

'c' disables case insensitive matching (Pattern.CASE_INSENSITIVE)

'n' allows the period to match the newline character (Pattern.DOTALL)

'm' enables multiline mode (Pattern.MULTILINE)

Example:

REGEXP_REPLACE('Hello    World', ' +', ' ')
REGEXP_REPLACE('Hello WWWWorld', 'w+', 'W', 'i')

### REGEXP_LIKE

```REGEXP_LIKE(inputString, regexString[, flagsString]) -> BOOLEAN```

Matches string to a regular expression. For details, see the Java Matcher.find() method. If any parameter is null (except optional flagsString parameter), the result is null.

Flags values are limited to 'i', 'c', 'n', 'm'. Other symbols cause exception. Multiple symbols could be used in one flagsString parameter (like 'im'). Later flags override first ones, for example 'ic' is equivalent to case sensitive matching 'c'.

'i' enables case insensitive matching (Pattern.CASE_INSENSITIVE)

'c' disables case insensitive matching (Pattern.CASE_INSENSITIVE)

'n' allows the period to match the newline character (Pattern.DOTALL)

'm' enables multiline mode (Pattern.MULTILINE)

Example:

REGEXP_LIKE('Hello    World', '[A-Z ]*', 'i')

### REGEXP_SUBSTR

```REGEXP_SUBSTR(inputString, regexString[, positionInt, occurrenceInt, flagsString, groupInt]) -> STRING```

Matches string to a regular expression and returns the matched substring. For details, see the java.util.regex.Pattern and related functionality.

The parameter position specifies where in inputString the match should start. Occurrence indicates which occurrence of pattern in inputString to search for.

Flags values are limited to 'i', 'c', 'n', 'm'. Other symbols cause exception. Multiple symbols could be used in one flagsString parameter (like 'im'). Later flags override first ones, for example 'ic' is equivalent to case sensitive matching 'c'.

'i' enables case insensitive matching (Pattern.CASE_INSENSITIVE)

'c' disables case insensitive matching (Pattern.CASE_INSENSITIVE)

'n' allows the period to match the newline character (Pattern.DOTALL)

'm' enables multiline mode (Pattern.MULTILINE)

If the pattern has groups, the group parameter can be used to specify which group to return.

Example:

REGEXP_SUBSTR('2020-10-01', '\d{4}')
REGEXP_SUBSTR('2020-10-01', '(\d{4})-(\d{2})-(\d{2})', 1, 1, NULL, 2)

### REPEAT

```REPEAT(string, int) -> STRING```

Returns a string repeated some number of times.

Example:

REPEAT(NAME || ' ', 10)

### REPLACE

```REPLACE(string, searchString[, replacementString]) -> STRING```

Replaces all occurrences of a search string in a text with another string. If no replacement is specified, the search string is removed from the original string. If any parameter is null, the result is null.

Example:

REPLACE(NAME, ' ')

### SPLIT

```SPLIT(string, delimiterString) -> ARRAY<STRING>```

Split a string into an array.

Example:

select SPLIT(test,';') as arrays

### MURMUR64

```MURMUR64(string) -> LONG```

Calculate MurmurHash 128 for the input string and return the lower 64 bits as a long value. MurmurHash is a non-cryptographic hash function suitable for general hash-based lookup. This method returns a long value, or null if the input parameter is null.

Example:

MURMUR64('hello world')
MURMUR64(NAME)

### SOUNDEX

```SOUNDEX(string) -> STRING```

Returns a four character code representing the sound of a string. This method returns a string, or null if parameter is null. See https://en.wikipedia.org/wiki/Soundex for more information.

Example:

SOUNDEX(NAME)

### SPACE

```SPACE(int) -> STRING```

Returns a string consisting of a number of spaces.

Example:

SPACE(80)

### SUBSTRING / SUBSTR

```SUBSTRING | SUBSTR(string, startInt[, lengthInt ]) -> STRING```

Returns a substring of a string starting at a position. If the start index is negative, then the start index is relative to the end of the string. The length is optional.

Example:

CALL SUBSTRING('[Hello]', 2);
CALL SUBSTRING('hour', 3, 2);

### TO_CHAR

```TO_CHAR(value[, formatString]) -> STRING```

Oracle-compatible TO_CHAR function that can format a timestamp, a number, or text.

Example:

CALL TO_CHAR(SYS_TIME, 'yyyy-MM-dd HH:mm:ss')

### TRANSLATE

```TRANSLATE(value, searchString, replacementString) -> STRING```

Oracle-compatible TRANSLATE function that replaces a sequence of characters in a string with another set of characters.

Example:

CALL TRANSLATE('Hello world', 'eo', 'EO')

## Numeric Functions

### ABS

```ABS(numeric) -> NUMERIC (same type)```

Returns the absolute value of a specified value. The returned value is of the same data type as the parameter.

Note that TINYINT, SMALLINT, INT, and BIGINT data types cannot represent absolute values of their minimum negative values, because they have more negative values than positive. For example, for INT data type allowed values are from -2147483648 to 2147483647. ABS(-2147483648) should be 2147483648, but this value is not allowed for this data type. It leads to an exception. To avoid it cast argument of this function to a higher data type.

Example:

ABS(I)

### ACOS

```ACOS(numeric) -> DOUBLE```

Calculate the arc cosine. See also Java Math.acos.

Example:

ACOS(D)

### ARRAY_MAX

```ARRAY_MAX(ARRAY) -> type(array element)```

The MAX function returns the maximum value of the expression.

Example:

ARRAY_MAX(I)

### ARRAY_MIN

```ARRAY_MIN(ARRAY) -> type(array element)```

The MIN function returns the minimum value of the expression.

Example:

ARRAY_MIN(I)

### ASIN

```ASIN(numeric) -> DOUBLE```

Calculate the arc sine. See also Java Math.asin.

Example:

ASIN(D)

### ATAN

```ATAN(numeric) -> DOUBLE```

Calculate the arc tangent. See also Java Math.atan.

Example:

ATAN(D)

### COS

```COS(numeric) -> DOUBLE```

Calculate the trigonometric cosine. See also Java Math.cos.

Example:

COS(ANGLE)

### COSH

```COSH(numeric) -> DOUBLE```

Calculate the hyperbolic cosine. See also Java Math.cosh.

Example:

COSH(X)

### COT

```COT(numeric) -> DOUBLE```

Calculate the trigonometric cotangent (1/TAN(ANGLE)). See also Java Math.* functions.

Example:

COT(ANGLE)

### SIN

```SIN(numeric) -> DOUBLE```

Calculate the trigonometric sine. See also Java Math.sin.

Example:

SIN(ANGLE)

### SINH

```SINH(numeric) -> DOUBLE```

Calculate the hyperbolic sine. See also Java Math.sinh.

Example:

SINH(ANGLE)

### TAN

```TAN(numeric) -> DOUBLE```

Calculate the trigonometric tangent. See also Java Math.tan.

Example:

TAN(ANGLE)

### TANH

```TANH(numeric) -> DOUBLE```

Calculate the hyperbolic tangent. See also Java Math.tanh.

Example:

TANH(X)

### MOD

```MOD(dividendNumeric, divisorNumeric ) -> type(divisorNumeric)```

The modulus expression.

Result is NULL if either of arguments is NULL. If divisor is 0, an exception is raised. Result has the same sign as dividend or is equal to 0.

Usually arguments should have scale 0, but it isn't required by H2.

Example:

MOD(A, B)

### CEIL / CEILING

```CEIL | CEILING (numeric) -> NUMERIC (same type, scale 0)```

Returns the smallest integer value that is greater than or equal to the argument. This method returns value of the same type as argument, but with scale set to 0 and adjusted precision, if applicable.

Example:

CEIL(A)

### EXP

```EXP(numeric) -> DOUBLE```

See also Java Math.exp.

Example:

EXP(A)

### FLOOR

```FLOOR(numeric) -> NUMERIC (same type, scale 0)```

Returns the largest integer value that is less than or equal to the argument. This method returns value of the same type as argument, but with scale set to 0 and adjusted precision, if applicable.

Example:

FLOOR(A)

### LN

```LN(numeric) -> DOUBLE```

Calculates the natural (base e) logarithm. Argument must be a positive numeric value.

Example:

LN(A)

### LOG

```LOG(baseNumeric, numeric) -> DOUBLE```

Calculates the logarithm with specified base. Argument and base must be positive numeric values. Base cannot be equal to 1.

The default base is e (natural logarithm), in the PostgreSQL mode the default base is base 10. In MSSQLServer mode the optional base is specified after the argument.

Single-argument variant of LOG function is deprecated, use LN or LOG10 instead.

Example:

LOG(2, A)

### LOG10

```LOG10(numeric) -> DOUBLE```

Calculates the base 10 logarithm. Argument must be a positive numeric value.

Example:

LOG10(A)

### RADIANS

```RADIANS(numeric) -> DOUBLE```

See also Java Math.toRadians.

Example:

RADIANS(A)

### SQRT

```SQRT(numeric) -> DOUBLE```

See also Java Math.sqrt.

Example:

SQRT(A)

### PI

```PI() -> DOUBLE```

See also Java Math.PI.

Example:

PI()

### POWER

```POWER(numeric, numeric) -> DOUBLE```

See also Java Math.pow.

Example:

POWER(A, B)

### RAND / RANDOM

```RAND | RANDOM([ int ]) -> DOUBLE```

Calling the function without parameter returns the next a pseudo random number. Calling it with an parameter seeds the session's random number generator. This method returns a double between 0 (including) and 1 (excluding).

Example:

RAND()

### ROUND

```ROUND(numeric[, digitsInt]) -> NUMERIC (same type)```

Rounds to a number of fractional digits. This method returns value of the same type as argument, but with adjusted precision and scale, if applicable.

Example:

ROUND(N, 2)

### SIGN

```SIGN(numeric) -> INT```

Returns -1 if the value is smaller than 0, 0 if zero or NaN, and otherwise 1.

Example:

SIGN(N)

### TRUNC

```TRUNC | TRUNCATE(numeric[, digitsInt]) -> NUMERIC (same type)```

When a numeric argument is specified, truncates it to a number of digits (to the next value closer to 0) and returns value of the same type as argument, but with adjusted precision and scale, if applicable.

Example:

TRUNC(N, 2)

### TRIM_SCALE

```TRIM_SCALE(numeric) -> NUMERIC (same type)```

Reduce the scale of a number by removing trailing zeroes. The scale is adjusted accordingly.

Example:

TRIM_SCALE(N)

## Time and Date Functions

### CURRENT_DATE

```CURRENT_DATE [()] -> DATE```

Returns the current date.

These functions return the same value within a transaction (default) or within a command depending on database mode.

Example:

CURRENT_DATE

### CURRENT_TIME

```CURRENT_TIME [()] -> TIME```

Returns the current time with system time zone. The actual maximum available precision depends on operating system and JVM and can be 3 (milliseconds) or higher. Higher precision is not available before Java 9.

Example:

CURRENT_TIME

### CURRENT_TIMESTAMP / NOW

```CURRENT_TIMESTAMP[()] | NOW() -> TIMESTAMP```

Returns the current timestamp with system time zone. The actual maximum available precision depends on operating system and JVM and can be 3 (milliseconds) or higher. Higher precision is not available before Java 9.

Example:

CURRENT_TIMESTAMP

### DATEADD / TIMESTAMPADD

```DATEADD | TIMESTAMPADD(dateAndTime, addIntLong, datetimeFieldString) -> type(dateAndTime)```

Adds units to a date-time value. The datetimeFieldString indicates the unit. Use negative values to subtract units. addIntLong may be a long value when manipulating milliseconds, microseconds, or nanoseconds otherwise its range is restricted to int. This method returns a value with the same type as specified value if unit is compatible with this value. If specified field is a HOUR, MINUTE, SECOND, MILLISECOND, etc and value is a DATE value DATEADD returns combined TIMESTAMP. Fields DAY, MONTH, YEAR, WEEK, etc are not allowed for TIME values.

Example:

DATEADD(CREATED, 1, 'MONTH')

### DATEDIFF

```DATEDIFF(aDateAndTime, bDateAndTime, datetimeFieldString) -> LONG```

Returns the number of crossed unit boundaries between two date-time values. The datetimeField indicates the unit.

Example:

DATEDIFF(T1.CREATED, T2.CREATED, 'MONTH')

### DATE_TRUNC

```DATE_TRUNC(dateAndTime, datetimeFieldString) -> dateAndTime (same type)```

Truncates the specified date-time value to the specified field.

Example:

DATE_TRUNC(CREATED, 'DAY')

### DAYNAME

```DAYNAME(dateAndTime) -> STRING```

Returns the name of the day (in English).

Example:

DAYNAME(CREATED)

### DAY_OF_MONTH

```DAY_OF_MONTH(dateAndTime) -> INT```

Returns the day of the month (1-31).

Example:

DAY_OF_MONTH(CREATED)

### DAY_OF_WEEK

```DAY_OF_WEEK(dateAndTime) -> INT```

Returns the day of the week (1-7) (Monday-Sunday), locale-specific.

Example:

DAY_OF_WEEK(CREATED)

### DAY_OF_YEAR

```DAY_OF_YEAR(dateAndTime) -> INT```

Returns the day of the year (1-366).

Example:

DAY_OF_YEAR(CREATED)

### EXTRACT

```EXTRACT(datetimeField FROM dateAndTime) -> INT | NUMERIC```

Returns a value of the specific time unit from a date/time value. This method returns a numeric value with EPOCH field and an int for all other fields.

The following are valid field names for EXTRACT:

- `CENTURY`: The century; for interval values, the year field divided by 100
- `DAY`: The day of the month (1-31); for interval values, the number of days
- `DECADE`: The year field divided by 10
- `DOW` or `DAYOFWEEK`: The day of the week as Sunday (0) to Saturday (6)
- `DOY`: The day of the year (1-365/366)
- `EPOCH`: For timestamp values, the number of seconds since 1970-01-01 00:00:00; for interval values, the total number of seconds
- `HOUR`: The hour field (0-23)
- `ISODOW`: The day of the week as Monday (1) to Sunday (7), matching ISO 8601
- `ISOYEAR`: The ISO 8601 week-numbering year
- `MICROSECONDS`: The seconds field, including fractional parts, multiplied by 1,000,000
- `MILLENNIUM`: The millennium; for interval values, the year field divided by 1000
- `MILLISECONDS`: The seconds field, including fractional parts, multiplied by 1,000
- `MINUTE`: The minutes field (0-59)
- `MONTH`: The number of the month within the year (1-12); for interval values, the number of months modulo 12 (0-11)
- `QUARTER`: The quarter of the year (1-4) that the date is in
- `SECOND`: The seconds field, including any fractional seconds
- `WEEK`: The number of the ISO 8601 week-numbering week of the year (1-53)
- `YEAR`: The year field

The EXTRACT function supports all four DateTime literal types:

- `DATE`: For extracting date components from a date literal
 ```sql
 EXTRACT(YEAR FROM DATE '2025-05-21')
 ```

- `TIME`: For extracting time components from a time literal
 ```sql
 EXTRACT(HOUR FROM TIME '17:57:40')
 ```

- `TIMESTAMP`: For extracting date and time components from a timestamp literal
 ```sql
 EXTRACT(YEAR FROM TIMESTAMP '2025-05-21T17:57:40')
 ```

- `TIMESTAMP WITH TIMEZONE`: For extracting components from a timestamp with timezone literal
 ```sql
 EXTRACT(HOUR FROM TIMESTAMPTZ '2025-05-21T17:57:40+08:00')
 ```

Examples:

```sql
EXTRACT(YEAR FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(HOUR FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(DOW FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(YEAR FROM eventTime)
EXTRACT(HOUR FROM eventTime)
EXTRACT(DOW FROM eventTime)
```

### FORMATDATETIME

```FORMATDATETIME(dateAndTime, formatString) -> STRING```

Formats a date, time or timestamp as a string. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see java.time.format.DateTimeFormatter.

Example:

CALL FORMATDATETIME(CREATED, 'yyyy-MM-dd HH:mm:ss')

### HOUR

```HOUR(dateAndTime) -> INT```

Returns the hour (0-23) from a date/time value.

Example:

HOUR(CREATED)

### MINUTE

```MINUTE(dateAndTime) -> INT```

Returns the minute (0-59) from a date/time value.

This function is deprecated, use EXTRACT instead of it.

Example:

MINUTE(CREATED)

### MONTH

```MONTH(dateAndTime) -> INT```

Returns the month (1-12) from a date/time value.

This function is deprecated, use EXTRACT instead of it.

Example:

MONTH(CREATED)

### MONTHNAME

```MONTHNAME(dateAndTime) -> STRING```

Returns the name of the month (in English).

Example:

MONTHNAME(CREATED)

### IS_DATE

```IS_DATE(string, formatString) -> BOOLEAN```
Parses a string. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see java.time.format.DateTimeFormatter.

Example:

CALL IS_DATE('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')

### PARSEDATETIME / TO_DATE

```PARSEDATETIME | TO_DATE(string, formatString) -> TIMESTAMP```
Parses a string. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see java.time.format.DateTimeFormatter.

Example:

CALL PARSEDATETIME('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')
CALL TO_DATE('2021-04-08'T'13:34:45','yyyy-MM-dd''T''HH:mm:ss')
Note that when filling in `'` in SQL functions, it needs to be escaped to `''`.

### QUARTER

```QUARTER(dateAndTime) -> INT```

Returns the quarter (1-4) from a date/time value.

Example:

QUARTER(CREATED)

### SECOND

```SECOND(dateAndTime) -> INT```

Returns the second (0-59) from a date/time value.

This function is deprecated, use EXTRACT instead of it.

Example:

SECOND(CREATED)

### WEEK

```WEEK(dateAndTime) -> INT```

Returns the week (1-53) from a date/time value.

This function uses the current system locale.

Example:

WEEK(CREATED)

### YEAR

```YEAR(dateAndTime) -> INT```

Returns the year from a date/time value.

Example:

YEAR(CREATED)

### FROM_UNIXTIME

```FROM_UNIXTIME(unixtime, formatString, timeZone) -> STRING```

Convert the number of seconds from the UNIX epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment.

The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see `java.time.format.DateTimeFormatter`.

`timeZone` is optional, default value is system's time zone. `timezone` value can be a `UTC+ timezone offset`, for example, `UTC+8` represents the Asia/Shanghai time zone, see  https://en.wikipedia.org/wiki/List_of_tz_database_time_zones .


Example:

// use default zone

CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss')

or

// use given zone

CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss','UTC+6')

### AT TIME ZONE

```dateAndTime AT TIME ZONE 'timeZone' -> TIMESTAMP_TZ```

Convert a timestamp value to a TIMESTAMP WITH TIME ZONE value in the specified time zone.

`timeZone` value can be a `UTC+ timezone offset`, for example, `+08:00` represents the Asia/Shanghai time zone, see https://en.wikipedia.org/wiki/List_of_tz_database_time_zones .

Example:

local_date_time AT TIME ZONE '+09:00'

offset_date_time AT TIME ZONE 'Pacific/Honolulu'

## System Functions

### CAST

```CAST(value as dataType) -> dataType```

Converts a value to another data type.

Supported data types: STRING | VARCHAR, TINYINT, SMALLINT, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES, BOOLEAN

Example:
* CAST(NAME AS INT)
* CAST(FLAG AS BOOLEAN)

NOTE:
Converts a value to a BOOLEAN data type according to the following rules:
1. If the value can be interpreted as a boolean string (`'true'` or `'false'`), it returns the corresponding boolean value.
2. If the value can be interpreted as a numeric value (`1` or `0`), it returns `true` for `1` and `false` for `0`.
3. If the value cannot be interpreted according to the above rules, it throws a `TransformException`.

### TRY_CAST

```TRY_CAST(value as dataType) -> dataType | NULL```

This function is similar to CAST, but when the conversion fails, it returns NULL instead of throwing an exception.

Supported data types: STRING | VARCHAR, TINYINT, SMALLINT, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES

Example:

TRY_CAST(NAME AS INT)

### COALESCE

```COALESCE(aValue, bValue [,...]) -> type(of first non-null arg)```

Returns the first value that is not null. If subsequent arguments have different data types from the first argument, they will be automatically converted to the type of the first argument.

Example:

COALESCE(A, B, C)

Example with type conversion:

```
-- If A is a string field and B is an integer field
-- B will be converted to string when A is null
SELECT COALESCE(A, B) as result FROM my_table
```

### IFNULL

```IFNULL(aValue, bValue) -> type(common of args)```

Returns the first value that is not null. If subsequent arguments have different data types from the first argument, they will be automatically converted to the type of the first argument.

Example:

IFNULL(A, B)

### NULLIF

```NULLIF(aValue, bValue) -> type(aValue) | NULL```

Returns NULL if 'a' is equal to 'b', otherwise 'a'.

Example:

NULLIF(A, B)


### MULTI_IF
```MULTI_IF(condition1, value1, condition2, value2,... conditionN, valueN, bValue) -> type(of values)```

returns the first value for which the corresponding condition is true. If all conditions are false, it returns the last value.

Example:

MULTI_IF(A > 1, 'A', B > 1, 'B', C > 1, 'C', 'D')

### CASE WHEN
```CASE WHEN <condition> THEN <expr> [WHEN...] [ELSE <expr>] END -> type(of result expressions)```
Returns different values based on conditions.

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
  end as c_number_0,
  case
    when c_boolean then 1
    else 0
  end as c_boolean_0
from
  dual
```

It is used to determine whether the condition is valid and return different values according to different judgments

Example:

case when c_string in ('c_string') then 1 else 0 end

case when c_string in ('c_string') then true else false end

### UUID

```UUID() -> STRING```

Generate a uuid through java function.

Example:

select UUID() as seatunnel_uuid

### ARRAY

```ARRAY<T> array(T, ...) -> ARRAY<T>```
Create an array consisting of variadic elements and return it. Here, T can be either “column” or “literal”.

Example:

select ARRAY(1,2,3) as arrays
select ARRAY('c_1',2,3.12) as arrays
select ARRAY(column1,column2,column3) as arrays

notes: Currently only string, double, long, int types are supported

### LATERAL VIEW
#### EXPLODE
```EXPLODE(array of T) -> rows(value: T)``` 
```OUTER EXPLODE(array of T) -> rows(value: T | NULL)```

Used to flatten array columns into multiple rows. It applies the EXPLODE function to an array and generates a new row for each element.

EXPLODE: Converts an array column into multiple rows. No rows generated if array is NULL or empty.

OUTER EXPLODE: Returns NULL when array is NULL or empty, ensuring at least one row is generated.

EXPLODE(SPLIT(field_name, separator)): Splits a string into an array using the specified separator, then explodes it into rows.

EXPLODE(ARRAY(value1, value2, ...)): Explodes a custom-defined array into multiple rows.

Example:
```
SELECT * FROM dual
	LATERAL VIEW EXPLODE ( SPLIT ( NAME, ',' ) ) AS NAME
	LATERAL VIEW EXPLODE ( SPLIT ( pk_id, ';' ) ) AS pk_id
	LATERAL VIEW OUTER EXPLODE ( age ) AS age
	LATERAL VIEW OUTER EXPLODE ( ARRAY(1,1) ) AS num
```

## Vector Functions

### VECTOR_DIMS

```VECTOR_DIMS(vector) -> INT```

Returns an INT value representing the number of dimensions (elements) in the vector.

Example:

VECTOR_DIMS(vector)

### VECTOR_NORM

```VECTOR_NORM(vector) -> DOUBLE```

Calculates the L2 norm (Euclidean norm) of a vector, which represents the length or magnitude of the vector.

Example:

VECTOR_NORM(vector)

### INNER_PRODUCT

```INNER_PRODUCT(vector1, vector2) -> DOUBLE```

Calculates the inner product (dot product) of two vectors, which is used to measure the similarity and projection between the vectors.

Example:

INNER_PRODUCT(vector1, vector2)

### COSINE_DISTANCE

```COSINE_DISTANCE(vector1, vector2) -> DOUBLE```

Returns a DOUBLE value between 0 and 1:

0: Identical vectors (completely similar)

1: Orthogonal vectors (completely dissimilar)

Example:

COSINE_DISTANCE(vector1, vector2)

### L1_DISTANCE

```L1_DISTANCE(vector1, vector2) -> DOUBLE```

Calculates the Manhattan (L1) distance between two vectors.

Example:

L1_DISTANCE(vector1, vector2)

### L2_DISTANCE

```L2_DISTANCE(vector1, vector2) -> DOUBLE```

Calculates the Euclidean (L2) distance between two vectors.

Example:

L2_DISTANCE(vector1, vector2)

### VECTOR_REDUCE

```VECTOR_REDUCE(vector_field, target_dimension, method)```

Generic vector dimension reduction function that supports multiple reduction methods.

**Parameters:**
- `vector_field`: The vector field to reduce (VECTOR type)
- `target_dimension`: The target dimension (INTEGER, must be smaller than source dimension)
- `method`: The reduction method (STRING):
  - **'TRUNCATE'**: Truncates the vector by keeping only the first N elements. This is the simplest and fastest dimension reduction method, but may lose important information in the truncated dimensions.
  - **'RANDOM_PROJECTION'**: Uses Gaussian random projection with normally distributed random matrix. This method preserves relative distances between vectors while reducing dimensionality, following the Johnson-Lindenstrauss lemma.
  - **'SPARSE_RANDOM_PROJECTION'**: Uses sparse random projection where matrix elements are mostly zero (±√3, 0). This is more computationally efficient than regular random projection while maintaining similar distance preservation properties.

**Returns:** VECTOR type with reduced dimensions

**Example:**
```sql
SELECT id, VECTOR_REDUCE(embedding, 256, 'TRUNCATE') as reduced_embedding FROM table
SELECT id, VECTOR_REDUCE(embedding, 128, 'RANDOM_PROJECTION') as reduced_embedding FROM table
SELECT id, VECTOR_REDUCE(embedding, 64, 'SPARSE_RANDOM_PROJECTION') as reduced_embedding FROM table
```

### VECTOR_NORMALIZE

```VECTOR_NORMALIZE(vector_field)```

Normalizes a vector to unit length (magnitude = 1). This is useful for computing cosine similarity.

**Parameters:**
- `vector_field`: The vector field to normalize (VECTOR type)

**Returns:** VECTOR type - the normalized vector

**Example:**
```sql
SELECT id, VECTOR_NORMALIZE(embedding) as normalized_embedding FROM table
```

