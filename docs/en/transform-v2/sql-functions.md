# SQL Functions

> The Functions of SQL transform plugin

## String Functions

### ASCII

 ```ASCII(string) -> INT```

 Returns the `ASCII` value of the first character in the string.

Example:
ASCII('Hi')

### BIT_LENGTH

 ```BIT_LENGTH(bytes) -> LONG```

 Returns the number of bits in a binary string.

Example:
BIT_LENGTH(NAME)

### CHAR_LENGTH / LENGTH

 ```CHAR_LENGTH | LENGTH (string) -> LONG```

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

 ```CONCAT(string, string[, string ...]) -> STRING```

 Combines strings. Unlike with the operator `||`, **NULL** parameters are ignored and do not cause the result to become **NULL**.

Example:
CONCAT(NAME, '_')

### CONCAT_WS

 ```CONCAT_WS(separatorString, string, string[, string ...]) -> STRING```

 Concatenates strings using the given separator. A NULL separator is treated as an empty string; other NULL arguments are ignored.

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

 Inserts an additional string into the original string at the specified start position. `lengthInt` is the number of characters removed starting at that position.

Example:
INSERT(NAME, 1, 1, ' ')


### LOWER / LCASE

 ```LOWER | LCASE (string) -> STRING```

 Converts a string to lowercase.

Example:
LOWER(NAME)

### UPPER / UCASE

 ```UPPER | UCASE (string) -> STRING```

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

 Removes all leading and trailing spaces or other specified characters from a string.

Example:
TRIM(NAME)

### REGEXP_REPLACE

 ```REGEXP_REPLACE(inputString, regexString, replacementString[, flagsString]) -> STRING```

 Replaces each substring that matches a regular expression. For details, see the Java `String.replaceAll()` method. If any parameter is null (except optional `flagsString`), the result is null.

 `i` enables case insensitive matching (Pattern.CASE_INSENSITIVE)  
 `c` disables case insensitive matching (Pattern.CASE_INSENSITIVE)  
 `n` allows the period to match the newline character (Pattern.DOTALL)  
 `m` enables multiline mode (Pattern.MULTILINE)

Example:
REGEXP_REPLACE('Hello    World', ' +', ' ')
REGEXP_REPLACE('Hello WWWWorld', 'w+', 'W', 'i')

### REGEXP_LIKE

 ```REGEXP_LIKE(inputString, regexString[, flagsString]) -> BOOLEAN```

 Matches string to a regular expression. For details, see the Java `Matcher.find()` method. If any parameter is null (except optional `flagsString`), the result is null.

 `i` enables case insensitive matching (Pattern.CASE_INSENSITIVE)  
 `c` disables case insensitive matching (Pattern.CASE_INSENSITIVE)  
 `n` allows the period to match the newline character (Pattern.DOTALL)  
 `m` enables multiline mode (Pattern.MULTILINE)


Example:
REGEXP_LIKE('Hello    World', '[A-Z ]*', 'i')

### REGEXP_SUBSTR

 ```REGEXP_SUBSTR(inputString, regexString[, positionInt, occurrenceInt, flagsString, groupInt]) -> STRING```

 Matches string to a regular expression and returns the matched substring. For details, see the `java.util.regex.Pattern` and related functionality.

 The parameter `position` specifies where in `inputString` the match should start. `Occurrence` indicates which occurrence of `pattern` in `inputString` to search for.

 `i` enables case insensitive matching (Pattern.CASE_INSENSITIVE)  
 `c` disables case insensitive matching (Pattern.CASE_INSENSITIVE)  
 `n` allows the period to match the newline character (Pattern.DOTALL)  
 `m` enables multiline mode (Pattern.MULTILINE)

 If the pattern has groups, the `group` parameter can be used to specify which group to return.

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

 Replaces all occurrences of the search string in the input string with the replacement string.  
 If the replacement string is omitted, all occurrences of the search string are removed.  

Example:
REPLACE(NAME, ' ')

### SPLIT

 ```SPLIT(string, delimiterString) -> ARRAY<STRING>```

 Split a string into an array.

Example:
SELECT SPLIT(test,';') AS arrays

### SOUNDEX

 ```SOUNDEX(string) -> STRING```

 Returns a four-character code representing the sound of a string. See https://en.wikipedia.org/wiki/Soundex

Example:
SOUNDEX(NAME)

### SPACE

 ```SPACE(int) -> STRING```

 Returns a string consisting of a number of spaces.

Example:
SPACE(80)

### SUBSTRING / SUBSTR

 ```SUBSTRING | SUBSTR (string, startInt[, lengthInt]) -> STRING```

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

```ABS(numeric) -> numeric (same type)```

 Returns the absolute value of the input. For signed integers, `ABS(MIN_VALUE)` overflows (e.g., `INT -2147483648`) and can raise an  error—cast to a wider type to avoid this.

Example:
ABS(I)

### ACOS

 ```ACOS(numeric) -> DOUBLE```

 Calculate the arc cosine. See also Java `Math.acos`.

Example:
ACOS(D)

### ARRAY_MAX

 ```ARRAY_MAX(array) -> type(array element)```

 Returns the maximum value of the array expression.

Example:
ARRAY_MAX(I)

### ARRAY_MIN

 ```ARRAY_MIN(array) -> type(array element)```

 Returns the minimum value of the array expression.

Example:
ARRAY_MIN(I)

### ASIN

 ```ASIN(numeric) -> DOUBLE```

 Calculate the arc sine. See also Java `Math.asin`.

Example:
ASIN(D)

### ATAN

 ```ATAN(numeric) -> DOUBLE```

 Calculate the arc tangent. See also Java `Math.atan`.

Example:
ATAN(D)

### COS

 ```COS(numeric) -> DOUBLE```

 Calculate the trigonometric cosine. See also Java `Math.cos`.

Example:
COS(ANGLE)

### COSH

 ```COSH(numeric) -> DOUBLE```

 Calculate the hyperbolic cosine. See also Java `Math.cosh`.

Example:
COSH(X)

### COT

 ```COT(numeric) -> DOUBLE```

 Calculate the trigonometric cotangent (1/TAN(ANGLE)).

Example:
COT(ANGLE)

### SIN

 ```SIN(numeric) -> DOUBLE```

 Calculate the trigonometric sine. See also Java `Math.sin`.

Example:
SIN(ANGLE)

### SINH

 ```SINH(numeric) -> DOUBLE```

 Calculate the hyperbolic sine. See also Java `Math.sinh`.

Example:
SINH(ANGLE)

### TAN

 ```TAN(numeric) -> DOUBLE```

 Calculate the trigonometric tangent. See also Java `Math.tan`.

Example:
TAN(ANGLE)

### TANH

 ```TANH(numeric) -> DOUBLE```

 Calculate the hyperbolic tangent. See also Java `Math.tanh`.

Example:
TANH(X)

### MOD

 ```MOD(dividendNumeric, divisorNumeric) -> type(divisorNumeric)```

 The modulus expression. Result is NULL if either argument is NULL; if divisor is 0, an exception is raised. Result has the same sign as dividend or is equal to 0.

Example:
MOD(A, B)

### CEIL / CEILING

 ```CEIL | CEILING (numeric) -> numeric (same type, scale 0)```

 Returns the smallest integer value that is greater than or equal to the argument (scale set to 0).

Example:
CEIL(A)

### EXP

 ```EXP(numeric) -> DOUBLE```

 See also Java `Math.exp`.

Example:
EXP(A)

### FLOOR

 ```FLOOR(numeric) -> numeric (same type, scale 0)```

 Returns the largest integer value that is less than or equal to the argument (scale set to 0).

Example:
FLOOR(A)

### LN

 ```LN(numeric) -> DOUBLE```

 Calculates the natural (base e) logarithm.

Example:
LN(A)

### LOG

```LOG(baseNumeric, numeric) -> DOUBLE```

 Computes the logarithm with the specified base. Arguments must be positive; base cannot be 1. The single-argument form is deprecated—use `LN` or `LOG10`.

Example:
LOG(2, A)

### LOG10

 ```LOG10(numeric) -> DOUBLE```

 Calculates the base 10 logarithm.

Example:
LOG10(A)

### RADIANS

 ```RADIANS(numeric) -> DOUBLE```

 See also Java `Math.toRadians`.

Example:
RADIANS(A)

### SQRT

 ```SQRT(numeric) -> DOUBLE```

 See also Java `Math.sqrt`.

Example:
SQRT(A)

### PI

 ```PI() -> DOUBLE```

 See also Java `Math.PI`.

Example:
PI()

### POWER

 ```POWER(numeric, numeric) -> DOUBLE```

 See also Java `Math.pow`.

Example:
POWER(A, B)

### RAND / RANDOM

 ```RAND | RANDOM([int]) -> DOUBLE```

 Returns a pseudorandom number in the range [0, 1). With an integer argument, seeds the session's random number generator.

Example:
RAND()

### ROUND

 ```ROUND(numeric[, digitsInt]) -> numeric (same type)```

 Rounds to a number of fractional digits (precision/scale adjusted if applicable).

Example:
ROUND(N, 2)

### SIGN

 ```SIGN(numeric) -> INT```

 Returns -1 if the value is smaller than 0, 0 if zero or NaN, and otherwise 1.

Example:
SIGN(N)

### TRUNC

 ```TRUNC | TRUNCATE(numeric[, digitsInt]) -> numeric (same type)```

 Truncates the value to the specified number of fractional digits (toward zero). Precision/scale may be adjusted if applicable.

Example:
TRUNC(N, 2)

### TRIM_SCALE

 ```TRIM_SCALE(numeric) -> STRING```

 Reduce the scale of a number by removing trailing zeroes.

Example:
TRIM_SCALE(N)

## Time and Date Functions

### CURRENT_DATE

 ```CURRENT_DATE -> DATE```

 Returns the current date. These functions return the same value within a transaction (default) or within a command depending on database mode.

Example:
CURRENT_DATE

### CURRENT_TIME

 ```CURRENT_TIME -> TIME```

 Returns the current time of day.

Example:
CURRENT_TIME

### CURRENT_TIMESTAMP / NOW

 ```CURRENT_TIMESTAMP | NOW() -> TIMESTAMP```

 Returns the current timestamp.

Example:
CURRENT_TIMESTAMP

### DATEADD / TIMESTAMPADD

 ```DATEADD | TIMESTAMPADD(dateAndTime, addIntLong, datetimeFieldString) -> dateAndTime (same type)```

 Adds units to a date-time value; use negative values to subtract. The `datetimeFieldString` indicates the unit.  
 **Note:** Adding time-based fields (HOUR/MINUTE/SECOND/MILLISECOND/MICROSECOND/NANOSECOND) to a `DATE` value may return a `TIMESTAMP`.

Example:
DATEADD(CREATED, 1, 'MONTH')

### DATEDIFF

 ```DATEDIFF(aDateAndTime, bDateAndTime, datetimeFieldString) -> LONG```

 Returns the number of crossed unit boundaries between two date-time values. The `datetimeField` indicates the unit.

Example:
DATEDIFF(T1.CREATED, T2.CREATED, 'MONTH')

### DATE_TRUNC

 ```DATE_TRUNC(dateAndTime, datetimeFieldString) -> dateAndTime (same type)```

 Truncates the specified date-time value to the specified field.

Example:
DATE_TRUNC(CREATED, 'DAY');

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

 ```EXTRACT(datetimeField FROM dateAndTime) -> INT```

 Returns a value of the specific time unit from a date/time value. (Note: `EPOCH` may be represented as seconds since epoch; implementations may return a wider integer.)

 The EXTRACT function supports all four DateTime literal types:

 - `DATE`: For extracting date components from a date literal
   EXTRACT(YEAR FROM DATE '2025-05-21')

 - `TIME`: For extracting time components from a time literal
   EXTRACT(HOUR FROM TIME '17:57:40')

 - `TIMESTAMP`: For extracting date and time components from a timestamp literal
   EXTRACT(YEAR FROM TIMESTAMP '2025-05-21T17:57:40')

 - `TIMESTAMP WITH TIMEZONE`: For extracting components from a timestamp with timezone literal
   EXTRACT(HOUR FROM TIMESTAMPTZ '2025-05-21T17:57:40+08:00')

Examples:
EXTRACT(YEAR FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(HOUR FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(DOW FROM TIMESTAMP '2001-02-16 20:38:40')
EXTRACT(YEAR FROM eventTime)
EXTRACT(HOUR FROM eventTime)
EXTRACT(DOW FROM eventTime)

### FORMATDATETIME

 ```FORMATDATETIME(dateAndTime, formatString) -> STRING```

 Formats a date/time/timestamp value as a string using a Java `DateTimeFormatter` pattern.

Example:
CALL FORMATDATETIME(CREATED, 'yyyy-MM-dd HH:mm:ss')

### HOUR

 ```HOUR(dateAndTime) -> INT```

 Returns the hour (0-23) from a date/time value.

Example:
HOUR(CREATED)

### MINUTE

 ```MINUTE(dateAndTime) -> INT```

 Returns the minute (0-59) from a date/time value. (Deprecated; use `EXTRACT`.)

Example:
MINUTE(CREATED)

### MONTH

 ```MONTH(dateAndTime) -> INT```

 Returns the month (1-12) from a date/time value. (Deprecated; use `EXTRACT`.)

Example:
MONTH(CREATED)

### MONTHNAME

 ```MONTHNAME(dateAndTime) -> STRING```

 Returns the name of the month (in English).

Example:
MONTHNAME(CREATED)

### IS_DATE

 ```IS_DATE(string, formatString) -> BOOLEAN```

 Returns whether the string can be parsed as a date/time using the given format.

Example:
CALL IS_DATE('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')

### PARSEDATETIME / TO_DATE

 ```PARSEDATETIME | TO_DATE(string, formatString) -> TIMESTAMP```

 Parses a string into a timestamp using the given format. In SQL text, single quotes must be escaped as `''`.

Example:
CALL PARSEDATETIME('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')
CALL TO_DATE('2021-04-08''T''13:34:45','yyyy-MM-dd''T''HH:mm:ss')

### QUARTER

 ```QUARTER(dateAndTime) -> INT```

 Returns the quarter (1-4) from a date/time value.

Example:
QUARTER(CREATED)

### SECOND

 ```SECOND(dateAndTime) -> INT```

 Returns the second (0-59) from a date/time value. (Deprecated; use `EXTRACT`.)

Example:
SECOND(CREATED)

### WEEK

 ```WEEK(dateAndTime) -> INT```

 Returns the week (1-53) from a date/time value. This function uses the current system locale.

Example:
WEEK(CREATED)

### YEAR

 ```YEAR(dateAndTime) -> INT```

 Returns the year from a date/time value.

Example:
YEAR(CREATED)

### FROM_UNIXTIME

 ```FROM_UNIXTIME(unixtime, formatString[, timeZone]) -> STRING```

 Convert the number of seconds from the UNIX epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment. `timeZone` is optional; e.g., `UTC+8`.

Example:
// use default zone
CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss')
// use given zone
CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss','UTC+6')

## System Functions

### CAST

 ```CAST(value AS dataType) -> dataType```

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

 ```TRY_CAST(value AS dataType) -> dataType | NULL```

 Similar to CAST, but returns NULL instead of throwing an exception when the conversion fails.

 Supported data types: STRING | VARCHAR, TINYINT, SMALLINT, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES

Example:
TRY_CAST(NAME AS INT)

### COALESCE

 ```COALESCE(aValue, bValue[, ...]) -> type(of first non-null arg)```

 Returns the first value that is not null.

Example:
COALESCE(A, B, C)

### IFNULL

 ```IFNULL(aValue, bValue) -> type(common of args)```

 Returns the first value that is not null.

Example:
IFNULL(A, B)

### NULLIF

 ```NULLIF(aValue, bValue) -> type(aValue) | NULL```

 Returns NULL if `a` is equal to `b`, otherwise `a`.

Example:
NULLIF(A, B)

### MULTI_IF

 ```MULTI_IF(condition1, value1, condition2, value2, ... conditionN, valueN, bValue) -> type(of values)```

 Returns the first value for which the corresponding condition is true. If all conditions are false, it returns the last value.

Example:
MULTI_IF(A > 1, 'A', B > 1, 'B', C > 1, 'C', 'D')

### CASE WHEN

Returns the type determined by result expressions.

```
select
  case when c_string in ('c_string') then 1 else 0 end as c_string_1,
  case when c_string not in ('c_string') then 1 else 0 end as c_string_0,
  case when c_tinyint = 117 and TO_CHAR(c_boolean) = 'true' then 1 else 0 end as c_tinyint_boolean_1,
  case when c_tinyint != 117 and TO_CHAR(c_boolean) = 'true' then 1 else 0 end as c_tinyint_boolean_0,
  case when c_tinyint != 117 or TO_CHAR(c_boolean) = 'true' then 1 else 0 end as c_tinyint_boolean_or_1,
  case
    when c_int > 1
     and c_bigint > 1
     and c_float > 1
     and c_double > 1
     and c_decimal > 1 then 1
    else 0
  end as c_number_1,
  case when c_tinyint <> 117 then 1 else 0 end as c_number_0,
  case when c_boolean then 1 else 0 end as c_boolean_0
from dual
```

 It is used to determine whether the condition is valid and return different values according to different judgments.

Example:
case when c_string in ('c_string') then 1 else 0 end
case when c_string in ('c_string') then true else false end

### UUID

 ```UUID() -> STRING```

 Generates a UUID.

Example:
SELECT UUID() AS seatunnel_uuid

### ARRAY

 ```ARRAY<T> array(T, ...) -> ARRAY<T>```

 Create an array consisting of variadic elements and return it. Here, `T` can be either “column” or “literal”.

Example:
SELECT ARRAY(1,2,3) AS arrays
SELECT ARRAY('c_1',2,3.12) AS arrays
SELECT ARRAY(column1,column2,column3) AS arrays

 Notes: Currently only STRING, DOUBLE, LONG, INT types are supported

### LATERAL VIEW
#### EXPLODE

 Used to flatten array columns into multiple rows. It applies the EXPLODE function to an array and generates a new row for each element.

 - EXPLODE: Converts an array column into multiple rows. No rows generated if array is NULL or empty.  
 - OUTER EXPLODE: Returns NULL when array is NULL or empty, ensuring at least one row is generated.  
 - EXPLODE(SPLIT(field_name, separator)): Splits a string into an array using the specified separator, then explodes it into rows.  
 - EXPLODE(ARRAY(value1, value2, ...)): Explodes a custom-defined array into multiple rows.

Example:
```
SELECT * FROM dual
        LATERAL VIEW EXPLODE ( SPLIT ( NAME, ',' ) ) AS NAME
        LATERAL VIEW EXPLODE ( SPLIT ( pk_id, ';' ) ) AS pk_id
        LATERAL VIEW OUTER EXPLODE ( age ) AS age
        LATERAL VIEW OUTER EXPLODE ( ARRAY(1,1) ) AS num
```

