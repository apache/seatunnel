# SQL Functions

> The Functions of SQL transform plugin

## String Functions

### ASCII

```ASCII(string)```

Returns the `ASCII` value of the first character in the string. This method returns an **INT**.

Example:

```ASCII('Hi')```

### BIT_LENGTH

```BIT_LENGTH(bytes)```

Returns the number of bits in a binary string. This method returns a **LONG**.

Example:

```BIT_LENGTH(NAME)```

### CHAR_LENGTH / LENGTH

```CHAR_LENGTH | LENGTH (string)```

Returns the number of characters in a character string. This method returns a **LONG**.

Example:

```CHAR_LENGTH(NAME)```

### OCTET_LENGTH

```OCTET_LENGTH(bytes)```

Returns the number of bytes in a binary string. This method returns a **LONG**.

Example:

```OCTET_LENGTH(NAME)```

### CHAR / CHR

```CHAR | CHR (int)```

Returns the character that represents the ASCII value. This method returns a **STRING**.

Example:

```CHAR(65)```

### CONCAT

```CONCAT(string, string[, string ...])```

Combines strings. Unlike with the operator `||`, **NULL** parameters are ignored, and do not cause the result to become **NULL**. If all parameters are NULL the result is an empty string. This method returns a **STRING**.

Example:

```CONCAT(NAME, '_')```

### CONCAT_WS

```CONCAT_WS(separatorString, string, string[, string ...])```

Combines strings with separator. If separator is **NULL** it is treated like an empty string. Other **NULL** parameters are ignored. Remaining **non-NULL** parameters, if any, are concatenated with the specified separator. If there are no remaining parameters the result is an empty string. This method returns a **STRING**.

Example:

```CONCAT_WS(',', NAME, '_')```

### HEXTORAW

```HEXTORAW(string)```

Converts a hex representation of a string to a string. 4 hex characters per string character are used. This method returns a **STRING**.

Example:

```HEXTORAW(DATA)```

### RAWTOHEX

```RAWTOHEX(string)```

```RAWTOHEX(bytes)```

Converts a string or bytes to the hex representation. 4 hex characters per string character are used. This method returns a **STRING**.

Example:

```RAWTOHEX(DATA)```

### INSERT

```INSERT(originalString, startInt, lengthInt, addString)```

Inserts an additional string into the original string at a specified start position. The length specifies the number of characters that are removed at the start position in the original string. This method returns a **STRING**.

Example:

```INSERT(NAME, 1, 1, ' ')```

### LOWER / LCASE

```LOWER | LCASE (string)```

Converts a string to lowercase. This method returns a **STRING**.

Example:

```LOWER(NAME)```

### UPPER / UCASE

```UPPER | UCASE (string)```

Converts a string to uppercase. This method returns a **STRING**.

Example:

```UPPER(NAME)```

### LEFT

```LEFT(string, int)```

Returns the leftmost number of characters. This method returns a **STRING**.

Example:

```LEFT(NAME, 3)```

### RIGHT

```RIGHT(string, int)```

Returns the rightmost number of characters. This method returns a **STRING**.

Example:

```RIGHT(NAME, 3)```

### LOCATE / INSTR / POSITION

```LOCATE(searchString, string[, startInt])```

```INSTR(string, searchString[, startInt])```

```POSITION(searchString, string)```

Returns the location of a search string in a string. If a start position is used, the characters before it are ignored. If position is negative, the rightmost location is returned. `0` is returned if the search string is not found. Please note this function is case sensitive, even if the parameters are not. This method returns an **INT**.

Example:

```LOCATE('.', NAME)```

### LPAD

```LPAD(string, int[, string])```

Left pad the string to the specified length. If the length is shorter than the string, it will be truncated at the end. If the padding string is not set, spaces will be used. This method returns a **STRING**.

Example:

```LPAD(AMOUNT, 10, '*')```

### RPAD

```RPAD(string, int[, string])```

Right pad the string to the specified length. If the length is shorter than the string, it will be truncated. If the padding string is not set, spaces will be used. This method returns a **STRING**.

Example:

```RPAD(TEXT, 10, '-')```

### LTRIM

```LTRIM(string[, characterToTrimString])```

Removes all leading spaces or other specified characters from a string. This method returns a **STRING**.

Example:

```LTRIM(NAME)```

### RTRIM

```RTRIM(string[, characterToTrimString])```

Removes all trailing spaces or other specified characters from a string. This method returns a **STRING**.

Example:

```RTRIM(NAME)```

### TRIM

```TRIM(string[, characterToTrimString])```

Removes all leading spaces and trailing spaces or other specified characters from a string. This method returns a **STRING**.

Example:

```TRIM(NAME)```

### REGEXP_REPLACE

```REGEXP_REPLACE(inputString, regexString, replacementString[, flagsString])```

Replaces each substring that matches a regular expression. For details, see the Java `String.replaceAll()` method. If any parameter is null (except optional `flagsString` parameter), the result is null.

Flags values are limited to `'i'`, `'c'`, `'n'`, `'m'`. Other symbols cause exception. Multiple symbols could be used in one `flagsString` parameter (like `'im'`). Later flags override first ones, for example `'ic'` is equivalent to case sensitive matching `'c'`.

- `'i'` enables case insensitive matching (`Pattern.CASE_INSENSITIVE`)
- `'c'` disables case insensitive matching (`Pattern.CASE_INSENSITIVE`)
- `'n'` allows the period to match the newline character (`Pattern.DOTALL`)
- `'m'` enables multiline mode (`Pattern.MULTILINE`)

This method returns a **STRING**.

Example:

```REGEXP_REPLACE('Hello    World', ' +', ' ')```

```REGEXP_REPLACE('Hello WWWWorld', 'w+', 'W', 'i')```

### REGEXP_LIKE

```REGEXP_LIKE(inputString, regexString[, flagsString])```

Matches string to a regular expression. For details, see the Java `Matcher.find()` method. If any parameter is null (except optional `flagsString` parameter), the result is null.

Flags values are limited to `'i'`, `'c'`, `'n'`, `'m'`. Other symbols cause exception. Multiple symbols could be used in one `flagsString` parameter (like `'im'`). Later flags override first ones, for example `'ic'` is equivalent to case sensitive matching `'c'`.

- `'i'` enables case insensitive matching (`Pattern.CASE_INSENSITIVE`)
- `'c'` disables case insensitive matching (`Pattern.CASE_INSENSITIVE`)
- `'n'` allows the period to match the newline character (`Pattern.DOTALL`)
- `'m'` enables multiline mode (`Pattern.MULTILINE`)

This method returns a **BOOLEAN**.

Example:

```REGEXP_LIKE('Hello    World', '[A-Z ]*', 'i')```

### REGEXP_SUBSTR

```REGEXP_SUBSTR(inputString, regexString[, positionInt, occurrenceInt, flagsString, groupInt])```

Matches string to a regular expression and returns the matched substring. For details, see the `java.util.regex.Pattern` and related functionality.

The parameter `position` specifies where in `inputString` the match should start. `Occurrence` indicates which occurrence of pattern in `inputString` to search for.

Flags values are limited to `'i'`, `'c'`, `'n'`, `'m'`. Other symbols cause exception. Multiple symbols could be used in one `flagsString` parameter (like `'im'`). Later flags override first ones, for example `'ic'` is equivalent to case sensitive matching `'c'`.

- `'i'` enables case insensitive matching (`Pattern.CASE_INSENSITIVE`)
- `'c'` disables case insensitive matching (`Pattern.CASE_INSENSITIVE`)
- `'n'` allows the period to match the newline character (`Pattern.DOTALL`)
- `'m'` enables multiline mode (`Pattern.MULTILINE`)

If the pattern has groups, the `group` parameter can be used to specify which group to return.

This method returns a **STRING**.

Example:

```REGEXP_SUBSTR('2020-10-01', '\d{4}')```

```REGEXP_SUBSTR('2020-10-01', '(\d{4})-(\d{2})-(\d{2})', 1, 1, NULL, 2)```

### REPEAT

```REPEAT(string, int)```

Returns a string repeated some number of times. This method returns a **STRING**.

Example:

```REPEAT(NAME || ' ', 10)```

### REPLACE

```REPLACE(string, searchString[, replacementString])```

Replaces all occurrences of a search string in a text with another string. If no replacement is specified, the search string is removed from the original string. If any parameter is null, the result is null. This method returns a **STRING**.

Example:

```REPLACE(NAME, ' ')```

### SPLIT

```SPLIT(string, delimiterString)```

Splits a string into an array. This method returns an **ARRAY**.

Example:

```SELECT SPLIT(test, ';') AS arrays```

### SOUNDEX

```SOUNDEX(string)```

Returns a four character code representing the sound of a string, or null if parameter is null. See [Soundex](https://en.wikipedia.org/wiki/Soundex) for more information. This method returns a **STRING**.

Example:

```SOUNDEX(NAME)```

### SPACE

```SPACE(int)```

Returns a string consisting of a number of spaces. This method returns a **STRING**.

Example:

```SPACE(80)```

### SUBSTRING / SUBSTR

```SUBSTRING | SUBSTR (string, startInt[, lengthInt])```

Returns a substring of a string starting at a position. If the start index is negative, then the start index is relative to the end of the string. The length is optional. This method returns a **STRING**.

Example:

```sql
CALL SUBSTRING('[Hello]', 2);

CALL SUBSTRING('hour', 3, 2);
```
### TO_CHAR

```TO_CHAR(value[, formatString])```

Oracle-compatible `TO_CHAR` function that can format a timestamp, a number, or text. This method returns a **STRING**.

Example:

```CALL TO_CHAR(SYS_TIME, 'yyyy-MM-dd HH:mm:ss')```

### TRANSLATE

```TRANSLATE(value, searchString, replacementString)```

Oracle-compatible `TRANSLATE` function that replaces a sequence of characters in a string with another set of characters. This method returns a **STRING**.

Example:

```CALL TRANSLATE('Hello world', 'eo', 'EO')```

## Numeric Functions

### ABS

```ABS(numeric)```

Returns the absolute value of a specified value. The returned value is of the same data type as the parameter.

Note that TINYINT, SMALLINT, INT, and BIGINT data types cannot represent absolute values of their minimum negative values, because they have more negative values than positive. For example, for INT data type allowed values are from -2147483648 to 2147483647. `ABS(-2147483648)` should be 2147483648, but this value is not allowed for this data type. It leads to an exception. To avoid it cast the argument of this function to a higher data type.

This method returns a **NUMERIC** type (same as input).

Example:

```ABS(I)```

### ACOS

```ACOS(numeric)```

Calculates the arc cosine. See also Java `Math.acos`. This method returns a **DOUBLE**.

Example:

```ACOS(D)```

### ARRAY_MAX

```ARRAY_MAX(array)```

Returns the maximum value of the expression. This method returns the **same type as the array element**.

Example:

```ARRAY_MAX(I)```

### ARRAY_MIN

```ARRAY_MIN(array)```

Returns the minimum value of the expression. This method returns the **same type as the array element**.

Example:

```ARRAY_MIN(I)```

### ASIN

```ASIN(numeric)```

Calculates the arc sine. See also Java `Math.asin`. This method returns a **DOUBLE**.

Example:

```ASIN(D)```

### ATAN

```ATAN(numeric)```

Calculates the arc tangent. See also Java `Math.atan`. This method returns a **DOUBLE**.

Example:

```ATAN(D)```

### COS

```COS(numeric)```

Calculates the trigonometric cosine. See also Java `Math.cos`. This method returns a **DOUBLE**.

Example:

```COS(ANGLE)```

### COSH

```COSH(numeric)```

Calculates the hyperbolic cosine. See also Java `Math.cosh`. This method returns a **DOUBLE**.

Example:

```COSH(X)```

### COT

```COT(numeric)```

Calculates the trigonometric cotangent (`1/TAN(ANGLE)`). See also Java Math.* functions. This method returns a **DOUBLE**.

Example:

```COT(ANGLE)```

### SIN

```SIN(numeric)```

Calculates the trigonometric sine. See also Java `Math.sin`. This method returns a **DOUBLE**.

Example:

```SIN(ANGLE)```

### SINH

```SINH(numeric)```

Calculates the hyperbolic sine. See also Java `Math.sinh`. This method returns a **DOUBLE**.

Example:

```SINH(ANGLE)```

### TAN

```TAN(numeric)```

Calculates the trigonometric tangent. See also Java `Math.tan`. This method returns a **DOUBLE**.

Example:

```TAN(ANGLE)```

### TANH

```TANH(numeric)```

Calculates the hyperbolic tangent. See also Java `Math.tanh`. This method returns a **DOUBLE**.

Example:

```TANH(X)```

### MOD

```MOD(dividendNumeric, divisorNumeric)```

The modulus expression.

Result has the same type as divisor. Result is NULL if either of arguments is NULL. If divisor is 0, an exception is raised. Result has the same sign as dividend or is equal to 0.

Usually arguments should have scale 0, but it isn't required by H2.

This method returns a **NUMERIC** type (same as divisor).

Example:

```MOD(A, B)```

### CEIL / CEILING

```CEIL | CEILING (numeric)```

Returns the smallest integer value that is greater than or equal to the argument. This method returns a **NUMERIC** type (same as input) with scale set to 0 and adjusted precision, if applicable.

Example:

```CEIL(A)```

### EXP

```EXP(numeric)```

See also Java `Math.exp`. This method returns a **DOUBLE**.

Example:

```EXP(A)```

### FLOOR

```FLOOR(numeric)```

Returns the largest integer less than or equal to the argument. This method returns the **same type as the argument** with scale set to 0.

Example:

```FLOOR(A)```

### LN

```LN(numeric)```

Calculates the natural (base e) logarithm as a double value. Argument must be a positive numeric value. This method returns a **DOUBLE**.

Example:

```LN(A)```

### LOG

```LOG(baseNumeric, numeric)```

Calculates the logarithm with the specified base as a double value. Argument and base must be positive numeric values. Base cannot be equal to 1.

The default base is e (natural logarithm); in PostgreSQL mode the default base is base 10. In MSSQLServer mode the optional base is specified after the argument.

Single-argument variant of `LOG` function is deprecated; use `LN` or `LOG10` instead.

This method returns a **DOUBLE**.

Example:

```LOG(2, A)```

### LOG10

```LOG10(numeric)```

Calculates the base 10 logarithm as a double value. Argument must be a positive numeric value. This method returns a **DOUBLE**.

Example:

```LOG10(A)```

### RADIANS

```RADIANS(numeric)```

See also Java `Math.toRadians`. This method returns a **DOUBLE**.

Example:

```RADIANS(A)```

### SQRT

```SQRT(numeric)```

See also Java `Math.sqrt`. This method returns a **DOUBLE**.

Example:

```SQRT(A)```

### PI

```PI()```

See also Java `Math.PI`. This method returns a **DOUBLE**.

Example:

```PI()```

### POWER

```POWER(numeric, numeric)```

See also Java `Math.pow`. This method returns a **DOUBLE**.

Example:

```POWER(A, B)```

### RAND / RANDOM

```RAND | RANDOM([ int ])```

Calling the function without a parameter returns the next pseudo-random number. Calling it with a parameter seeds the session's random number generator. This method returns a **DOUBLE** between 0 (inclusive) and 1 (exclusive).

Example:

```RAND()```

### ROUND

```ROUND(numeric[, digitsInt])```

Rounds to a number of fractional digits. This method returns a value of the same type as the argument, but with adjusted precision and scale, if applicable.

Example:

```ROUND(N, 2)```

### SIGN

```SIGN(numeric)```

Returns `-1` if the value is smaller than 0, `0` if zero or NaN, and otherwise `1`. This method returns an **INT**.

Example:

```SIGN(N)```

### TRUNC

```TRUNC | TRUNCATE(numeric[, digitsInt])```

Truncates a numeric value to the specified number of digits (toward zero). This method returns the **same type as the argument** with adjusted precision and scale, if applicable.

Example:

```TRUNC(N, 2)```

### TRIM_SCALE

```TRIM_SCALE(numeric)```

Reduces the scale of a number by removing trailing zeroes. This method returns a **STRING** with adjusted scale.

Example:

```TRIM_SCALE(N)```

## Time and Date Functions

### CURRENT_DATE

```CURRENT_DATE [()]```

Returns the current date. These functions return the same value within a transaction (default) or within a command depending on database mode. This method returns a **DATE**.

Example:

```CURRENT_DATE```

### CURRENT_TIME

```CURRENT_TIME [()]```

Returns the current time with system time zone. The actual maximum available precision depends on the operating system and JVM and can be 3 (milliseconds) or higher. Higher precision is not available before Java 9. This method returns a **TIME WITH TIME ZONE**.

Example:

```CURRENT_TIME```

### CURRENT_TIMESTAMP / NOW

```CURRENT_TIMESTAMP[()] | NOW()```

Returns the current timestamp with system time zone. The actual maximum available precision depends on the operating system and JVM and can be 3 (milliseconds) or higher. Higher precision is not available before Java 9. This method returns a **TIMESTAMP WITH TIME ZONE**.

Example:

```CURRENT_TIMESTAMP```

### DATEADD / TIMESTAMPADD

```DATEADD | TIMESTAMPADD(dateAndTime, addIntLong, datetimeFieldString)```

Adds units to a date-time value. The `datetimeFieldString` indicates the unit. Use negative values to subtract units. `addIntLong` may be a long value when manipulating milliseconds, microseconds, or nanoseconds; otherwise its range is restricted to int.  
This method returns a **DATE**, **TIME**, or **TIMESTAMP** depending on the specified value and unit. If the specified field is HOUR, MINUTE, SECOND, MILLISECOND, etc., and value is a DATE, `DATEADD` returns a combined TIMESTAMP. Fields DAY, MONTH, YEAR, WEEK, etc., are not allowed for TIME values.

Example:

```DATEADD(CREATED, 1, 'MONTH')```

### DATEDIFF

```DATEDIFF(aDateAndTime, bDateAndTime, datetimeFieldString)```

Returns the number of crossed unit boundaries between two date-time values. This method returns a **LONG**. The `datetimeField` indicates the unit.

Example:

```DATEDIFF(T1.CREATED, T2.CREATED, 'MONTH')```

### DATE_TRUNC

```DATE_TRUNC(dateAndTime, datetimeFieldString)```

Truncates the specified date-time value to the specified field. This method returns the **same type as the input date/time**.

Example:

```DATE_TRUNC(CREATED, 'DAY')```

### DAYNAME

```DAYNAME(dateAndTime)```

Returns the name of the day (in English). This method returns a **STRING**.

Example:

```DAYNAME(CREATED)```

### DAY_OF_MONTH

```DAY_OF_MONTH(dateAndTime)```

Returns the day of the month (1-31). This method returns an **INT**.

Example:

```DAY_OF_MONTH(CREATED)```

### DAY_OF_WEEK

```DAY_OF_WEEK(dateAndTime)```

Returns the day of the week (1-7) (Monday-Sunday), locale-specific. This method returns an **INT**.

Example:

```DAY_OF_WEEK(CREATED)```

### DAY_OF_YEAR

```DAY_OF_YEAR(dateAndTime)```

Returns the day of the year (1-366). This method returns an **INT**.

Example:

```DAY_OF_YEAR(CREATED)```

### EXTRACT

```EXTRACT(datetimeField FROM dateAndTime)```

Returns a value of the specific time unit from a date/time value. This method returns a **NUMERIC** value with `EPOCH` field and an **INT** for all other fields.

The following are valid field names for `EXTRACT`:

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

```FORMATDATETIME(dateAndTime, formatString)```

Formats a date, time, or timestamp as a string. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see `java.time.format.DateTimeFormatter`. This method returns a **STRING**.

Example:

```CALL FORMATDATETIME(CREATED, 'yyyy-MM-dd HH:mm:ss')```

### HOUR

```HOUR(dateAndTime)```

Returns the hour (0-23) from a date/time value. This method returns an **INT**.

Example:

```HOUR(CREATED)```

### MINUTE

```MINUTE(dateAndTime)```

Returns the minute (0-59) from a date/time value. This method returns an **INT**.

This function is deprecated; use `EXTRACT` instead.

Example:

```MINUTE(CREATED)```

### MONTH

```MONTH(dateAndTime)```

Returns the month (1-12) from a date/time value. This method returns an **INT**.

This function is deprecated; use `EXTRACT` instead.

Example:

```MONTH(CREATED)```

### MONTHNAME

```MONTHNAME(dateAndTime)```

Returns the name of the month (in English). This method returns a **STRING**.

Example:

```MONTHNAME(CREATED)```

### IS_DATE

```IS_DATE(string, formatString)```

Parses a string and returns a boolean value. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see `java.time.format.DateTimeFormatter`. This method returns a **BOOLEAN**.

Example:

```CALL IS_DATE('2021-04-08 13:34:45', 'yyyy-MM-dd HH:mm:ss')```

### PARSEDATETIME / TO_DATE

```PARSEDATETIME | TO_DATE(string, formatString)```

Parses a string and returns a **TIMESTAMP WITH TIME ZONE** value. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see `java.time.format.DateTimeFormatter`.

Example:

```CALL PARSEDATETIME('2021-04-08 13:34:45', 'yyyy-MM-dd HH:mm:ss')```

```CALL TO_DATE('2021-04-08'T'13:34:45', 'yyyy-MM-dd''T''HH:mm:ss')```

Note: When filling in `'` in SQL functions, it needs to be escaped to `''`.

### QUARTER

```QUARTER(dateAndTime)```

Returns the quarter (1-4) from a date/time value. This method returns an **INT**.

Example:

```QUARTER(CREATED)```

### SECOND

```SECOND(dateAndTime)```

Returns the second (0-59) from a date/time value. This method returns an **INT**.

This function is deprecated; use `EXTRACT` instead.

Example:

```SECOND(CREATED)```

### WEEK

```WEEK(dateAndTime)```

Returns the week (1-53) from a date/time value. This method returns an **INT**.

This function uses the current system locale.

Example:

```WEEK(CREATED)```

### YEAR

```YEAR(dateAndTime)```

Returns the year from a date/time value. This method returns an **INT**.

Example:

```YEAR(CREATED)```

### FROM_UNIXTIME

```FROM_UNIXTIME(unixtime, formatString, timeZone)```

Converts the number of seconds from the UNIX epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment.

The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see `java.time.format.DateTimeFormatter`.

`timeZone` is optional; the default value is the system's time zone. `timeZone` value can be a `UTC+` timezone offset, for example, `UTC+8` represents the Asia/Shanghai time zone, see `java.time.ZoneId`.

This method returns a **STRING**.

Example:

```CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss')```  
```CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss', 'UTC+6')```

## System Functions

### CAST

```CAST(value AS dataType)```

Converts a value to another data type.

Supported data types: STRING | VARCHAR, TINYINT, SMALLINT, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES, BOOLEAN

This method returns the specified **DATA TYPE**.

Example:

```CAST(NAME AS INT)```  
```CAST(FLAG AS BOOLEAN)```

Note: Converts a value to a BOOLEAN data type according to the following rules:  
1. If the value can be interpreted as a boolean string (`'true'` or `'false'`), it returns the corresponding boolean value.  
2. If the value can be interpreted as a numeric value (`1` or `0`), it returns `true` for `1` and `false` for `0`.  
3. If the value cannot be interpreted according to the above rules, it throws a `TransformException`.

### TRY_CAST

```TRY_CAST(value AS dataType)```

This function is similar to `CAST`, but when the conversion fails, it returns `NULL` instead of throwing an exception.

Supported data types: STRING | VARCHAR, TINYINT, SMALLINT, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES

This method returns the specified **DATA TYPE** or **NULL**.

Example:

```TRY_CAST(NAME AS INT)```

### COALESCE

```COALESCE(aValue, bValue [,...])```

Returns the first value that is not null. This method returns the **same type as the first non-null value**.

Example:

```COALESCE(A, B, C)```

### IFNULL

```IFNULL(aValue, bValue)```

Returns the first value that is not null. This method returns the **same type as the first non-null value**.

Example:

```IFNULL(A, B)```

### NULLIF

```NULLIF(aValue, bValue)```

Returns NULL if `a` is equal to `b`, otherwise returns `a`. This method returns the **same type as aValue**.

Example:

```NULLIF(A, B)```

### MULTI_IF

```MULTI_IF(condition1, value1, condition2, value2, ... conditionN, valueN, bValue)```

Returns the first value for which the corresponding condition is true. If all conditions are false, it returns the last value. This method returns the **type of the matched value**.

Example:

```MULTI_IF(A > 1, 'A', B > 1, 'B', C > 1, 'C', 'D')```

### CASE WHEN

Used to determine whether the condition is valid and return different values according to different judgments. This method returns the **type of the selected value**.

Example:

```sql
SELECT
  CASE
    WHEN c_string IN ('c_string') THEN 1
    ELSE 0
  END AS c_string_1,
  CASE
    WHEN c_string NOT IN ('c_string') THEN 1
    ELSE 0
  END AS c_string_0,
  CASE
    WHEN c_tinyint = 117
      AND TO_CHAR(c_boolean) = 'true' THEN 1
    ELSE 0
  END AS c_tinyint_boolean_1,
  CASE
    WHEN c_tinyint != 117
      AND TO_CHAR(c_boolean) = 'true' THEN 1
    ELSE 0
  END AS c_tinyint_boolean_0,
  CASE
    WHEN c_tinyint != 117
      OR TO_CHAR(c_boolean) = 'true' THEN 1
    ELSE 0
  END AS c_tinyint_boolean_or_1,
  CASE
    WHEN c_int > 1
      AND c_bigint > 1
      AND c_float > 1
      AND c_double > 1
      AND c_decimal > 1 THEN 1
    ELSE 0
  END AS c_number_1,
  CASE
    WHEN c_tinyint <> 117 THEN 1
    ELSE 0
  END AS c_number_0,
  CASE
    WHEN c_boolean THEN 1
    ELSE 0
  END AS c_boolean_0
FROM dual;
```
Example:

```sql
CASE WHEN c_string IN ('c_string') THEN 1 ELSE 0 END
CASE WHEN c_string IN ('c_string') THEN TRUE ELSE FALSE END
```

### UUID

```UUID()```

Generates a UUID through a Java function. This method returns a **STRING**.

Example:

```sql
SELECT UUID() AS seatunnel_uuid;
```
### ARRAY

```ARRAY<T> array(T, ...)```

Creates an array consisting of variadic elements and returns it. Here, `T` can be either “column” or “literal”. This method returns an **ARRAY**.

Example:

```sql
SELECT ARRAY(1, 2, 3) AS arrays;
SELECT ARRAY('c_1', 2, 3.12) AS arrays;
SELECT ARRAY(column1, column2, column3) AS arrays;
```
Note: Currently only string, double, long, int types are supported.

### LATERAL VIEW

#### EXPLODE

Used to flatten array columns into multiple rows. It applies the `EXPLODE` function to an array and generates a new row for each element.

- **EXPLODE**: Converts an array column into multiple rows. No rows are generated if the array is NULL or empty.  
- **OUTER EXPLODE**: Returns NULL when the array is NULL or empty, ensuring at least one row is generated.  
- `EXPLODE(SPLIT(field_name, separator))`: Splits a string into an array using the specified separator, then explodes it into rows.  
- `EXPLODE(ARRAY(value1, value2, ...))`: Explodes a custom-defined array into multiple rows.  

This method returns multiple rows, each containing an **element of the array**.

Example:

```sql
SELECT * FROM dual
  LATERAL VIEW EXPLODE(SPLIT(NAME, ',')) AS NAME
  LATERAL VIEW EXPLODE(SPLIT(pk_id, ';')) AS pk_id
  LATERAL VIEW OUTER EXPLODE(age) AS age
  LATERAL VIEW OUTER EXPLODE(ARRAY(1, 1)) AS num;
```
