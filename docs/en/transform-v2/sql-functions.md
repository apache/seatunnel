# SQL Functions

> The Functions of SQL transform plugin

## String Functions

### ASCII

```ASCII(string)```

Returns the ```ASCII``` value of the first character in the string. This method returns an int.

Example:

ASCII('Hi')

### BIT_LENGTH

```BIT_LENGTH(bytes)```

Returns the number of bits in a binary string. This method returns a long.

Example:

BIT_LENGTH(NAME)

### CHAR_LENGTH / LENGTH

```CHAR_LENGTH | LENGTH (string)```

Returns the number of characters in a character string. This method returns a long.

Example:

CHAR_LENGTH(NAME)

### OCTET_LENGTH

```OCTET_LENGTH(bytes)```

Returns the number of bytes in a binary string. This method returns a long.

Example:

OCTET_LENGTH(NAME)

### CHAR / CHR

```CHAR | CHR (int)```

Returns the character that represents the ASCII value. This method returns a string.

Example:

CHAR(65)

### CONCAT

```CONCAT(string, string[, string ...] )```

Combines strings. Unlike with the operator ```||```, **NULL** parameters are ignored, and do not cause the result to become **NULL**. If all parameters are NULL the result is an empty string. This method returns a string.

Example:

CONCAT(NAME, '_')

### CONCAT_WS

```CONCAT_WS(separatorString, string, string[, string ...] )```

Combines strings with separator. If separator is **NULL** it is treated like an empty string. Other **NULL** parameters are ignored. Remaining **non-NULL** parameters, if any, are concatenated with the specified separator. If there are no remaining parameters the result is an empty string. This method returns a string.

Example:

CONCAT_WS(',', NAME, '_')

### HEXTORAW

```HEXTORAW(string)```

Converts a hex representation of a string to a string. 4 hex characters per string character are used.

Example:

HEXTORAW(DATA)

### RAWTOHEX

```RAWTOHEX(string)```

```RAWTOHEX(bytes)```

Converts a string or bytes to the hex representation. 4 hex characters per string character are used. This method returns a string.

Example:

RAWTOHEX(DATA)

### INSERT

```INSERT(originalString, startInt, lengthInt, addString)```

Inserts a additional string into the original string at a specified start position. The length specifies the number of characters that are removed at the start position in the original string. This method returns a string.

Example:

INSERT(NAME, 1, 1, ' ')

### LOWER / LCASE

```LOWER | LCASE (string)```

Converts a string to lowercase.

Example:

LOWER(NAME)

### UPPER / UCASE

```UPPER | UCASE (string)```

Converts a string to uppercase.

Example:

UPPER(NAME)

### LEFT

```LEFT(string, int)```

Returns the leftmost number of characters.

Example:

LEFT(NAME, 3)

### RIGHT

```RIGHT(string, int)```

Returns the rightmost number of characters.

Example:

RIGHT(NAME, 3)

### LOCATE / INSTR / POSITION

```LOCATE(searchString, string[, startInit])```

```INSTR(string, searchString[, startInit])```

```POSITION(searchString, string)```

Returns the location of a search string in a string. If a start position is used, the characters before it are ignored. If position is negative, the rightmost location is returned. 0 is returned if the search string is not found. Please note this function is case sensitive, even if the parameters are not.

Example:

LOCATE('.', NAME)

### LPAD

```LPAD(string ,int[, string])```

Left pad the string to the specified length. If the length is shorter than the string, it will be truncated at the end. If the padding string is not set, spaces will be used.

Example:

LPAD(AMOUNT, 10, '*')

### RPAD

```RPAD(string, int[, string])```

Right pad the string to the specified length. If the length is shorter than the string, it will be truncated. If the padding string is not set, spaces will be used.

Example:

RPAD(TEXT, 10, '-')

### LTRIM

```LTRIM(string[, characterToTrimString])```

Removes all leading spaces or other specified characters from a string.

This function is deprecated, use TRIM instead of it.

Example:

LTRIM(NAME)

### RTRIM

```RTRIM(string[, characterToTrimString])```

Removes all trailing spaces or other specified characters from a string.

This function is deprecated, use TRIM instead of it.

Example:

RTRIM(NAME)

### TRIM

```TRIM(string[, characterToTrimString])```

Removes all leading spaces or other specified characters from a string.

This function is deprecated, use TRIM instead of it.

Example:

LTRIM(NAME)

### REGEXP_REPLACE

```REGEXP_REPLACE(inputString, regexString, replacementString[, flagsString])```

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

```REGEXP_LIKE(inputString, regexString[, flagsString])```

Matches string to a regular expression. For details, see the Java Matcher.find() method. If any parameter is null (except optional flagsString parameter), the result is null.

Flags values are limited to 'i', 'c', 'n', 'm'. Other symbols cause exception. Multiple symbols could be used in one flagsString parameter (like 'im'). Later flags override first ones, for example 'ic' is equivalent to case sensitive matching 'c'.

'i' enables case insensitive matching (Pattern.CASE_INSENSITIVE)

'c' disables case insensitive matching (Pattern.CASE_INSENSITIVE)

'n' allows the period to match the newline character (Pattern.DOTALL)

'm' enables multiline mode (Pattern.MULTILINE)

Example:

REGEXP_LIKE('Hello    World', '[A-Z ]*', 'i')

### REGEXP_SUBSTR

```REGEXP_SUBSTR(inputString, regexString[, positionInt, occurrenceInt, flagsString, groupInt])```

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

```REPEAT(string, int)```

Returns a string repeated some number of times.

Example:

REPEAT(NAME || ' ', 10)

### REPLACE

```REPLACE(string, searchString[, replacementString])```

Replaces all occurrences of a search string in a text with another string. If no replacement is specified, the search string is removed from the original string. If any parameter is null, the result is null.

Example:

REPLACE(NAME, ' ')

### SPLIT

Split a string into an array.

Example:

select SPLIT(test,';') as arrays

### SOUNDEX

```SOUNDEX(string)```

Returns a four character code representing the sound of a string. This method returns a string, or null if parameter is null. See https://en.wikipedia.org/wiki/Soundex for more information.

Example:

SOUNDEX(NAME)

### SPACE

```SPACE(int)```

Returns a string consisting of a number of spaces.

Example:

SPACE(80)

### SUBSTRING / SUBSTR

```SUBSTRING | SUBSTR (string, startInt[, lengthInt ])```

Returns a substring of a string starting at a position. If the start index is negative, then the start index is relative to the end of the string. The length is optional.

Example:

CALL SUBSTRING('[Hello]', 2);
CALL SUBSTRING('hour', 3, 2);

### TO_CHAR

```TO_CHAR(value[, formatString])```

Oracle-compatible TO_CHAR function that can format a timestamp, a number, or text.

Example:

CALL TO_CHAR(SYS_TIME, 'yyyy-MM-dd HH:mm:ss')

### TRANSLATE

```TRANSLATE(value, searchString, replacementString)```

Oracle-compatible TRANSLATE function that replaces a sequence of characters in a string with another set of characters.

Example:

CALL TRANSLATE('Hello world', 'eo', 'EO')

## Numeric Functions

### ABS

```ABS(numeric)```

Returns the absolute value of a specified value. The returned value is of the same data type as the parameter.

Note that TINYINT, SMALLINT, INT, and BIGINT data types cannot represent absolute values of their minimum negative values, because they have more negative values than positive. For example, for INT data type allowed values are from -2147483648 to 2147483647. ABS(-2147483648) should be 2147483648, but this value is not allowed for this data type. It leads to an exception. To avoid it cast argument of this function to a higher data type.

Example:

ABS(I)

### ACOS

```ACOS(numeric)```

Calculate the arc cosine. See also Java Math.acos. This method returns a double.

Example:

ACOS(D)

### ASIN

```ASIN(numeric)```

Calculate the arc sine. See also Java Math.asin. This method returns a double.

Example:

ASIN(D)

### ATAN

```ATAN(numeric)```

Calculate the arc tangent. See also Java Math.atan. This method returns a double.

Example:

ATAN(D)

### COS

```COS(numeric)```

Calculate the trigonometric cosine. See also Java Math.cos. This method returns a double.

Example:

COS(ANGLE)

### COSH

```COSH(numeric)```

Calculate the hyperbolic cosine. See also Java Math.cosh. This method returns a double.

Example:

COSH(X)

### COT

```COT(numeric)```

Calculate the trigonometric cotangent (1/TAN(ANGLE)). See also Java Math.* functions. This method returns a double.

Example:

COT(ANGLE)

### SIN

```SIN(numeric)```

Calculate the trigonometric sine. See also Java Math.sin. This method returns a double.

Example:

SIN(ANGLE)

### SINH

```SINH(numeric)```

Calculate the hyperbolic sine. See also Java Math.sinh. This method returns a double.

Example:

SINH(ANGLE)

### TAN

```TAN(numeric)```

Calculate the trigonometric tangent. See also Java Math.tan. This method returns a double.

Example:

TAN(ANGLE)

### TANH

```TANH(numeric)```

Calculate the hyperbolic tangent. See also Java Math.tanh. This method returns a double.

Example:

TANH(X)

### MOD

```MOD(dividendNumeric, divisorNumeric )```

The modulus expression.

Result has the same type as divisor. Result is NULL if either of arguments is NULL. If divisor is 0, an exception is raised. Result has the same sign as dividend or is equal to 0.

Usually arguments should have scale 0, but it isn't required by H2.

Example:

MOD(A, B)

### CEIL / CEILING

```CEIL | CEILING (numeric)```

Returns the smallest integer value that is greater than or equal to the argument. This method returns value of the same type as argument, but with scale set to 0 and adjusted precision, if applicable.

Example:

CEIL(A)

### EXP

```EXP(numeric)```

See also Java Math.exp. This method returns a double.

Example:

EXP(A)

### FLOOR

```FLOOR(numeric)```

Returns the largest integer value that is less than or equal to the argument. This method returns value of the same type as argument, but with scale set to 0 and adjusted precision, if applicable.

Example:

FLOOR(A)

### LN

```LN(numeric)```

Calculates the natural (base e) logarithm as a double value. Argument must be a positive numeric value.

Example:

LN(A)

### LOG

```LOG(baseNumeric, numeric)```

Calculates the logarithm with specified base as a double value. Argument and base must be positive numeric values. Base cannot be equal to 1.

The default base is e (natural logarithm), in the PostgreSQL mode the default base is base 10. In MSSQLServer mode the optional base is specified after the argument.

Single-argument variant of LOG function is deprecated, use LN or LOG10 instead.

Example:

LOG(2, A)

### LOG10

```LOG10(numeric)```

Calculates the base 10 logarithm as a double value. Argument must be a positive numeric value.

Example:

LOG10(A)

### RADIANS

```RADIANS(numeric)```

See also Java Math.toRadians. This method returns a double.

Example:

RADIANS(A)

### SQRT

```SQRT(numeric)```

See also Java Math.sqrt. This method returns a double.

Example:

SQRT(A)

### PI

```PI()```

See also Java Math.PI. This method returns a double.

Example:

PI()

### POWER

```POWER(numeric, numeric)```

See also Java Math.pow. This method returns a double.

Example:

POWER(A, B)

### RAND / RANDOM

```RAND | RANDOM([ int ])```

Calling the function without parameter returns the next a pseudo random number. Calling it with an parameter seeds the session's random number generator. This method returns a double between 0 (including) and 1 (excluding).

Example:

RAND()

### ROUND

```ROUND(numeric[, digitsInt])```

Rounds to a number of fractional digits. This method returns value of the same type as argument, but with adjusted precision and scale, if applicable.

Example:

ROUND(N, 2)

### SIGN

```SIGN(numeric)```

Returns -1 if the value is smaller than 0, 0 if zero or NaN, and otherwise 1.

Example:

SIGN(N)

### TRUNC

```TRUNC | TRUNCATE(numeric[, digitsInt])```

When a numeric argument is specified, truncates it to a number of digits (to the next value closer to 0) and returns value of the same type as argument, but with adjusted precision and scale, if applicable.

Example:

TRUNC(N, 2)

## Time and Date Functions

### CURRENT_DATE

```CURRENT_DATE [()]```

Returns the current date.

These functions return the same value within a transaction (default) or within a command depending on database mode.

Example:

CURRENT_DATE

### CURRENT_TIME

```CURRENT_TIME [()]```

Returns the current time with system time zone. The actual maximum available precision depends on operating system and JVM and can be 3 (milliseconds) or higher. Higher precision is not available before Java 9.

Example:

CURRENT_TIME

### CURRENT_TIMESTAMP / NOW

```CURRENT_TIMESTAMP[()] | NOW()```

Returns the current timestamp with system time zone. The actual maximum available precision depends on operating system and JVM and can be 3 (milliseconds) or higher. Higher precision is not available before Java 9.

Example:

CURRENT_TIMESTAMP

### DATEADD / TIMESTAMPADD

```DATEADD| TIMESTAMPADD(dateAndTime, addIntLong, datetimeFieldString)```

Adds units to a date-time value. The datetimeFieldString indicates the unit. Use negative values to subtract units. addIntLong may be a long value when manipulating milliseconds, microseconds, or nanoseconds otherwise its range is restricted to int. This method returns a value with the same type as specified value if unit is compatible with this value. If specified field is a HOUR, MINUTE, SECOND, MILLISECOND, etc and value is a DATE value DATEADD returns combined TIMESTAMP. Fields DAY, MONTH, YEAR, WEEK, etc are not allowed for TIME values.

Example:

DATEADD(CREATED, 1, 'MONTH')

### DATEDIFF

```DATEDIFF(aDateAndTime, bDateAndTime, datetimeFieldString)```

Returns the number of crossed unit boundaries between two date-time values. This method returns a long. The datetimeField indicates the unit.

Example:

DATEDIFF(T1.CREATED, T2.CREATED, 'MONTH')

### DATE_TRUNC

```DATE_TRUNC (dateAndTime, datetimeFieldString)```

Truncates the specified date-time value to the specified field.

Example:

DATE_TRUNC(CREATED, 'DAY');

### DAYNAME

```DAYNAME(dateAndTime)```

Returns the name of the day (in English).

Example:

DAYNAME(CREATED)

### DAY_OF_MONTH

```DAY_OF_MONTH(dateAndTime)```

Returns the day of the month (1-31).

Example:

DAY_OF_MONTH(CREATED)

### DAY_OF_WEEK

```DAY_OF_WEEK(dateAndTime)```

Returns the day of the week (1-7) (Monday-Sunday), locale-specific.

Example:

DAY_OF_WEEK(CREATED)

### DAY_OF_YEAR

```DAY_OF_YEAR(dateAndTime)```

Returns the day of the year (1-366).

Example:

DAY_OF_YEAR(CREATED)

### EXTRACT

```EXTRACT ( datetimeField FROM dateAndTime)```

Returns a value of the specific time unit from a date/time value. This method returns a numeric value with EPOCH field and an int for all other fields.

Example:

EXTRACT(SECOND FROM CURRENT_TIMESTAMP)

### FORMATDATETIME

```FORMATDATETIME (dateAndTime, formatString)```

Formats a date, time or timestamp as a string. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see java.time.format.DateTimeFormatter.

This method returns a string.

Example:

CALL FORMATDATETIME(CREATED, 'yyyy-MM-dd HH:mm:ss')

### HOUR

```HOUR(dateAndTime)```

Returns the hour (0-23) from a date/time value.

Example:

HOUR(CREATED)

### MINUTE

```MINUTE(dateAndTime)```

Returns the minute (0-59) from a date/time value.

This function is deprecated, use EXTRACT instead of it.

Example:

MINUTE(CREATED)

### MONTH

```MONTH(dateAndTime)```

Returns the month (1-12) from a date/time value.

This function is deprecated, use EXTRACT instead of it.

Example:

MONTH(CREATED)

### MONTHNAME

```MONTHNAME(dateAndTime)```

Returns the name of the month (in English).

Example:

MONTHNAME(CREATED)

### IS_DATE

```IS_DATE(string, formatString)```
Parses a string and returns a boolean value. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see java.time.format.DateTimeFormatter.

Example:

CALL IS_DATE('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')

### PARSEDATETIME / TO_DATE

```PARSEDATETIME | TO_DATE(string, formatString)```
Parses a string and returns a TIMESTAMP WITH TIME ZONE value. The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see java.time.format.DateTimeFormatter.

Example:

CALL PARSEDATETIME('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')

### QUARTER

```QUARTER(dateAndTime)```

Returns the quarter (1-4) from a date/time value.

Example:

QUARTER(CREATED)

### SECOND

```SECOND(dateAndTime)```

Returns the second (0-59) from a date/time value.

This function is deprecated, use EXTRACT instead of it.

Example:

SECOND(CREATED)

### WEEK

```WEEK(dateAndTime)```

Returns the week (1-53) from a date/time value.

This function uses the current system locale.

Example:

WEEK(CREATED)

### YEAR

```YEAR(dateAndTime)```

Returns the year from a date/time value.

Example:

YEAR(CREATED)

### FROM_UNIXTIME

```FROM_UNIXTIME (unixtime, formatString,timeZone)```

Convert the number of seconds from the UNIX epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment.

The most important format characters are: y year, M month, d day, H hour, m minute, s second. For details of the format, see `java.time.format.DateTimeFormatter`.

`timeZone` is optional, default value is system's time zone.  `timezone` value can be a `UTC+ timezone offset`, for example, `UTC+8` represents the Asia/Shanghai time zone, see `java.time.ZoneId`

This method returns a string.

Example:

// use default zone

CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss')

or

// use given zone

CALL FROM_UNIXTIME(1672502400, 'yyyy-MM-dd HH:mm:ss','UTC+6')

## System Functions

### CAST

```CAST(value as dataType)```

Converts a value to another data type.

Supported data types: STRING | VARCHAR, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE, DECIMAL(p,s), TIMESTAMP, DATE, TIME, BYTES

Example:

CONVERT(NAME AS INT)

### COALESCE

```COALESCE(aValue, bValue [,...])```

Returns the first value that is not null.

Example:

COALESCE(A, B, C)

### IFNULL

```IFNULL(aValue, bValue)```

Returns the first value that is not null.

Example:

IFNULL(A, B)

### NULLIF

```NULLIF(aValue, bValue)```

Returns NULL if 'a' is equal to 'b', otherwise 'a'.

Example:

NULLIF(A, B)

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
  dual
```

It is used to determine whether the condition is valid and return different values according to different judgments

Example:

case when c_string in ('c_string') then 1 else 0 end

### UUID

```UUID()```

Generate a uuid through java function.

Example:

select UUID() as seatunnel_uuid

### ARRAY

```ARRAY<T> array(T, ...)```
Create an array consisting of variadic elements and return it. Here, T can be either “column” or “literal”.

Example:

select ARRAY(1,2,3) as arrays
select ARRAY('c_1',2,3.12) as arrays
select ARRAY(column1,column2,column3) as arrays

notes: Currently only string, double, long, int types are supported

### LATERAL VIEW 
#### EXPLODE

explode array column to rows.
OUTER EXPLODE will return NULL, while array is NULL or empty
EXPLODE(SPLIT(FIELD_NAME,separator))Used to split string type. The first parameter of SPLIT function  is the field name, the second parameter is the separator
EXPLODE(ARRAY(value1,value2)) Used to custom array type.
```
SELECT * FROM dual 
	LATERAL VIEW EXPLODE ( SPLIT ( NAME, ',' ) ) AS NAME 
	LATERAL VIEW EXPLODE ( SPLIT ( pk_id, ';' ) ) AS pk_id 
	LATERAL VIEW OUTER EXPLODE ( age ) AS age
	LATERAL VIEW OUTER EXPLODE ( ARRAY(1,1) ) AS num
```
