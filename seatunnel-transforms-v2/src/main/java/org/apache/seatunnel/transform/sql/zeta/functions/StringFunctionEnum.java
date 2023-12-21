/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum StringFunctionEnum {
    /**
     * ASCII(string)
     *
     * <p>Returns the ASCII value of the first character in the string. This method returns an int.
     *
     * <p>Example:
     *
     * <p>ASCII('Hi')
     */
    ASCII {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            Integer val =
                    Optional.ofNullable(objects.get(0))
                            .map(origin -> (int) origin.toString().charAt(0))
                            .orElse(null);
            return Pair.of(BasicType.INT_TYPE, val);
        }
    },
    /**
     * BIT_LENGTH(bytes)
     *
     * <p>Returns the number of bits in a binary string. This method returns a long.
     *
     * <p>Example:
     *
     * <p>BIT_LENGTH(NAME)
     */
    BIT_LENGTH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            Long val =
                    Optional.ofNullable(objects.get(0))
                            .map(
                                    origin ->
                                            origin.toString()
                                                            .getBytes(StandardCharsets.UTF_8)
                                                            .length
                                                    * 8L)
                            .orElse(null);
            return Pair.of(BasicType.LONG_TYPE, val);
        }
    },
    /**
     * CHAR_LENGTH | LENGTH (string)
     *
     * <p>Returns the number of characters in a character string. This method returns a long.
     *
     * <p>Example:
     *
     * <p>CHAR_LENGTH(NAME)
     */
    CHAR_LENGTH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            Long val =
                    Optional.ofNullable(objects.get(0))
                            .map(origin -> Long.valueOf(origin.toString().length()))
                            .orElse(null);
            return Pair.of(BasicType.LONG_TYPE, val);
        }
    },
    LENGTH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return CHAR_LENGTH.execute(types, objects);
        }
    },
    /**
     * OCTET_LENGTH(bytes)
     *
     * <p>Returns the number of bytes in a binary string. This method returns a long.
     *
     * <p>Example:
     *
     * <p>OCTET_LENGTH(NAME)
     */
    OCTET_LENGTH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            Long val =
                    Optional.ofNullable(objects.get(0))
                            .map(
                                    origin ->
                                            Long.valueOf(
                                                    origin.toString()
                                                            .getBytes(StandardCharsets.UTF_8)
                                                            .length))
                            .orElse(null);
            return Pair.of(BasicType.LONG_TYPE, val);
        }
    },
    /**
     * CHAR | CHR (int)
     *
     * <p>Returns the character that represents the ASCII value. This method returns a string.
     *
     * <p>Example:
     *
     * <p>CHAR(65)
     */
    CHAR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            String val =
                    Optional.ofNullable(objects.get(0))
                            .map(
                                    origin ->
                                            String.valueOf(
                                                    (char) NumberUtils.toInt(origin.toString())))
                            .orElse(null);
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    CHR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return CHAR.execute(types, objects);
        }
    },
    /**
     * CONCAT(string, string[, string ...] )
     *
     * <p>Combine strings. Unlike with the operator ||, NULL parameters are ignored, and do not
     * cause the result to become NULL. If all parameters are NULL the result is an empty string.
     * This method returns a string.
     *
     * <p>Example:
     *
     * <p>CONCAT(NAME, '_')
     */
    CONCAT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require ge 1");
            }
            String val =
                    objects.stream()
                            .filter(Objects::nonNull)
                            .map(Object::toString)
                            .collect(Collectors.joining());
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * CONCAT_WS(separatorString, string, string[, string ...] )
     *
     * <p>Combine strings with separator. If separator is NULL it is treated like an empty string.
     * Other NULL parameters are ignored. Remaining non-NULL parameters, if any, are concatenated
     * with the specified separator. If there are no remaining parameters the result is an empty
     * string. This method returns a string.
     *
     * <p>Example:
     *
     * <p>CONCAT_WS(',', NAME, '')
     */
    CONCAT_WS {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require ge 2");
            }

            String separator = FunctionUtils.convertToString(objects.get(0), "");
            String val =
                    objects.stream()
                            .filter(Objects::nonNull)
                            .map(Object::toString)
                            .collect(Collectors.joining(separator));
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * HEXTORAW(string)
     *
     * <p>Converts a hex representation of a string to a string. 4 hex characters per string
     * character are used.
     *
     * <p>Example:
     *
     * <p>HEXTORAW(DATA)
     */
    HEXTORAW {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            String val =
                    Optional.ofNullable(objects.get(0))
                            .map(
                                    item -> {
                                        String origin = item.toString();
                                        int len = origin.length();
                                        if (len % 4 != 0) {
                                            return null;
                                        }
                                        StringBuilder builder = new StringBuilder(len / 4);
                                        for (int i = 0; i < len; i += 4) {
                                            builder.append(
                                                    (char)
                                                            Integer.parseInt(
                                                                    origin.substring(i, i + 4),
                                                                    16));
                                        }
                                        return builder.toString();
                                    })
                            .orElse(null);
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * RAWTOHEX(string)
     *
     * <p>RAWTOHEX(bytes)
     *
     * <p>Converts a string or bytes to the hex representation. 4 hex characters per string
     * character are used. This method returns a string.
     *
     * <p>Example:
     *
     * <p>RAWTOHEX(DATA)
     */
    RAWTOHEX {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            String val =
                    Optional.ofNullable(objects.get(0))
                            .map(
                                    item -> {
                                        String origin = item.toString();
                                        int length = origin.length();
                                        StringBuilder buff = new StringBuilder(4 * length);
                                        for (int i = 0; i < length; i++) {
                                            String hex =
                                                    Integer.toHexString(origin.charAt(i) & 0xffff);
                                            for (int j = hex.length(); j < 4; j++) {
                                                buff.append('0');
                                            }
                                            buff.append(hex);
                                        }
                                        return buff.toString();
                                    })
                            .orElse(null);
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * INSERT(originalString, startInt, lengthInt, addString)
     *
     * <p>Inserts an additional string into the original string at a specified start position. The
     * length specifies the number of characters that are removed at the start position in the
     * original string. This method returns a string.
     *
     * <p>Example:
     *
     * <p>INSERT(NAME, 1, 1, ' ')
     */
    INSERT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 4) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 4");
            }

            String origin = FunctionUtils.convertToString(objects.get(0), "");
            String overlay = FunctionUtils.convertToString(objects.get(3), "");
            int start =
                    Optional.ofNullable(objects.get(1))
                            .map(
                                    item -> {
                                        int n = NumberUtils.toInt(item.toString()) - 1;
                                        return Math.max(n, 0);
                                    })
                            .orElse(0);
            int end =
                    Optional.ofNullable(objects.get(2))
                            .map(item -> NumberUtils.toInt(item.toString()) + start)
                            .orElse(0);
            String val = StringUtils.overlay(origin, overlay, start, end);
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * LOWER | LCASE (string)
     *
     * <p>Converts a string to lowercase.
     *
     * <p>Example:
     *
     * <p>LOWER(NAME)
     */
    LOWER {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.lowerOrUpper(types, objects, true);
        }
    },
    LCASE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return LOWER.execute(types, objects);
        }
    },
    /**
     * UPPER | UCASE (string)
     *
     * <p>Converts a string to uppercase.
     *
     * <p>Example:
     *
     * <p>UPPER(NAME)
     */
    UPPER {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.lowerOrUpper(types, objects, false);
        }
    },
    UCASE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return UPPER.execute(types, objects);
        }
    },
    /**
     * LEFT(string, int)
     *
     * <p>Returns the leftmost number of characters.
     *
     * <p>Example:
     *
     * <p>LEFT(NAME, 3)
     */
    LEFT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.leftOrRight(types, objects, true);
        }
    },
    /**
     * RIGHT(string, int)
     *
     * <p>Returns the rightmost number of characters.
     *
     * <p>Example:
     *
     * <p>RIGHT(NAME, 3)
     */
    RIGHT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.leftOrRight(types, objects, false);
        }
    },
    /**
     * LOCATE(searchString, string[, startInit])
     *
     * <p>INSTR(string, searchString[, startInit])
     *
     * <p>POSITION(searchString, string)
     *
     * <p>Returns the location of a search string in a string. If a start position is used, the
     * characters before it are ignored. If position is negative, the rightmost location is
     * returned. 0 is returned if the search string is not found. Please note this function is case-
     * * sensitive, even if the parameters are not.
     *
     * <p>Example:
     *
     * <p>LOCATE('.', NAME)
     */
    LOCATE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.positionCommon(types, objects, false);
        }
    },
    POSITION {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return LOCATE.execute(types, objects);
        }
    },
    INSTR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.positionCommon(types, objects, true);
        }
    },
    /**
     * LPAD(string ,int[, string])
     *
     * <p>Left pad the string to the specified length. If the length is shorter than the string, it
     * will be truncated at the end. If the padding string is not set, spaces will be used.
     *
     * <p>Example:
     *
     * <p>LPAD(AMOUNT, 10, '*')
     */
    LPAD {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.pad(types, objects, true);
        }
    },
    RPAD {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.pad(types, objects, false);
        }
    },
    /**
     * LTRIM(string[, characterToTrimString])
     *
     * <p>Removes all leading spaces or other specified characters from a string.
     *
     * <p>This function is deprecated, use TRIM instead of it.
     *
     * <p>Example:
     *
     * <p>LTRIM(NAME)
     */
    LTRIM {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.trim(types, objects, true, false);
        }
    },
    /**
     * RTRIM(string[, characterToTrimString])
     *
     * <p>Removes all trailing spaces or other specified characters from a string.
     *
     * <p>This function is deprecated, use TRIM instead of it.
     *
     * <p>Example:
     *
     * <p>RTRIM(NAME)
     */
    RTRIM {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.trim(types, objects, false, true);
        }
    },
    /**
     * TRIM(string[, characterToTrimString])
     *
     * <p>Removes all leading spaces or other specified characters from a string.
     *
     * <p>This function is deprecated, use TRIM instead of it.
     *
     * <p>Example:
     *
     * <p>LTRIM(NAME)
     */
    TRIM {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return FunctionUtils.trim(types, objects, true, true);
        }
    },
    /**
     * REGEXP_REPLACE(inputString, regexString, replacementString[, flagsString])
     *
     * <p>Replaces each substring that matches a regular expression. For details, see the Java
     * String.replaceAll() method. If any parameter is null (except optional flagsString parameter),
     * the result is null.
     *
     * <p>Flags values are limited to 'i', 'c', 'n', 'm'. Other symbols cause exception. Multiple
     * symbols could be used in one flagsString parameter (like 'im'). Later flags override first
     * ones, for example 'ic' is equivalent to case-sensitive matching 'c'.
     *
     * <p>'i' enables case-insensitive matching (Pattern.CASE_INSENSITIVE)
     *
     * <p>'c' disables case-insensitive matching (Pattern.CASE_INSENSITIVE)
     *
     * <p>'n' allows the period to match the newline character (Pattern.DOTALL)
     *
     * <p>'m' enables multiline mode (Pattern.MULTILINE)
     *
     * <p>Example:
     *
     * <p>REGEXP_REPLACE('Hello World', ' +', ' ') REGEXP_REPLACE('Hello WWWWorld', 'w+', 'W', 'i')
     */
    REGEXP_REPLACE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 3 || objects.size() > 4) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 3 and 4");
            }
            Object val = objects.get(0);
            if (val != null) {
                String origin = val.toString();
                String regexp = FunctionUtils.convertToString(objects.get(1), "");
                String replacement = FunctionUtils.convertToString(objects.get(2), "");
                String regexpMode = null;
                if (objects.size() == 4) {
                    regexpMode = FunctionUtils.convertToString(objects.get(3), null);
                }
                int flags = FunctionUtils.makeRegexpFlags(regexpMode);
                Matcher matcher =
                        Pattern.compile(regexp, flags).matcher(origin).region(0, origin.length());
                val = matcher.replaceAll(replacement);
            }
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * REGEXP_LIKE(inputString, regexString[, flagsString])
     *
     * <p>Matches string to a regular expression. For details, see the Java Matcher.find() method.
     * If any parameter is null (except optional flagsString parameter), the result is null.
     *
     * <p>Flags values are limited to 'i', 'c', 'n', 'm'. Other symbols cause exception. Multiple
     * symbols could be used in one flagsString parameter (like 'im'). Later flags override first
     * ones, for example 'ic' is equivalent to case-sensitive matching 'c'.
     *
     * <p>'i' enables case-insensitive matching (Pattern.CASE_INSENSITIVE)
     *
     * <p>'c' disables case-insensitive matching (Pattern.CASE_INSENSITIVE)
     *
     * <p>'n' allows the period to match the newline character (Pattern.DOTALL)
     *
     * <p>'m' enables multiline mode (Pattern.MULTILINE)
     *
     * <p>Example:
     *
     * <p>REGEXP_LIKE('Hello World', '[A-Z ]*', 'i')
     */
    REGEXP_LIKE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2 || objects.size() > 3) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 2 and 3");
            }
            Object val = objects.get(0);
            if (val != null) {
                String origin = val.toString();
                String regexp = FunctionUtils.convertToString(objects.get(1), "");
                String regexpMode = null;
                if (objects.size() == 3) {
                    regexpMode = FunctionUtils.convertToString(objects.get(2), null);
                }
                int flags = FunctionUtils.makeRegexpFlags(regexpMode);
                val = Pattern.compile(regexp, flags).matcher(origin).find();
            }
            return Pair.of(BasicType.BOOLEAN_TYPE, val);
        }
    },
    /**
     * REGEXP_SUBSTR(inputString, regexString[, positionInt, occurrenceInt, flagsString, groupInt])
     *
     * <p>Matches string to a regular expression and returns the matched substring. For details, see
     * the java.util.regex.Pattern and related functionality.
     *
     * <p>The parameter position specifies where in inputString the match should start. Occurrence
     * indicates which occurrence of pattern in inputString to search for.
     *
     * <p>Flags values are limited to 'i', 'c', 'n', 'm'. Other symbols cause exception. Multiple
     * symbols could be used in one flagsString parameter (like 'im'). Later flags override first
     * ones, for example 'ic' is equivalent to case-sensitive matching 'c'.
     *
     * <p>'i' enables case-insensitive matching (Pattern.CASE_INSENSITIVE)
     *
     * <p>'c' disables case-insensitive matching (Pattern.CASE_INSENSITIVE)
     *
     * <p>'n' allows the period to match the newline character (Pattern.DOTALL)
     *
     * <p>'m' enables multiline mode (Pattern.MULTILINE)
     *
     * <p>If the pattern has groups, the group parameter can be used to specify which group to
     * return.
     *
     * <p>Example:
     *
     * <p>REGEXP_SUBSTR('2020-10-01', '\d{4}') REGEXP_SUBSTR('2020-10-01',
     * '(\d{4})-(\d{2})-(\d{2})', 1, 1, NULL, 2)
     */
    REGEXP_SUBSTR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2 && objects.size() != 6) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require 2 or 6");
            }
            Object val = objects.get(0);
            if (val != null) {
                String origin = val.toString();
                String regexp = FunctionUtils.convertToString(objects.get(1), "");
                int position = 0;
                int requestedOccurrence = 1;
                int subexpression = 0;
                String regexpMode = null;
                if (objects.size() == 6) {
                    Object obj = objects.get(2);
                    if (obj != null) {
                        position = Integer.parseInt(obj.toString()) - 1;
                    }
                    obj = objects.get(3);
                    if (obj != null) {
                        requestedOccurrence = Integer.parseInt(obj.toString());
                    }
                    obj = objects.get(4);
                    if (obj != null) {
                        regexpMode = obj.toString();
                    }
                    obj = objects.get(5);
                    if (obj != null) {
                        subexpression = Integer.parseInt(obj.toString());
                    }
                }
                int flags = FunctionUtils.makeRegexpFlags(regexpMode);
                Matcher m = Pattern.compile(regexp, flags).matcher(origin);
                boolean found = m.find(position);
                for (int occurrence = 1; occurrence < requestedOccurrence && found; occurrence++) {
                    found = m.find();
                }
                if (found) {
                    val = m.group(subexpression);
                }
            }
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * REPEAT(string, int)
     *
     * <p>Returns a string repeated some number of times.
     *
     * <p>Example:
     *
     * <p>REPEAT(NAME || ' ', 10)
     */
    REPEAT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            Object val = objects.get(0);
            if (val != null) {
                String origin = val.toString();
                int count =
                        Optional.ofNullable(objects.get(1))
                                .map(item -> NumberUtils.toInt(item.toString()))
                                .orElse(0);

                val = StringUtils.repeat(origin, count);
            }
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * REPLACE(string, searchString[, replacementString])
     *
     * <p>Replaces all occurrences of a search string in a text with another string. If no
     * replacement is specified, the search string is removed from the original string. If any
     * parameter is null, the result is null.
     *
     * <p>Example:
     *
     * <p>REPLACE(NAME, ' ')
     */
    REPLACE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2 || objects.size() > 3) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 2 and 3");
            }
            Object val = objects.get(0);
            if (val != null) {
                String origin = val.toString();
                String searchStr = FunctionUtils.convertToString(objects.get(1), null);
                String replaceStr = "";
                if (objects.size() == 3) {
                    replaceStr = FunctionUtils.convertToString(objects.get(2), replaceStr);
                }

                val = StringUtils.replace(origin, searchStr, replaceStr);
            }
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * Returns a four character code representing the sound of a string. This method returns a
     * string, or null if parameter is null. See <a
     * href="https://en.wikipedia.org/wiki/Soundex">...</a> for more information.
     *
     * <p>Example:
     *
     * <p>SOUNDEX(NAME)
     */
    SOUNDEX {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            String val =
                    Optional.ofNullable(objects.get(0))
                            .map(
                                    origin ->
                                            new String(
                                                    FunctionUtils.getSoundex(origin.toString()),
                                                    StandardCharsets.ISO_8859_1))
                            .orElse(null);
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * SPACE(int)
     *
     * <p>Returns a string consisting of a number of spaces.
     *
     * <p>Example:
     *
     * <p>SPACE(80)
     */
    SPACE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            int count =
                    Optional.ofNullable(objects.get(0))
                            .map(item -> NumberUtils.toInt(item.toString()))
                            .orElse(0);
            String val = StringUtils.repeat(StringUtils.SPACE, count);
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * SUBSTRING | SUBSTR SUBSTRING | SUBSTR (string, startInt[, lengthInt ])
     *
     * <p>Returns a substring of a string starting at a position. If the start index is negative,
     * then the start index is relative to the end of the string. The length is optional.
     *
     * <p>Example:
     *
     * <p>CALL SUBSTRING('[Hello]', 2); CALL SUBSTRING('hour', 3, 2);
     */
    SUBSTRING {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2 || objects.size() > 3) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 2 and 3");
            }
            Object val = objects.get(0);
            if (val != null) {
                String origin = val.toString();

                int start =
                        Optional.ofNullable(objects.get(1))
                                .map(
                                        item -> {
                                            int n = NumberUtils.toInt(item.toString());
                                            return n > 0 ? n - 1 : n;
                                        })
                                .orElse(0);
                int end = 0;
                if (StringUtils.isNotEmpty(origin)) {
                    end = origin.length();
                }
                if (objects.size() == 3) {
                    end =
                            Optional.ofNullable(objects.get(2))
                                    .map(
                                            item -> {
                                                int n = NumberUtils.toInt(item.toString());
                                                return start >= 0 ? start + n : start - n;
                                            })
                                    .orElse(0);
                }
                val = StringUtils.substring(origin, start, end);
            }
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    SUBSTR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return SUBSTRING.execute(types, objects);
        }
    },
    /**
     * TO_CHAR(value[, formatString])
     *
     * <p>Oracle-compatible TO_CHAR function that can format a timestamp, a number, or text.
     *
     * <p>Example:
     *
     * <p>CALL TO_CHAR(SYS_TIME, 'yyyy-MM-dd HH:mm:ss')
     */
    TO_CHAR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 1 || objects.size() > 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 1 and 2");
            }
            Object val = objects.get(0);
            if (val != null) {
                if (objects.size() == 2 && val instanceof Temporal) {
                    String format = FunctionUtils.convertToString(objects.get(1), "");
                    TemporalAccessor datetime = (Temporal) val;
                    DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
                    val = df.format(datetime);
                }
                if (val instanceof Number) {
                    val = val.toString();
                }
            }
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    },
    /**
     * TRANSLATE(value, searchString, replacementString)
     *
     * <p>Oracle-compatible TRANSLATE function that replaces a sequence of characters in a string
     * with another set of characters.
     *
     * <p>Example:
     *
     * <p>CALL TRANSLATE('Hello world', 'eo', 'EO')
     */
    TRANSLATE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 3) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 3");
            }
            String val = FunctionUtils.convertToString(objects.get(0), null);
            String searchStr = FunctionUtils.convertToString(objects.get(1), null);
            String replaceStr = FunctionUtils.convertToString(objects.get(2), null);
            val = StringUtils.replaceChars(val, searchStr, replaceStr);
            return Pair.of(BasicType.STRING_TYPE, val);
        }
    };

    public abstract Pair<SeaTunnelDataType<?>, Object> execute(
            List<SeaTunnelDataType<?>> types, List<Object> objects);

    public static Pair<SeaTunnelDataType<?>, Object> execute(
            String funcName, List<SeaTunnelDataType<?>> types, List<Object> objects) {
        for (StringFunctionEnum value : values()) {
            if (value.name().equals(funcName.toUpperCase())) {
                return value.execute(types, objects);
            }
        }
        return null;
    }
}
