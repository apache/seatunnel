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

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigOriginFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConfigValueUtils {

    // Characters that may appear right before a start quote.
    private static final Set<Character> START_DELIMITERS =
            new HashSet<>(Arrays.asList('=', ':', '{', '[', ','));

    // Characters that may appear right after an end quote.
    private static final Set<Character> END_DELIMITERS =
            new HashSet<>(Arrays.asList(',', '}', ']', ':'));

    private ConfigValueUtils() {}

    /**
     * Parses a raw string value into a {@link ConfigValue}.
     *
     * <p>The parsing rules are:
     *
     * <ul>
     *   <li>{@code null} is converted to a {@code ConfigValue} holding {@code null}.
     *   <li>A value wrapped in double quotes is unwrapped and treated as a plain string, e.g.
     *       {@code "123"} becomes the string {@code 123} rather than a number.
     *   <li>A value that starts with {@code "{"} and ends with {@code "}"}, or starts with {@code
     *       "["} and ends with {@code "]"}, is parsed as a JSON object or array.
     *   <li>Any other value is treated as a plain string.
     * </ul>
     *
     * @param value the raw string value to parse
     * @return the parsed {@link ConfigValue}
     * @throws ConfigException.BadValue if the value looks like a JSON object or array but fails to
     *     parse
     */
    public static ConfigValue parseValue(String value) {
        if (value == null) {
            return ConfigValueFactory.fromAnyRef(null);
        }

        if (value.isEmpty()) {
            return ConfigValueFactory.fromAnyRef("");
        }

        if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
            value = StringUtils.unwrap(value, "\"");
            return ConfigValueFactory.fromAnyRef(value);
        }

        if (isStructured(value)) {
            return parseStructured(value);
        }

        return ConfigValueFactory.fromAnyRef(value);
    }

    public static ConfigValue parseStructured(String value) {
        try {
            Config parsed = ConfigFactory.parseString("v = " + value);
            ConfigValue parsedValue = parsed.root().get("v");
            // check ambiguous format in []
            // config file : option=[${name}]
            // -i name=[0-9] -> k:string "[0-9]"
            // -i name=["0-9"]-> k: a list contains single element "0-9",`"` is not a part of the
            // element
            if (parsedValue instanceof List) {
                ConfigList parsedList = (ConfigList) parsedValue;
                String content = value.substring(0, value.length() - 1);
                if ((parsedList).size() == 1) {
                    if (content.startsWith("\"") && content.endsWith("\"")) {
                        return parsedValue;
                    } else {
                        return ConfigValueFactory.fromAnyRef(value);
                    }
                } else {
                    // check type of elements, element of string type must be wrapped in quotes
                    List<Object> elements = (List<Object>) parsedValue.unwrapped();
                    for (int i = 0; i < elements.size(); i++) {
                        Object element = elements.get(i);
                        if (element instanceof String) {
                            String quotedElement = StringUtils.wrap((String) element, "\"");
                            String originElement = content.split(",")[i];
                            if (!quotedElement.equals(originElement)) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Element: '%s' of string type in list: '%s' must be wrapped in quotes",
                                                originElement, value));
                            }
                        }
                    }
                }
            }
            return parsedValue;
        } catch (ConfigException e) {
            throw new ConfigException.BadValue(
                    ConfigOriginFactory.newSimple(),
                    "",
                    String.format(
                            "Value '%s' looks like JSON or Array but failed to parse. "
                                    + "If you intended to pass a Map/List, please check the syntax. "
                                    + "If you intended to pass a plain string with comma, wrap the entire value in double quotes (e.g., \"your_value\"). ",
                            value),
                    e);
        }
    }

    /**
     * Returns {@code true} if the quote character at {@code quoteIndex} is escaped by a preceding
     * backslash.
     *
     * <p>A quote is treated as escaped when it is preceded by an odd number of consecutive
     * backslashes, e.g. {@code \"} is escaped, {@code \\"} is not.
     *
     * @param value the user input string via {@code -i}
     * @param quoteIndex index of the current quote character in {@code value}
     * @return {@code true} if the quote at {@code quoteIndex} is escaped
     */
    public static boolean isEscapedQuote(String value, int quoteIndex) {
        int backslashCount = 0;
        int i = quoteIndex - 1;
        while (i >= 0 && value.charAt(i) == '\\') {
            backslashCount++;
            i--;
        }
        return backslashCount % 2 == 1;
    }

    /**
     * Returns the updated "inside quotes" state after scanning the character at {@code quoteIndex}
     * in {@code value}.
     *
     * @param value the user input string being scanned
     * @param quoteIndex index of the current character in {@code value}
     * @param insideQuotes the quote state before processing this character
     * @return the quote state after processing this character
     */
    public static boolean updateQuoteState(String value, int quoteIndex, boolean insideQuotes) {

        boolean isStartWrapper = isStartWrapper(insideQuotes, value, quoteIndex);
        boolean isEndWrapper = isEndWrapper(insideQuotes, value, quoteIndex);

        if (isStartWrapper) {
            insideQuotes = true;
        } else if (isEndWrapper) {
            insideQuotes = false;
        } else {
            insideQuotes = !insideQuotes;
        }
        return insideQuotes;
    }

    /**
     * Checks if the quote at {@code quoteIndex} is a start wrapper: not inside quotes, and preceded
     * by a {@link #START_DELIMITERS} or the start of string.
     *
     * @param insideQuotes whether the caller is currently inside quotes
     * @param value the string being scanned
     * @param quoteIndex index of the quote character to check
     * @return {@code true} if the quote starts a quoted region
     */
    private static boolean isStartWrapper(boolean insideQuotes, String value, int quoteIndex) {
        char prev = (quoteIndex > 0) ? value.charAt(quoteIndex - 1) : 0;
        char beforePrev = (quoteIndex > 1) ? value.charAt(quoteIndex - 2) : 0;

        return !insideQuotes
                && (quoteIndex == 0
                        || START_DELIMITERS.contains(prev)
                        || (prev == ' ' && START_DELIMITERS.contains(beforePrev)));
    }

    /**
     * Checks if the quote at {@code quoteIndex} is an end wrapper: inside quotes, and followed by
     * an {@link #END_DELIMITERS} or the end of string.
     *
     * <p>Checks are ordered: an already-closed quote state short-circuits first, then the
     * end-of-string case, then the immediate next character, then the "space + end delimiter" form.
     * A sentinel {@code 0} is used for missing following characters at the string boundary, so they
     * never match a delimiter.
     *
     * @param insideQuotes whether the caller is currently inside quotes
     * @param value the string being scanned
     * @param quoteIndex index of the quote character to check
     * @return {@code true} if the quote ends a quoted region
     */
    private static boolean isEndWrapper(boolean insideQuotes, String value, int quoteIndex) {
        char next = (quoteIndex + 1 < value.length()) ? value.charAt(quoteIndex + 1) : 0;
        char afterNext = (quoteIndex + 2 < value.length()) ? value.charAt(quoteIndex + 2) : 0;

        return insideQuotes
                && (quoteIndex == value.length() - 1
                        || END_DELIMITERS.contains(next)
                        || (next == ' ' && END_DELIMITERS.contains(afterNext)));
    }

    /**
     * Checks whether the value should be parsed as a structured value (Map or List).
     *
     * <p>Returns {@code true} only if the value is wrapped in matching {@code {...}} or {@code
     * [...]}, all brackets are balanced (ignoring quotes), and—for objects—at least one top-level
     * {@code ':'} or {@code '='} is present. Arrays are not element-validated; {@code [1]}, {@code
     * [0-9]}, and {@code []} are structured. Empty objects/arrays are structured.
     *
     * <p>{@code {"a":1}} and {@code [1,2]} return {@code true}; {@code {x}}, {@code {{x}}}, and
     * {@code {"a"}} return {@code false}.
     *
     * @param value user input string value
     * @return {@code true} if the value should be parsed as a structured value
     */
    public static boolean isStructured(String value) {
        if (value.length() < 2) return false;

        char first = value.charAt(0);
        char last = value.charAt(value.length() - 1);
        boolean isObject = first == '{' && last == '}';
        boolean isArray = first == '[' && last == ']';
        if (!isObject && !isArray) return false;

        Deque<Character> stack = new ArrayDeque<>();
        boolean insideQuotes = false;
        boolean hasKeyValue = false;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (c == '"' && !isEscapedQuote(value, i)) {
                insideQuotes = !insideQuotes;
                continue;
            }
            if (insideQuotes) continue;

            if (c == '{' || c == '[') {
                stack.push(c);
            } else if (c == '}' || c == ']') {
                if (stack.isEmpty()) return false;
                char open = stack.pop();
                if ((c == '}' && open != '{') || (c == ']' && open != '[')) {
                    return false;
                }
            } else if ((c == ':' || c == '=') && stack.size() == 1) {
                hasKeyValue = true;
            }
        }

        if (insideQuotes || !stack.isEmpty()) return false;

        String inner = value.substring(1, value.length() - 1).trim();
        if (inner.isEmpty()) return true;

        return !isObject || hasKeyValue;
    }
}
