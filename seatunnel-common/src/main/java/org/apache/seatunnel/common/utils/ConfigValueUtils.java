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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigOriginFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
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

    /**
     * Parses a string value into a structured ConfigValue (e.g., JSON object or Array).
     *
     * <p><b>Parsing Semantics:</b> Attempts to interpret the input as HOCON/JSON. Plain strings
     * with commas must be wrapped in double quotes.
     *
     * <p><b>Error Semantics:</b> Throws {@link StructuredValueParseException} on failure. The
     * exception contains <b>no raw value content</b> and a sanitized cause message. Callers must
     * catch this exception and append the configuration key/location context.
     *
     * @param value The raw string value to parse.
     * @return The parsed ConfigValue.
     * @throws StructuredValueParseException if the value is malformed.
     */
    public static ConfigValue parseStructured(String value) {
        try {
            Config parsed = ConfigFactory.parseString("v = " + value);
            return parsed.root().get("v");
        } catch (ConfigException e) {
            String sanitizedDetail = sanitizeExceptionDetail(e);
            throw new StructuredValueParseException("Structured Value", sanitizedDetail, e);
        }
    }

    /**
     * Parses a string value with configuration key context for better error reporting.
     *
     * <p>This method wraps {@link #parseStructured(String)} and converts parsing failures into a
     * {@link ConfigException.BadValue} that includes the key/location, while strictly excluding the
     * raw value content to prevent sensitive data leakage.
     *
     * @param key The configuration key (used for error reporting).
     * @param value The raw string value to parse.
     * @return The parsed ConfigValue.
     * @throws ConfigException.BadValue if the value is malformed.
     */
    public static ConfigValue parseStructuredValueForKey(String key, String value) {
        try {
            return parseStructured(value);
        } catch (StructuredValueParseException e) {
            // TODO: check if the key is sensitive field
            String errorMessage =
                    String.format(
                            "Failed to parse value for key: %s. %s. "
                                    + "If you intended to pass a Map/List, please check the syntax. "
                                    + "If you intended to pass a plain string with comma, wrap the entire value in double quotes (e.g., \"your_value\"). ",
                            key, e.getMessage());
            throw new ConfigException.BadValue(
                    ConfigOriginFactory.newSimple(), key, errorMessage, e);
        }
    }

    private static String sanitizeExceptionDetail(ConfigException e) {
        String msg = e.getMessage();
        if (msg == null) return e.getClass().getSimpleName();
        return msg;
    }

    /**
     * Exception thrown when a raw string value fails to parse as a structured format.
     *
     * <p><b>Error Semantics:</b> This exception intentionally excludes the raw input value to
     * prevent sensitive data leakage. It only reports the expected syntax category and a sanitized
     * parsing detail. The caller is responsible for providing the configuration key/location
     * context.
     */
    public static class StructuredValueParseException extends RuntimeException {

        private final String syntaxCategory;

        public StructuredValueParseException(
                String syntaxCategory, String sanitizedDetail, Throwable cause) {
            super(
                    String.format(
                            "Expected syntax category: [%s]. Parsing detail: %s",
                            syntaxCategory, sanitizedDetail),
                    cause);
            this.syntaxCategory = syntaxCategory;
        }

        public String getSyntaxCategory() {
            return syntaxCategory;
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
