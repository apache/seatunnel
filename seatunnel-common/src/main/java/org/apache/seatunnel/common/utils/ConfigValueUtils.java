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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ConfigValueUtils {

    private static final Set<Character> START_DELIMITERS =
            new HashSet<>(Arrays.asList('=', ':', '{', '[', ','));
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
            try {
                Config parsed = ConfigFactory.parseString("v = " + value);
                return parsed.root().get("v");
            } catch (ConfigException e) {
                throw new ConfigException.BadValue(
                        ConfigOriginFactory.newSimple(),
                        String.format(
                                "Value '%s' looks like JSON or Array but failed to parse. "
                                        + "If you intended to pass a Map/List, please check the syntax. "
                                        + "If you intended to pass a plain string with comma, wrap the entire value in double quotes (e.g., \"your_value\"). ",
                                value),
                        e.getMessage());
            }
        }

        return ConfigValueFactory.fromAnyRef(value);
    }

    public static boolean isEscapedQuote(String value, int quoteIndex) {
        int backslashCount = 0;
        int i = quoteIndex - 1;
        while (i >= 0 && value.charAt(i) == '\\') {
            backslashCount++;
            i--;
        }
        return backslashCount % 2 == 1;
    }

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

    private static boolean isStartWrapper(boolean insideQuotes, String value, int quoteIndex) {
        char prev = (quoteIndex > 0) ? value.charAt(quoteIndex - 1) : 0;
        char beforePrev = (quoteIndex > 1) ? value.charAt(quoteIndex - 2) : 0;

        return !insideQuotes
                && (quoteIndex == 0
                        || START_DELIMITERS.contains(prev)
                        || (prev == ' ' && START_DELIMITERS.contains(beforePrev)));
    }

    private static boolean isEndWrapper(boolean insideQuotes, String value, int quoteIndex) {
        char next = (quoteIndex + 1 < value.length()) ? value.charAt(quoteIndex + 1) : 0;
        char afterNext = (quoteIndex + 2 < value.length()) ? value.charAt(quoteIndex + 2) : 0;

        return insideQuotes
                && (quoteIndex == value.length() - 1
                        || END_DELIMITERS.contains(next)
                        || (next == ' ' && END_DELIMITERS.contains(afterNext)));
    }

    public static boolean isStructured(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return false;
        }

        char firstChar = trimmed.charAt(0);
        if (firstChar != '{' && firstChar != '[') {
            return false;
        }

        int braceDepth = 0;
        int bracketDepth = 0;
        boolean insideQuotes = false;

        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);

            if (c == '"') {
                if (isEscapedQuote(trimmed, i)) {
                    continue;
                }
                insideQuotes = updateQuoteState(trimmed, i, insideQuotes);
                continue;
            }

            if (insideQuotes) {
                continue;
            }

            if (c == '{') {
                braceDepth++;
            } else if (c == '}') {
                braceDepth--;
                if (braceDepth < 0) {
                    return false;
                }
            } else if (c == '[') {
                bracketDepth++;
            } else if (c == ']') {
                bracketDepth--;
                if (bracketDepth < 0) {
                    return false;
                }
            }
        }

        if (braceDepth != 0 || bracketDepth != 0 || insideQuotes) {
            return false;
        }

        return true;
    }
}
