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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.seatunnel.common.utils.ConfigValueUtils.parseValue;

public class PlaceholderUtils {

    public static final String PLACEHOLDER_STARTER = "${";

    public static String replacePlaceholders(String input, String placeholderName, String value) {
        return replacePlaceholders(input, placeholderName, value, null);
    }

    public static String replacePlaceholders(
            String input, String placeholderName, String value, String defaultValue) {
        String placeholderRegex = "\\$\\{" + Pattern.quote(placeholderName) + "(:[^}]*)?\\}";
        Pattern pattern = Pattern.compile(placeholderRegex);
        Matcher matcher = pattern.matcher(input);

        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement =
                    value != null && !value.isEmpty()
                            ? StringUtils.unwrap(value, "\"")
                            : (matcher.group(1) != null
                                    ? matcher.group(1).substring(1).trim()
                                    : defaultValue);
            if (replacement == null) {
                continue;
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    public static String replacePlaceholders(String input, JsonNode supportedValues) {
        Pattern pattern = Pattern.compile("\\$\\{([^}]*)\\}");
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            String placeholder = matcher.group(1);

            if (supportedValues.has(placeholder)) {
                String replaced = supportedValues.get(placeholder).asText();
                return replacePlaceholders(input, placeholder, replaced);
            }
        }
        return input;
    }

    /**
     * Processes placeholders in the given input string and replaces them with resolved values.
     *
     * <p>A placeholder is defined as {@code ${key}} or {@code ${key:default}}. The resolution
     * priority is: user-provided value > default value > system property > original placeholder.
     *
     * <p>This method also collects placeholder default values into {@code defaultConfigMap} for
     * consistency validation:
     *
     * <ul>
     *   <li>The same placeholder key must not have conflicting default values across the
     *       configuration.
     *   <li>The same placeholder key must not be used with a default value in one place and without
     *       a default value in another.
     * </ul>
     *
     * <p>If the input does not contain any placeholder or no replacement is performed, the original
     * string is returned as-is.
     *
     * @param input the input string potentially containing placeholders, may be null
     * @param isSystemPlaceholder a predicate to determine whether a placeholder key is a system
     *     placeholder (e.g., {@code TablePlaceholder::isSystemPlaceholder}), must not be null
     * @param userConfigMap the user-provided configuration map (typically from {@code -i}
     *     command-line arguments), must not be null
     * @param defaultConfigMap a mutable map to collect and validate placeholder default values
     *     across the entire configuration, must not be null
     * @return the string with placeholders resolved, or the original string if no replacement
     *     occurred
     * @throws IllegalArgumentException if the same placeholder key has conflicting default values,
     *     or if the same key is used inconsistently (with and without default value)
     */
    public static String processPlaceholders(
            String input,
            Predicate<String> isSystemPlaceholder,
            Map<String, String> userConfigMap,
            Map<String, String> defaultConfigMap) {

        Objects.requireNonNull(isSystemPlaceholder, "isSystemPlaceholder predicate cannot be null");
        Objects.requireNonNull(userConfigMap, "userConfigMap cannot be null");
        Objects.requireNonNull(defaultConfigMap, "defaultConfigMap cannot be null");

        if (StringUtils.isBlank(input) || !input.contains(PLACEHOLDER_STARTER)) {
            return input;
        }

        StringBuilder result = new StringBuilder();
        int i = 0;

        while (i < input.length()) {
            int start = input.indexOf(PLACEHOLDER_STARTER, i);
            if (start == -1) {
                result.append(input.substring(i));
                break;
            }

            result.append(input, i, start);

            int keyStart = start + 2;
            boolean optional = false;
            if (keyStart < input.length() && input.charAt(keyStart) == '?') {
                keyStart++;
                optional = true;
            }

            int closePos = ConfigValueUtils.findClosePos(input, keyStart);
            if (closePos == -1) {
                result.append(input.substring(start));
                break;
            }

            int colonPos = input.indexOf(':', keyStart);
            if (colonPos == -1 || colonPos > closePos) {
                colonPos = -1;
            }

            String key;
            String defaultValue = null;
            if (colonPos > 0) {
                key = input.substring(keyStart, colonPos).trim();
                defaultValue = input.substring(colonPos + 1, closePos);
            } else {
                key = input.substring(keyStart, closePos).trim();
            }

            boolean hasDefault = (defaultValue != null);

            if (defaultConfigMap.containsKey(key)) {
                String existingDefault = defaultConfigMap.get(key);
                boolean prevHasDefault = (existingDefault != null);

                if (prevHasDefault != hasDefault) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Inconsistent placeholder usage for key '%s'. "
                                            + "It is used with a default value in one place and without a default value in another. "
                                            + "Please ensure consistent usage across the configuration.",
                                    key));
                }

                if (hasDefault) {
                    if (!existingDefault.equals(defaultValue)) {
                        Object existingObj = parseValue(existingDefault);
                        Object currObj = parseValue(defaultValue);

                        if (!existingObj.equals(currObj)) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Duplicate placeholder key '%s' with conflicting default values. "
                                                    + "Existing: '%s', New: '%s'. Please ensure the exact same default value is used.",
                                            key, existingDefault, defaultValue));
                        }
                    }
                }
            } else {
                defaultConfigMap.put(key, defaultValue);
            }

            String resolvedValue = null;

            // Priority: input > default > system
            if (!isSystemPlaceholder.test(key)) {
                resolvedValue = userConfigMap.get(key);
            }

            String replacement =
                    Stream.of(resolvedValue, defaultValue, System.getProperty(key))
                            .filter(Objects::nonNull)
                            .findFirst()
                            .orElse(optional ? "" : input.substring(start, closePos + 1));

            result.append(replacement);
            i = closePos + 1;
        }

        return result.toString();
    }
}
