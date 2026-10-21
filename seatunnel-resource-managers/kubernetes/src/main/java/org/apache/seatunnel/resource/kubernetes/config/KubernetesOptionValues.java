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

package org.apache.seatunnel.resource.kubernetes.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared parsing of comma-separated option values used by application and Session mode. */
final class KubernetesOptionValues {

    private KubernetesOptionValues() {}

    /**
     * Parses a comma-separated list, rejecting empty entries.
     *
     * @param value configured value, or {@code null} when absent
     * @param optionKey option name used in error messages
     * @return immutable values in configuration order
     */
    static List<String> parseList(String value, String optionKey) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (String item : value.split(",")) {
            String trimmed = item.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException(optionKey + " must not contain empty entries");
            }
            result.add(trimmed);
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Parses comma-separated {@code key:value} pairs while keeping escaped commas in values.
     *
     * @param value configured value, or {@code null} when absent
     * @param optionKey option name used in error messages
     * @return immutable pairs in configuration order
     */
    static Map<String, String> parsePairs(String value, String optionKey) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (String pair : splitPairs(value)) {
            int separator = pair.indexOf(':');
            if (separator <= 0 || separator == pair.length() - 1) {
                throw new IllegalArgumentException(
                        optionKey + " must contain comma-separated key:value pairs");
            }
            String key = pair.substring(0, separator).trim();
            String configuredValue = pair.substring(separator + 1).trim().replace("\\,", ",");
            if (key.isEmpty()
                    || configuredValue.isEmpty()
                    || result.put(key, configuredValue) != null) {
                throw new IllegalArgumentException(
                        optionKey + " contains an empty or duplicate key:value pair");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static List<String> splitPairs(String value) {
        List<String> pairs = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (escaped) {
                current.append('\\').append(character);
                escaped = false;
            } else if (character == '\\') {
                escaped = true;
            } else if (character == ',') {
                pairs.add(current.toString());
                current.setLength(0);
            } else {
                current.append(character);
            }
        }
        if (escaped) {
            current.append('\\');
        }
        pairs.add(current.toString());
        return pairs;
    }
}
