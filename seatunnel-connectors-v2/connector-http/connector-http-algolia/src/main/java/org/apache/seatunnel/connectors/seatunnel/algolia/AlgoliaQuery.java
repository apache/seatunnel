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
package org.apache.seatunnel.connectors.seatunnel.algolia;

import java.io.Serializable;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Preserves the provider query form and interprets only the four pagination controls. */
final class AlgoliaQuery implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Set<String> PAGING = Set.of("page", "offset", "length", "hitsPerPage");
    final String raw;
    final Integer page;
    final Integer offset;
    final Integer length;
    final Integer hitsPerPage;

    AlgoliaQuery(String raw) {
        if (raw == null
                || raw.length() > 16000
                || raw.codePoints().anyMatch(Character::isISOControl)) {
            throw invalid();
        }
        this.raw = raw;
        Map<String, String> values = new HashMap<>();
        for (String part : raw.split("&", -1)) {
            if (part.isEmpty() && raw.isEmpty()) {
                break;
            }
            int equals = part.indexOf('=');
            if (equals <= 0) {
                throw invalid();
            }
            String key = decode(part.substring(0, equals));
            String value = decode(part.substring(equals + 1));
            int bracket = key.indexOf('[');
            if (bracket >= 0 && PAGING.contains(key.substring(0, bracket))
                    || PAGING.contains(key) && values.putIfAbsent(key, value) != null) {
                throw invalid();
            }
        }
        page = number(values.get("page"), 0, Integer.MAX_VALUE);
        offset = number(values.get("offset"), 0, Integer.MAX_VALUE);
        length = number(values.get("length"), 1, 1000);
        hitsPerPage = number(values.get("hitsPerPage"), 1, 1000);
        if (page != null && offset != null || length != null && offset == null) {
            throw invalid();
        }
    }

    boolean allPages() {
        return page == null && offset == null;
    }

    String params(int pageNumber) {
        if (!allPages()) {
            return raw;
        }
        return raw + (raw.isEmpty() ? "" : "&") + "page=" + pageNumber;
    }

    private static String decode(String value) {
        try {
            String decoded = URLDecoder.decode(value, StandardCharsets.UTF_8);
            if (decoded.indexOf('�') < 0
                    && decoded.codePoints().noneMatch(Character::isISOControl)) {
                return decoded;
            }
        } catch (IllegalArgumentException ignored) {
            // Preserve the diagnostic without exposing the user query.
        }
        throw invalid();
    }

    private static Integer number(String value, int lower, int upper) {
        if (value == null) {
            return null;
        }
        try {
            if (value.matches("[0-9]+")) {
                int result = Integer.parseInt(value);
                if (result >= lower && result <= upper) {
                    return result;
                }
            }
        } catch (NumberFormatException ignored) {
            // Preserve the diagnostic without exposing the user query.
        }
        throw invalid();
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException(
                "Algolia search_query has invalid or conflicting form pagination parameters");
    }
}
