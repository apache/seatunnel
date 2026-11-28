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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** Maps each exported resource to its fixed read-only operation and identity field. */
enum AlgoliaResource {
    INDICES("items", "name"),
    AVAILABLE_LANGUAGES(null, null),
    LOGS("logs", "sha1"),
    INDEXES_QUERY("hits", "objectID"),
    INDEXES_SETTINGS(null, null);

    static final int LOG_HORIZON = 1000;
    final String array;
    final String key;

    AlgoliaResource(String array, String key) {
        this.array = array;
        this.key = key;
    }

    boolean child() {
        return this == INDEXES_QUERY || this == INDEXES_SETTINGS;
    }

    String optionName() {
        return name().toLowerCase(Locale.ROOT);
    }

    static AlgoliaResource forName(String name) {
        for (AlgoliaResource value : values()) {
            if (value.optionName().equals(name)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Algolia resources contains an unsupported name");
    }

    String target(AlgoliaConfig config, String index, AlgoliaSplit.Cursor cursor) {
        switch (this) {
            case INDICES:
                return "/1/indexes?page=" + cursor.page + "&hitsPerPage=" + config.indicesSize;
            case AVAILABLE_LANGUAGES:
                return "/1/dictionaries/*/languages";
            case LOGS:
                return "/1/logs?offset="
                        + cursor.offset
                        + "&length="
                        + Math.min(config.logsSize, LOG_HORIZON - cursor.offset);
            case INDEXES_QUERY:
                return "/1/indexes/" + encoded(index) + "/query";
            case INDEXES_SETTINGS:
                return "/1/indexes/" + encoded(index) + "/settings";
            default:
                throw failure("unknown route");
        }
    }

    private String encoded(String value) {
        if (value == null
                || value.isBlank()
                || value.length() > 512
                || ".".equals(value)
                || "..".equals(value)
                || value.indexOf('/') >= 0
                || value.indexOf('\\') >= 0
                || value.codePoints().anyMatch(Character::isISOControl)) {
            throw failure("invalid index name");
        }
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    Failure failure(String message) {
        return new Failure("Algolia " + optionName() + ": " + message);
    }

    /**
     * Carries connector-owned diagnostic text without provider payloads, headers or request
     * parameters.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super(message);
        }
    }
}
