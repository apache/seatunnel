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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

enum AkeneoResource {
    PRODUCTS("/products-uuid"),
    CATEGORIES("/categories"),
    FAMILIES("/families"),
    FAMILY_VARIANTS("/families"),
    ATTRIBUTES("/attributes"),
    ATTRIBUTE_GROUPS("/attribute-groups"),
    ASSOCIATION_TYPES("/association-types"),
    CHANNELS("/channels"),
    LOCALES("/locales"),
    CURRENCIES("/currencies"),
    MEASURE_FAMILIES("/measure-families");

    private static final String PREFIX = "/api/rest/v1";
    static final int MAX_CURSOR_CHARS = 4096;
    private final String path;

    AkeneoResource(String path) {
        this.path = path;
    }

    String optionName() {
        return name().toLowerCase(Locale.ROOT);
    }

    static AkeneoResource fromOption(String name) {
        for (AkeneoResource value : values()) {
            if (value.optionName().equals(name)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Akeneo resources contains an unsupported name");
    }

    boolean unpaged(AkeneoConfig config) {
        return this == MEASURE_FAMILIES && !config.legacyMeasurement;
    }

    String path(AkeneoConfig config, String family) {
        if (this == FAMILY_VARIANTS) {
            if (family == null
                    || family.isBlank()
                    || family.length() > 512
                    || ".".equals(family)
                    || "..".equals(family)
                    || family.indexOf('/') >= 0
                    || family.indexOf('\\') >= 0
                    || family.codePoints().anyMatch(Character::isISOControl)) {
                throw failure("invalid family code");
            }
            return PREFIX + path + "/" + encode(family) + "/variants";
        }
        return PREFIX + (unpaged(config) ? "/measurement-families" : path);
    }

    String target(AkeneoConfig config, String family, AkeneoSplit.Position page) {
        String route = path(config, family);
        if (unpaged(config)) {
            return route;
        }
        if (this == PRODUCTS) {
            return route
                    + "?pagination_type=search_after&limit="
                    + config.pageSize
                    + (page.cursor == null ? "" : "&search_after=" + encode(page.cursor));
        }
        return route
                + "?limit="
                + config.pageSize
                + (page.number == 1 ? "" : "&page=" + page.number);
    }

    String identity(JsonNode object) {
        JsonNode id = object.get(this == PRODUCTS ? "uuid" : "code");
        if (id == null
                || !id.isTextual()
                || id.textValue().isBlank()
                || id.textValue().length() > 512
                || id.textValue().codePoints().anyMatch(Character::isISOControl)) {
            throw failure("record has an invalid identity");
        }
        String result = id.textValue();
        if (this == PRODUCTS) {
            try {
                if (result.length() == 36
                        && UUID.fromString(result).toString().equalsIgnoreCase(result)) {
                    return result;
                }
            } catch (IllegalArgumentException ignored) {
                // Do not include the supplied identity.
            }
            throw failure("record has an invalid product UUID");
        }
        return result;
    }

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    Failure failure(String message) {
        return new Failure("Akeneo " + optionName() + ": " + message);
    }

    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super(message);
        }
    }
}
