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

package org.apache.seatunnel.connectors.seatunnel.bigmailer;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSourceOptions.TOKEN;

/**
 * Validates connector options and binds saved cursors to the configured API credential and list.
 */
final class BigMailerConfig implements Serializable {
    static final String HOST = "api.bigmailer.io";
    private static final long serialVersionUID = 1L;
    private static final Pattern TOKEN_VALUE = Pattern.compile("[\\x21-\\x7e]{1,512}");
    private static final Pattern BRAND_ID =
            Pattern.compile(
                    "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    final String token;
    final Resource resource;
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    BigMailerConfig(ReadonlyConfig config) {
        token = config.get(TOKEN);
        if (token == null || !TOKEN_VALUE.matcher(token).matches()) {
            throw invalid("token must be a nonempty API key without whitespace");
        }
        resource = Resource.parse(config.get(RESOURCE));
        pageSize = range(config, PAGE_SIZE, 1, 100);
        maxPages = range(config, MAX_PAGES, 1, 10000);
        connectTimeout = range(config, CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(config, READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(config, REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(config, MAX_ATTEMPTS, 1, 10);
        maxBytes = range(config, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        scope =
                BigMailerJson.digestText(
                        "BigMailer/v1/" + resource.id + "/" + pageSize + "/" + token);
    }

    /**
     * Builds a relative read-only target. Provider continuation is encoded as a query parameter.
     */
    String target(Resource list, String brand, String cursor) {
        return list.path(brand)
                + "?limit="
                + pageSize
                + (cursor == null
                        ? ""
                        : "&cursor=" + URLEncoder.encode(cursor, StandardCharsets.UTF_8));
    }

    static boolean validBrand(String value) {
        return value != null && BRAND_ID.matcher(value).matches();
    }

    static boolean validCursor(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= 4096
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.codePoints().noneMatch(Character::isISOControl);
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("BigMailer configuration: " + reason);
    }

    /**
     * The read-only v1 collections, with explicit account or brand ownership.
     *
     * <p>Only a known collection name and a validated UUID can become part of the request path.
     */
    enum Resource {
        BRANDS("brands", "brands", false),
        CONTACTS("contacts", "contacts", true),
        LISTS("lists", "lists", true),
        FIELDS("fields", "fields", true),
        MESSAGE_TYPES("message-types", "message-types", true),
        SEGMENTS("segments", "segments", true),
        BULK_CAMPAIGNS("bulk_campaigns", "bulk-campaigns", true),
        TRANSACTIONAL_CAMPAIGNS("transactional_campaigns", "transactional-campaigns", true),
        SUPPRESSION_LISTS("suppression_lists", "suppression-lists", true),
        USERS("users", "users", false);

        final String id;
        final String collection;
        final boolean brandScoped;

        Resource(String id, String collection, boolean brandScoped) {
            this.id = id;
            this.collection = collection;
            this.brandScoped = brandScoped;
        }

        String path(String brand) {
            if (!brandScoped) {
                return "/v1/" + collection;
            }
            if (!validBrand(brand)) {
                throw invalid("invalid brand UUID for brand-scoped collection");
            }
            return "/v1/brands/" + brand + "/" + collection;
        }

        static Resource parse(String value) {
            for (Resource resource : values()) {
                if (resource.id.equals(value)) {
                    return resource;
                }
            }
            throw invalid("resource must name a supported BigMailer collection");
        }
    }
}
