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

package org.apache.seatunnel.connectors.seatunnel.boldsign;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.MAX_UNPAGED_RECORDS;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSourceOptions.TOKEN;

/**
 * Validates a BoldSign read and builds its allowed request targets.
 *
 * <p>The only destination is the provider hostname corresponding to the account region.
 */
final class BoldSignConfig implements Serializable {
    static final int DOCUMENT_WINDOW_LIMIT = 10000;
    private static final long serialVersionUID = 1L;
    private static final Pattern KEY = Pattern.compile("[\\x21-\\x7e]{1,512}");
    private static final Map<String, String> ENDPOINTS =
            Map.of(
                    "us",
                    "api.boldsign.com",
                    "eu",
                    "api-eu.boldsign.com",
                    "ca",
                    "api-ca.boldsign.com",
                    "au",
                    "api-au.boldsign.com");

    final String host;
    final String token;
    final Resource resource;
    final Long from;
    final Long until;
    final int pageSize;
    final int maxPages;
    final int maxUnpaged;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    BoldSignConfig(ReadonlyConfig config) {
        String region = config.get(REGION);
        host = region == null ? null : ENDPOINTS.get(region.toLowerCase(Locale.ROOT));
        if (host == null) {
            throw invalid("region must be us, eu, ca, or au");
        }
        token = config.get(TOKEN);
        if (token == null || !KEY.matcher(token).matches()) {
            throw invalid("token must contain a valid API key without whitespace");
        }
        resource = Resource.parse(config.get(RESOURCE));
        from = seconds(config.getOptional(START_DATE).orElse(null), "start_date");
        until = seconds(config.getOptional(END_DATE).orElse(null), "end_date");
        if ((from != null || until != null) && !resource.dated) {
            throw invalid(
                    "creation time options require documents, teams, templates or users_list");
        }
        if (from != null && until != null && from > until) {
            throw invalid("start_date must be before or equal to end_date");
        }
        pageSize = range(config, PAGE_SIZE, 1, 100);
        if (resource == Resource.DOCUMENTS && DOCUMENT_WINDOW_LIMIT % pageSize != 0) {
            throw invalid("page_size must divide 10000 evenly for documents");
        }
        maxPages = range(config, MAX_PAGES, 1, 20000);
        maxUnpaged = range(config, MAX_UNPAGED_RECORDS, 1, 50000);
        connectTimeout = range(config, CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(config, READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(config, REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(config, MAX_ATTEMPTS, 1, 10);
        maxBytes = range(config, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        scope =
                BoldSignJson.digest(
                        host
                                + "|"
                                + token
                                + "|"
                                + resource.id
                                + "|"
                                + pageSize
                                + "|"
                                + from
                                + "|"
                                + until);
    }

    /**
     * Freezes an optional inclusive creation window once, independent of the provider's sort order.
     *
     * @param now the source initialization instant
     * @return the bound saved with this traversal
     */
    Window freeze(Instant now) {
        Long upper = until;
        if (from != null && upper == null) {
            upper = now.getEpochSecond();
        }
        if (from != null && upper != null && from > upper) {
            throw invalid("start_date is after the effective end_date");
        }
        return new Window(from, upper);
    }

    /**
     * Creates exactly one documented read request. Neither creation filter is sent as a transmit
     * date query.
     *
     * @param collection the active approved API collection
     * @param brand optional brand ID from discovery
     * @param page the requested numbered page
     * @param anchor the optional document cursor window anchor
     * @return an encoded, relative request target
     */
    String target(Resource collection, String brand, int page, Long anchor) {
        if (collection == Resource.CUSTOM_FIELDS) {
            if (!validId(brand)) {
                throw invalid("invalid discovered brand ID");
            }
            return collection.path + "?brandId=" + URLEncoder.encode(brand, StandardCharsets.UTF_8);
        }
        if (!collection.paged) {
            return collection.path;
        }
        return collection.path
                + "?Page="
                + page
                + "&PageSize="
                + pageSize
                + (anchor == null ? "" : "&NextCursor=" + anchor);
    }

    static boolean validId(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= 512
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.codePoints().noneMatch(Character::isISOControl);
    }

    private static Long seconds(String text, String option) {
        if (text == null) {
            return null;
        }
        try {
            Instant time = OffsetDateTime.parse(text).toInstant();
            if (time.getNano() == 0) {
                return time.getEpochSecond();
            }
        } catch (DateTimeException ignored) {
            // Use fixed configuration context without echoing option data.
        }
        throw invalid(
                option
                        + " must be an ISO 8601 timestamp with an explicit offset and whole seconds");
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("BoldSign configuration: " + reason);
    }

    /**
     * The eight documented GET collections.
     *
     * <p>Brand lists and custom fields are unpaginated, and only teams use the plural results key.
     */
    enum Resource {
        DOCUMENTS("documents", "/v1/document/list", "result", true, true),
        BRANDS("brands", "/v1/brand/list", "result", false, false),
        SENDER_IDENTITIES("senderIdentities", "/v1/senderIdentities/list", "result", true, false),
        TEAMS("teams", "/v1/teams/list", "results", true, true),
        TEMPLATES("templates", "/v1/template/list", "result", true, true),
        USERS("users_list", "/v1/users/list", "result", true, true),
        CUSTOM_FIELDS("custom_fields", "/v1/customField/list", "result", false, false),
        CONTACTS("contacts", "/v1/contacts/list", "result", true, false);

        final String id;
        final String path;
        final String member;
        final boolean paged;
        final boolean dated;

        Resource(String id, String path, String member, boolean paged, boolean dated) {
            this.id = id;
            this.path = path;
            this.member = member;
            this.paged = paged;
            this.dated = dated;
        }

        static Resource parse(String value) {
            for (Resource resource : values()) {
                if (resource.id.equals(value)) {
                    return resource;
                }
            }
            throw invalid("resource must name a supported BoldSign collection");
        }
    }

    /**
     * Immutable creation-time boundaries in UTC epoch seconds.
     *
     * <p>The bounds are applied locally to dated output records.
     */
    static final class Window implements Serializable {
        private static final long serialVersionUID = 1L;
        final Long from;
        final Long until;

        Window(Long from, Long until) {
            this.from = from;
            this.until = until;
        }
    }
}
