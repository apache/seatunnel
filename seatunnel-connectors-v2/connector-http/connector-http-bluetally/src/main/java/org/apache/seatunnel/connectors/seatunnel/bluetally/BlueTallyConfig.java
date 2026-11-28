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
package org.apache.seatunnel.connectors.seatunnel.bluetally;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Validates BlueTally’s resource, tenant, window and transport settings.
 *
 * <p>The split stores a scope hash rather than a copy of the authentication token.
 */
final class BlueTallyConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BlueTally API key sent as a Bearer token.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Ordered list of selected BlueTally resources: assets and employees.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive RFC 3339 update-time lower bound.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive RFC 3339 update-time upper bound, defaulting to scan start.");
    static final Option<Integer> TENANT =
            Options.key("tenant_id")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Positive tenant identifier for a multi-tenant account.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription("BlueTally limit, from 1 to 1000 records.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages_per_resource")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum successful pages per resource, including the last empty page.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Total attempts per request for eligible responses.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Maximum page acquisition time in milliseconds, including retries.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes per successful page.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTPS origin requiring a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Worker-local PEM for the configured custom origin.");

    final String token;
    final List<BlueTallyResource> resources;
    final Instant start;
    final Instant end;
    final Integer tenant;
    final int pageSize;
    final int maxPages;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    BlueTallyConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 1024
                || token.chars().anyMatch(value -> value < 33 || value > 126)) {
            throw invalid(
                    "token", "expected a non-empty API key without spaces or control characters");
        }
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > BlueTallyResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        List<BlueTallyResource> parsed = new ArrayList<>(selected.size());
        EnumSet<BlueTallyResource> unique = EnumSet.noneOf(BlueTallyResource.class);
        for (String name : selected) {
            BlueTallyResource resource = BlueTallyResource.from(name);
            if (!unique.add(resource)) {
                throw invalid("resources", "choose each resource only once");
            }
            parsed.add(resource);
        }
        resources = List.copyOf(parsed);
        start = parseDate(options.get(START_DATE), START_DATE);
        String requestedEnd = options.get(END_DATE);
        end = requestedEnd == null ? null : parseDate(requestedEnd, END_DATE);
        if (!start.isBefore(end == null ? Instant.now() : end)) {
            throw invalid("start_date", "must be earlier than the update window end");
        }
        tenant = options.get(TENANT);
        if (tenant != null && tenant <= 0) {
            throw invalid("tenant_id", "expected a positive 32-bit tenant identifier");
        }
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://app.bluetallyapp.com" : httpsOrigin(endpoint);
        pageSize = range(options, PAGE_SIZE, 1, 1000);
        maxPages = range(options, MAX_PAGES, 1, 10000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(token),
                                resources.toString(),
                                start.toString(),
                                Objects.toString(end, ""),
                                Objects.toString(tenant, ""),
                                origin,
                                Objects.toString(trustCertificate, ""),
                                String.valueOf(pageSize)));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, RESOURCES, START_DATE)
                .optional(
                        END_DATE,
                        TENANT,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    /**
     * Selects a default upper bound when the split is created.
     *
     * <p>Each resource uses this same saved upper bound after restoration.
     */
    Instant initialEnd() {
        return end == null ? Instant.now() : end;
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int lower, int upper) {
        int value = options.get(option);
        if (value < lower || value > upper) {
            throw invalid(option.key(), "expected a value between " + lower + " and " + upper);
        }
        return value;
    }

    private static Instant parseDate(String date, Option<String> option) {
        try {
            if (date != null && date.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T.{1,30}")) {
                return OffsetDateTime.parse(date).toInstant();
            }
        } catch (DateTimeException ignored) {
            // User-provided values are intentionally excluded from validation messages.
        }
        throw invalid(option.key(), "expected an RFC 3339 timestamp with an offset");
    }

    private static String httpsOrigin(String value) {
        try {
            URI uri = URI.create(value);
            int port = uri.getPort();
            if ("https".equals(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getUserInfo() == null
                    && uri.getQuery() == null
                    && uri.getFragment() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (port == -1 || (port > 0 && port <= 65535))) {
                return "https://"
                        + uri.getHost().toLowerCase(Locale.ROOT)
                        + (port == -1 ? "" : ":" + port);
            }
        } catch (IllegalArgumentException ignored) {
            // An invalid URI could expose a credential.
        }
        throw invalid("endpoint", "expected an HTTPS origin without path, query or credentials");
    }

    static String digest(String value) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String option, String reason) {
        return new IllegalArgumentException("BlueTally option " + option + ": " + reason);
    }
}
