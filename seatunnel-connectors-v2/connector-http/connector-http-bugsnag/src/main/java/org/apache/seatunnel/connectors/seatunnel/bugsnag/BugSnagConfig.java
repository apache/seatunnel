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
package org.apache.seatunnel.connectors.seatunnel.bugsnag;

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
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Validates the BugSnag resource selection, read window and transport settings.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class BugSnagConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BugSnag personal Data Access authentication token.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Selected BugSnag Data Access resources.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive RFC 3339 lower bound for dated output.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive RFC 3339 upper bound, defaulting to scan start.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Number of records per paged request, at most 100 and 10 for releases.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages_per_scope")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum number of successful pages per resource and parent scope.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum successfully processed API responses in this scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum attempts per page including the initial HTTPS request.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total page acquisition budget in milliseconds, including retries.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes in one successful response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "BugSnag SaaS origin or a custom HTTPS origin requiring a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM certificate for the configured custom origin.");

    final String token;
    final Instant start;
    final Instant end;
    final List<BugSnagResource> resources;
    final int pageSize;
    final int maxPages;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    BugSnagConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(value -> value < 33 || value > 126)) {
            throw invalid(
                    "token", "expected a token without a prefix, spaces or control characters");
        }
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > BugSnagResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<BugSnagResource> parsed = EnumSet.noneOf(BugSnagResource.class);
        for (String name : selected) {
            if (!parsed.add(BugSnagResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        resources = List.copyOf(parsed);
        start = date(options.get(START_DATE), START_DATE);
        String until = options.get(END_DATE);
        end = until == null ? null : date(until, END_DATE);
        if (!start.isBefore(end == null ? Instant.now() : end)) {
            throw invalid("start_date", "must be earlier than the scan end");
        }
        pageSize = range(options, PAGE_SIZE, 1, 100);
        maxPages = range(options, MAX_PAGES, 1, 10000);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        origin = endpoint == null ? "https://api.bugsnag.com" : httpsOrigin(endpoint);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        boolean saas =
                "https://api.bugsnag.com".equals(origin)
                        || "https://api.bugsnag.smartbear.com".equals(origin);
        if ((!saas && trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())
                || (endpoint == null && trustCertificate != null)) {
            throw invalid(
                    "endpoint", "custom HTTPS requires trust_certificate and an explicit origin");
        }
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(token),
                                resources.toString(),
                                start.toString(),
                                Objects.toString(end, ""),
                                origin,
                                Objects.toString(trustCertificate, ""),
                                String.valueOf(pageSize)));
    }

    /**
     * Freezes the upper bound once when creating the unassigned split.
     *
     * <p>All paged work and recovery use the same instant.
     */
    Instant scanEnd() {
        Instant upper = end == null ? Instant.now() : end;
        if (!start.isBefore(upper)) {
            throw invalid("start_date", "must be earlier than the scan end");
        }
        return upper;
    }

    private static Instant date(String text, Option<String> option) {
        try {
            if (text != null && text.length() <= 48) {
                return OffsetDateTime.parse(text).toInstant();
            }
        } catch (DateTimeException invalid) {
            // Do not include the untrusted value in a configuration diagnostic.
        }
        throw invalid(option.key(), "expected an RFC 3339 timestamp with a time zone");
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, RESOURCES, START_DATE)
                .optional(
                        END_DATE,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    private static String httpsOrigin(String value) {
        try {
            URI uri = URI.create(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() <= 65535))
                    && ("".equals(uri.getRawPath()) || "/".equals(uri.getRawPath()))) {
                return "https://" + uri.getRawAuthority();
            }
        } catch (IllegalArgumentException ignored) {
            // The invalid option value may contain secrets and must not enter diagnostics.
        }
        throw invalid("endpoint", "expected a bare HTTPS origin");
    }

    private static int range(ReadonlyConfig settings, Option<Integer> option, int min, int max) {
        int value = settings.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value from " + min + " to " + max);
        }
        return value;
    }

    static String digest(String text) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(sha256().digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("BugSnag option " + key + ": " + message);
    }
}
