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
package org.apache.seatunnel.connectors.seatunnel.callrail;

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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Validates the CallRail resource selection, read window and transport settings.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class CallRailConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CallRail API v3 key from the authorized user profile.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Ordered selection of calls, conversations, users and companies.");
    static final Option<String> ACCOUNT =
            Options.key("account_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Numeric CallRail account ID or masked account identifier.");
    static final Option<String> CALLS_PAGINATION =
            Options.key("calls_pagination")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Calls pagination mode: relative or offset. Other resources always use numbered pages.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive UTC date or RFC 3339 lower bound for dated output.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive UTC date or RFC 3339 upper bound, defaulting to scan start.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum requested records per page, from 1 to 250.");
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
                            "Bare HTTPS origin. Custom origins require a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM certificate for the configured custom origin.");

    final String token;
    final String accountId;
    final boolean relativeCalls;
    final Instant start;
    final Instant end;
    final List<CallRailResource> resources;
    final int pageSize;
    final int maxPages;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    CallRailConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars()
                        .anyMatch(
                                value ->
                                        value < 33
                                                || value > 126
                                                || value == '"'
                                                || value == '\\')) {
            throw invalid(
                    "token", "expected a token without a prefix, spaces or control characters");
        }
        accountId = options.get(ACCOUNT);
        if (accountId == null || !accountId.matches("[A-Za-z0-9]{1,128}")) {
            throw invalid("account_id", "expected a numeric or masked account identifier");
        }
        String paging = options.get(CALLS_PAGINATION);
        if (paging != null && !"relative".equals(paging) && !"offset".equals(paging)) {
            throw invalid("calls_pagination", "choose relative or offset");
        }
        relativeCalls = paging == null ? accountId.matches("[0-9]+") : "relative".equals(paging);
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > CallRailResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<CallRailResource> unique = EnumSet.noneOf(CallRailResource.class);
        List<CallRailResource> parsed = new ArrayList<>(selected.size());
        for (String name : selected) {
            CallRailResource resource = CallRailResource.from(name);
            if (!unique.add(resource)) {
                throw invalid("resources", "choose each resource only once");
            }
            parsed.add(resource);
        }
        resources = List.copyOf(parsed);
        start = date(options.get(START_DATE), START_DATE);
        String until = options.get(END_DATE);
        end = until == null ? null : date(until, END_DATE);
        if (!start.isBefore(end == null ? Instant.now() : end)) {
            throw invalid("start_date", "must be earlier than the scan end");
        }
        pageSize = range(options, PAGE_SIZE, 1, 250);
        maxPages = range(options, MAX_PAGES, 1, 10000);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://api.callrail.com" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(token),
                                accountId,
                                resources.toString(),
                                start.toString(),
                                Objects.toString(end, ""),
                                String.valueOf(relativeCalls),
                                String.valueOf(pageSize),
                                origin,
                                Objects.toString(trustCertificate, "")));
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
            if (text != null && text.length() == 10) {
                return LocalDate.parse(text).atStartOfDay(ZoneOffset.UTC).toInstant();
            }
            if (text != null && text.length() <= 48) {
                return OffsetDateTime.parse(text).toInstant();
            }
        } catch (DateTimeException invalid) {
            // Do not include the untrusted value in a configuration diagnostic.
        }
        throw invalid(option.key(), "expected an ISO date or RFC 3339 timestamp with a time zone");
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, ACCOUNT, RESOURCES, START_DATE)
                .optional(
                        END_DATE,
                        CALLS_PAGINATION,
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
        return new IllegalArgumentException("CallRail option " + key + ": " + message);
    }
}
