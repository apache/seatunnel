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
package org.apache.seatunnel.connectors.seatunnel.buzzsprout;

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
import java.util.Objects;

/**
 * Validates Buzzsprout resources, publication window and transport settings.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class BuzzsproutConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("API token from the Buzzsprout profile.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Ordered unique resources: podcasts and episodes.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive RFC 3339 lower bound required for episodes.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive RFC 3339 episode upper bound, defaulting to scan start.");
    static final Option<String> PODCAST_ID =
            Options.key("podcast_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Positive numeric podcast ID required for episodes.");
    static final Option<Boolean> INCLUDE_UNDATED =
            Options.key("include_undated")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Include episodes with no published_at timestamp.");

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
                            "Total list acquisition budget in milliseconds, including retries.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes in one successful list response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTPS origin that requires a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM certificate for the configured custom origin.");

    final String token;
    final Instant start;
    final Instant end;
    final List<BuzzsproutResource> resources;
    final String podcastId;
    final boolean includeUndated;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    BuzzsproutConfig(ReadonlyConfig options) {
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
                || selected.size() > BuzzsproutResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<BuzzsproutResource> unique = EnumSet.noneOf(BuzzsproutResource.class);
        List<BuzzsproutResource> parsed = new ArrayList<>(selected.size());
        for (String name : selected) {
            BuzzsproutResource resource = BuzzsproutResource.from(name);
            if (!unique.add(resource)) {
                throw invalid("resources", "choose each resource only once");
            }
            parsed.add(resource);
        }
        resources = List.copyOf(parsed);
        boolean episodes = unique.contains(BuzzsproutResource.EPISODES);
        podcastId = options.get(PODCAST_ID);
        if ((episodes || podcastId != null)
                && (podcastId == null || !podcastId.matches("[1-9][0-9]{0,127}"))) {
            throw invalid("podcast_id", "expected a positive decimal identifier for episodes");
        }
        String configuredStart = options.get(START_DATE);
        start = configuredStart == null && !episodes ? null : date(configuredStart, START_DATE);
        String configuredEnd = options.get(END_DATE);
        end = configuredEnd == null ? null : date(configuredEnd, END_DATE);
        includeUndated = options.get(INCLUDE_UNDATED);
        scanEnd();
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://www.buzzsprout.com" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(token),
                                resources.toString(),
                                Objects.toString(podcastId, ""),
                                Objects.toString(start, ""),
                                Objects.toString(end, ""),
                                String.valueOf(includeUndated),
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
        if (start != null && !start.isBefore(upper)) {
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
                .required(TOKEN, RESOURCES)
                .optional(
                        PODCAST_ID,
                        START_DATE,
                        END_DATE,
                        INCLUDE_UNDATED,
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
        return new IllegalArgumentException("Buzzsprout option " + key + ": " + message);
    }
}
