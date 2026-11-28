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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Validated options and the checkpoint identity for a catalog scan.
 *
 * <p>Scan identity includes the selected order and data boundaries. The access key is never placed
 * in split state or diagnostics.
 */
final class AviationstackConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_ORIGIN = "https://api.aviationstack.com";
    static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aviationstack API access key.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Which catalogs to read, in request order: "
                                    + AviationstackResource.names());
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive RFC 3339 airplane delivery time lower bound.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive RFC 3339 airplane delivery time upper bound.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Request this many catalog records per page.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages_per_resource")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum successful pages for each selected catalog.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Total attempts for a page with retryable failures.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Page acquisition deadline in milliseconds including retries.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes per successful response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom HTTPS origin with a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Worker-local PEM trust anchor for a custom HTTPS origin.");

    final String accessKey;
    final List<AviationstackResource> resources;
    final String origin;
    final String trustCertificate;
    final int pageSize;
    final int maxPages;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final Instant start;
    final Instant end;
    // Only the scope digest, rather than the access key, is copied to the split.
    final String scope;

    AviationstackConfig(ReadonlyConfig config) {
        accessKey = config.get(ACCESS_KEY);
        if (accessKey == null
                || accessKey.isBlank()
                || accessKey.length() > 512
                || accessKey.chars().anyMatch(Character::isISOControl)) {
            throw invalid("access_key", "expected a non-empty API key");
        }
        List<String> names = config.get(RESOURCES);
        if (names == null
                || names.isEmpty()
                || names.size() > AviationstackResource.values().length
                || new HashSet<>(names).size() != names.size()) {
            throw invalid("resources", "select each required catalog once");
        }
        resources =
                names.stream()
                        .map(AviationstackResource::from)
                        .collect(Collectors.toUnmodifiableList());
        String override = config.get(ENDPOINT);
        trustCertificate = config.get(TRUST_CERTIFICATE);
        if ((override == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = override == null ? DEFAULT_ORIGIN : normalizeOrigin(override);
        pageSize = range(config.get(PAGE_SIZE), 1, 100, PAGE_SIZE);
        maxPages = range(config.get(MAX_PAGES), 1, 20000, MAX_PAGES);
        attempts = range(config.get(MAX_ATTEMPTS), 1, 10, MAX_ATTEMPTS);
        timeout = range(config.get(TIMEOUT), 100, 300000, TIMEOUT);
        maxBytes = range(config.get(MAX_BYTES), 1024, 64 * 1024 * 1024, MAX_BYTES);
        start = date(config.get(START_DATE), START_DATE);
        end = date(config.get(END_DATE), END_DATE);
        if ((start != null || end != null)
                && !resources.contains(AviationstackResource.AIRPLANES)) {
            throw invalid("start_date/end_date", "a delivery window requires airplanes");
        }
        if (start != null && start.isAfter(end == null ? Instant.now() : end)) {
            throw invalid(
                    "start_date", "must be at or before the end of the airplane delivery window");
        }
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(accessKey),
                                String.join(",", names),
                                origin,
                                Objects.toString(trustCertificate, ""),
                                String.valueOf(pageSize),
                                Objects.toString(start, ""),
                                Objects.toString(end, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(ACCESS_KEY, RESOURCES)
                .optional(
                        START_DATE,
                        END_DATE,
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
     * Chooses the implicit upper bound when the coordinator first owns the split.
     *
     * <p>Persisting it prevents a restart from silently extending the delivery window.
     */
    Instant initialEnd() {
        return end == null && start != null ? Instant.now() : end;
    }

    private static int range(int value, int min, int max, Option<Integer> option) {
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value between " + min + " and " + max);
        }
        return value;
    }

    private static Instant date(String value, Option<String> option) {
        if (value == null) {
            return null;
        }
        try {
            if (value.length() <= 40 && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T.+")) {
                return OffsetDateTime.parse(value).toInstant();
            }
        } catch (DateTimeException ignored) {
            // User data is unnecessary in a validation diagnostic.
        }
        throw invalid(option.key(), "expected an RFC 3339 timestamp with an offset");
    }

    private static String normalizeOrigin(String raw) {
        try {
            URI uri = URI.create(raw);
            if ("https".equals(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getUserInfo() == null
                    && uri.getQuery() == null
                    && uri.getFragment() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() <= 65535))) {
                return "https://"
                        + uri.getHost().toLowerCase(Locale.ROOT)
                        + (uri.getPort() == -1 ? "" : ":" + uri.getPort());
            }
        } catch (IllegalArgumentException ignored) {
            // A malformed URL could itself include an access key.
        }
        throw invalid("endpoint", "expected an HTTPS origin with no query or credentials");
    }

    static String digest(String value) {
        try {
            byte[] hash =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String problem) {
        return new IllegalArgumentException("Aviationstack option " + key + ": " + problem);
    }
}
