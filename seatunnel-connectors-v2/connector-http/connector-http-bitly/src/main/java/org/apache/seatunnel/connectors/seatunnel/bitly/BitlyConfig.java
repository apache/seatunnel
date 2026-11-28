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
package org.apache.seatunnel.connectors.seatunnel.bitly;

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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Validates the token scope, query window and transport limits for a Bitly scan.
 *
 * <p>A hash of the request scope survives checkpoints. The plaintext credential remains in the
 * runtime configuration only.
 */
final class BitlyConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter METRIC_TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssxx").withZone(ZoneOffset.UTC);

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bitly API token, sent as an HTTPS Bearer credential.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Select the Bitly resources to emit: " + BitlyResource.names());
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive lower bound for resource dates, as RFC 3339.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive upper bound for resource dates, as RFC 3339.");
    static final Option<String> QR_MODE =
            Options.key("qr_mode")
                    .stringType()
                    .defaultValue("stored")
                    .withDescription("QR extraction method: stored or legacy_bitlink.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription("Requested page size for Bitlinks and stored QR codes.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages_per_parent")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum pages per paginated resource and parent.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(50000)
                    .withDescription("Maximum logical requests before a scan must finish.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Total attempts per eligible HTTPS request.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Acquisition deadline including retries, in milliseconds.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded response bytes per successful GET.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTPS origin used with an explicit worker-local trust anchor.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to the custom origin's PEM trust anchor on the worker.");

    final String token;
    final Set<BitlyResource> selected;
    final Instant start;
    final Instant end;
    final String metricReference;
    final String origin;
    final String trustCertificate;
    final boolean legacyQr;
    final int pageSize;
    final int maxPages;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String scope;

    BitlyConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 1024
                || token.chars().anyMatch(value -> value < 33 || value > 126)) {
            throw invalid(
                    "token", "expected a non-empty token without spaces or control characters");
        }

        List<String> names = options.get(RESOURCES);
        if (names == null || names.isEmpty() || names.size() > BitlyResource.values().length) {
            throw invalid("resources", "select at least one documented Bitly resource");
        }
        EnumSet<BitlyResource> resources = EnumSet.noneOf(BitlyResource.class);
        for (String name : names) {
            if (!resources.add(BitlyResource.from(name))) {
                throw invalid("resources", "select each resource only once");
            }
        }
        selected = Set.copyOf(resources);

        start = date(options.get(START_DATE), START_DATE);
        end = date(options.get(END_DATE), END_DATE);
        if (!start.isBefore(end)) {
            throw invalid("start_date", "must be earlier than end_date");
        }
        metricReference = METRIC_TIME.format(end);
        String mode = options.get(QR_MODE);
        if (!"stored".equals(mode) && !"legacy_bitlink".equals(mode)) {
            throw invalid("qr_mode", "expected stored or legacy_bitlink");
        }
        legacyQr = "legacy_bitlink".equals(mode);

        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://api-ssl.bitly.com" : normalizeOrigin(endpoint);
        pageSize = range(options, PAGE_SIZE, 1, 100);
        maxPages = range(options, MAX_PAGES, 1, 5000);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
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
                                end.toString(),
                                mode,
                                origin,
                                Objects.toString(trustCertificate, ""),
                                Integer.toString(pageSize)));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, RESOURCES, START_DATE, END_DATE)
                .optional(
                        QR_MODE,
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

    boolean includes(BitlyResource resource) {
        return selected.contains(resource);
    }

    boolean groupsNeeded() {
        return includes(BitlyResource.GROUPS)
                || includes(BitlyResource.GROUP_PREFERENCES)
                || includes(BitlyResource.GROUP_COUNTS)
                || bitlinksNeeded()
                || includes(BitlyResource.QR_CODES);
    }

    boolean bitlinksNeeded() {
        return includes(BitlyResource.BITLINKS) || linksNeededForChildren();
    }

    boolean linksNeededForChildren() {
        return includes(BitlyResource.CLICKS) || (includes(BitlyResource.QR_CODES) && legacyQr);
    }

    boolean inside(Instant timestamp) {
        return !timestamp.isBefore(start) && timestamp.isBefore(end);
    }

    long queryStartSecond() {
        return start.getEpochSecond() - 1;
    }

    long queryEndSecond() {
        return end.getEpochSecond() + (end.getNano() == 0 ? 0 : 1);
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value between " + min + " and " + max);
        }
        return value;
    }

    private static Instant date(String value, Option<String> option) {
        try {
            if (value != null && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T.{1,30}")) {
                Instant instant = OffsetDateTime.parse(value).toInstant();
                if (!instant.isBefore(Instant.parse("0001-01-02T00:00:00Z"))
                        && instant.isBefore(Instant.parse("9999-12-31T00:00:00Z"))) {
                    return instant;
                }
            }
        } catch (DateTimeException ignored) {
            // The user-provided value is intentionally excluded from the option error.
        }
        throw invalid(option.key(), "expected a supported RFC 3339 time with offset");
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
            // A malformed origin might include credentials.
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

    static IllegalArgumentException invalid(String key, String problem) {
        return new IllegalArgumentException("Bitly option " + key + ": " + problem);
    }
}
