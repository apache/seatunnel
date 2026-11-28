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
package org.apache.seatunnel.connectors.seatunnel.beamer;

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
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;

/**
 * Validated settings for a bounded scan of Beamer NPS responses.
 *
 * <p>The checkpoint identity describes the request scope and omits the plaintext credential.
 */
final class BeamerConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_ORIGIN = "https://api.getbeamer.com";

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Beamer API key sent in the Beamer-Api-Key header.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive RFC 3339 creation-time lower bound.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive RFC 3339 creation-time upper bound, defaulting to job start.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Beamer maxResults, from 1 to 100.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum successful page requests including the terminating empty page.");
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
                            "Page acquisition deadline in milliseconds, including retries.");
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

    final String token;
    final String origin;
    final String trustCertificate;
    final int pageSize;
    final int maxPages;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final Instant start;
    final Instant end;
    final String scope;

    BeamerConfig(ReadonlyConfig config) {
        token = config.get(TOKEN);
        // Reject characters OkHttp cannot encode in an HTTP header before creating a request.
        if (token == null
                || token.isEmpty()
                || token.length() > 512
                || token.chars().anyMatch(value -> value < 33 || value > 126)) {
            throw invalid(
                    "token", "expected a non-empty API key without spaces or control characters");
        }
        String override = config.get(ENDPOINT);
        trustCertificate = config.get(TRUST_CERTIFICATE);
        if ((override == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = override == null ? DEFAULT_ORIGIN : normalizeOrigin(override);
        pageSize = range(config.get(PAGE_SIZE), 1, 100, PAGE_SIZE);
        maxPages = range(config.get(MAX_PAGES), 1, 5000, MAX_PAGES);
        attempts = range(config.get(MAX_ATTEMPTS), 1, 10, MAX_ATTEMPTS);
        timeout = range(config.get(TIMEOUT), 100, 300000, TIMEOUT);
        maxBytes = range(config.get(MAX_BYTES), 1024, 64 * 1024 * 1024, MAX_BYTES);
        start = date(config.get(START_DATE), START_DATE);
        end = config.get(END_DATE) == null ? null : date(config.get(END_DATE), END_DATE);
        if (!start.isBefore(end == null ? Instant.now() : end)) {
            throw invalid("start_date", "must be earlier than the end of the creation window");
        }
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(token),
                                origin,
                                Objects.toString(trustCertificate, ""),
                                String.valueOf(pageSize),
                                start.toString(),
                                Objects.toString(end, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, START_DATE)
                .optional(
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
     * Moves the provider lower bound far enough back to cover the requested inclusive boundary.
     *
     * <p>Filtering the returned records against the exact bounds also permits a provider that
     * rounds the supplied ISO-8601 precision to whole seconds.
     */
    Instant queryStart() {
        return start.truncatedTo(ChronoUnit.SECONDS).minusSeconds(1);
    }

    /**
     * Rounds the provider upper bound outwards to a whole second.
     *
     * <p>The reader always filters against the original exclusive boundary.
     */
    static Instant queryEnd(Instant end) {
        return end.getNano() == 0 ? end : end.truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
    }

    /**
     * Selects the implicit upper bound once when the coordinator creates the scan.
     *
     * <p>Restoring an existing scan continues to use its saved upper bound.
     */
    Instant initialEnd() {
        return end == null ? Instant.now() : end;
    }

    private static int range(int value, int min, int max, Option<Integer> option) {
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value between " + min + " and " + max);
        }
        return value;
    }

    private static Instant date(String value, Option<String> option) {
        try {
            if (value != null
                    && value.length() <= 40
                    && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T.+")) {
                Instant instant = OffsetDateTime.parse(value).toInstant();
                if (!instant.isBefore(Instant.parse("0001-01-02T00:00:00Z"))
                        && instant.isBefore(Instant.parse("9999-12-31T00:00:00Z"))) {
                    return instant;
                }
            }
        } catch (DateTimeException ignored) {
            // Invalid input may contain sensitive information.
        }
        throw invalid(option.key(), "expected a supported RFC 3339 timestamp with an offset");
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
            // A malformed URL might itself contain a secret.
        }
        throw invalid("endpoint", "expected an HTTPS origin without a path, query or credentials");
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
        return new IllegalArgumentException("Beamer option " + key + ": " + problem);
    }
}
