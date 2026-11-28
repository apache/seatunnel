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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Locale;

import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.APPLICATION_CHILD_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.MAX_REQUESTS_PER_PASS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.POLL_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.SYNC_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyOptions.TRUST_CERTIFICATE;

/**
 * Read configuration and immutable scope identity for one organization and resource.
 *
 * <p>Checkpoint state retains the fingerprint without the authentication credential.
 */
final class AshbySettings implements Serializable {
    private static final long serialVersionUID = 1L;
    static final int MAX_OPAQUE_LENGTH = 8192;

    final AshbyResource resource;
    final String token;
    final String initialSyncToken;
    final String origin;
    final String trustCertificate;
    final Instant start;
    final int interval;
    final int pageSize;
    final int maxRequests;
    final int childInterval;
    final int maxBytes;
    final int maxAttempts;
    final int timeout;
    // Only this digest identifies the configured scope in checkpoints.
    final String fingerprint;

    AshbySettings(ReadonlyConfig config) {
        resource = AshbyResource.select(config.get(RESOURCE));
        token = config.get(TOKEN);
        if (!opaque(token) || !token.chars().allMatch(c -> c >= 33 && c <= 126 && c != ':')) {
            throw invalid("token", "expected a non-empty Ashby API key");
        }
        initialSyncToken = config.getOptional(SYNC_TOKEN).orElse(null);
        if (initialSyncToken != null && (!resource.syncs() || !opaque(initialSyncToken))) {
            throw invalid("sync.token", "requires a valid token for a resource supporting sync");
        }

        String endpoint = config.getOptional(ENDPOINT).orElse(null);
        trustCertificate = config.getOptional(TRUST_CERTIFICATE).orElse(null);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires a worker trust certificate");
        }
        origin = endpoint == null ? "https://api.ashbyhq.com" : httpsOrigin(endpoint);
        start = date(config.getOptional(START_DATE).orElse(null));
        if (start != null
                && (!(resource.createdFilter() || resource == AshbyResource.APPLICATION_HISTORY)
                        || start.isAfter(Instant.now()))) {
            throw invalid("start_date", "requires an applicable resource and a date in the past");
        }
        interval = range(config.get(POLL_INTERVAL_MS), 0, 86400000, "poll_interval_ms");
        if (interval > 0 && (!resource.syncs() || interval < 100)) {
            throw invalid(
                    "poll_interval_ms",
                    "continuous reads require native sync and at least 100 milliseconds");
        }
        pageSize = range(config.get(PAGE_SIZE), 1, 100, "page_size");
        maxRequests = range(config.get(MAX_REQUESTS_PER_PASS), 1, 1000000, "max_requests_per_pass");
        childInterval =
                range(
                        config.get(APPLICATION_CHILD_INTERVAL_MS),
                        0,
                        30000,
                        "application_child_interval_ms");
        maxBytes =
                range(config.get(MAX_RESPONSE_BYTES), 1024, 64 * 1024 * 1024, "max_response_bytes");
        maxAttempts = range(config.get(MAX_ATTEMPTS), 1, 10, "max_attempts");
        timeout = range(config.get(REQUEST_TIMEOUT_MS), 100, 300000, "request_timeout_ms");
        fingerprint =
                hash(
                        String.join(
                                "\0",
                                resource.key,
                                hash(token),
                                origin,
                                text(trustCertificate),
                                text(start),
                                Integer.toString(pageSize),
                                initialSyncToken == null ? "" : hash(initialSyncToken),
                                Boolean.toString(interval > 0)));
    }

    private static Instant date(String value) {
        if (value == null) {
            return null;
        }
        try {
            if (value.length() > 35 || !value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T.+")) {
                throw invalid("start_date", "expected a four-digit RFC 3339 calendar year");
            }
            Instant time = OffsetDateTime.parse(value).toInstant();
            if (time.getNano() % 1000000 == 0) {
                return time;
            }
        } catch (DateTimeException ignored) {
            // The configured value is not needed in a diagnostic.
        }
        throw invalid("start_date", "expected RFC 3339 with at most millisecond precision");
    }

    private static String httpsOrigin(String raw) {
        try {
            URI value = URI.create(raw);
            if ("https".equals(value.getScheme())
                    && value.getHost() != null
                    && value.getRawUserInfo() == null
                    && value.getRawQuery() == null
                    && value.getRawFragment() == null
                    && (value.getRawPath().isEmpty() || "/".equals(value.getRawPath()))
                    && (value.getPort() == -1
                            || (value.getPort() > 0 && value.getPort() <= 65535))) {
                return "https://"
                        + value.getHost().toLowerCase(Locale.ROOT)
                        + (value.getPort() == -1 ? "" : ":" + value.getPort());
            }
        } catch (IllegalArgumentException ignored) {
            // Configuration errors do not reproduce the supplied origin.
        }
        throw invalid("endpoint", "expected an HTTPS origin without path or credentials");
    }

    static boolean opaque(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= MAX_OPAQUE_LENGTH
                && value.chars().noneMatch(Character::isISOControl);
    }

    static String hash(String value) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    private static int range(int value, int min, int max, String key) {
        if (value < min || value > max) {
            throw invalid(key, "must be between " + min + " and " + max);
        }
        return value;
    }

    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    static IllegalArgumentException invalid(String key, String reason) {
        return new IllegalArgumentException("Ashby " + key + ": " + reason);
    }
}
