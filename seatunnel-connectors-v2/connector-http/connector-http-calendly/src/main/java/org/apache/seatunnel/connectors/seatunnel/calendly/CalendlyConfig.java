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

package org.apache.seatunnel.connectors.seatunnel.calendly;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validates a bounded resource read and builds a credential-sensitive restore scope.
 *
 * <p>The file path identifies the rotating OAuth credential store. Its refresh token is never read
 * by the factory or included in a source split.
 */
final class CalendlyConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern CREDENTIAL = Pattern.compile("[\\x21-\\x7e]{1,8192}");
    private static final Pattern CURSOR_DATE =
            Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[^\\s]{9,28}");

    final CalendlyResource resource;
    final String token;
    final String clientId;
    final String clientSecret;
    final String tokenFile;
    final String trustCertificate;
    final Instant start;
    final Instant end;
    final Instant incrementalStart;
    final Instant lower;
    final int lookbackDays;
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    CalendlyConfig(ReadonlyConfig options) {
        resource = CalendlyResource.parse(options.get(CalendlySourceOptions.RESOURCE));
        token = options.getOptional(CalendlySourceOptions.TOKEN).orElse(null);
        clientId = options.getOptional(CalendlySourceOptions.CLIENT_ID).orElse(null);
        clientSecret = options.getOptional(CalendlySourceOptions.CLIENT_SECRET).orElse(null);
        String rawTokenFile =
                options.getOptional(CalendlySourceOptions.OAUTH_TOKEN_FILE).orElse(null);
        if (token != null) {
            if (!validCredential(token)
                    || clientId != null
                    || clientSecret != null
                    || rawTokenFile != null) {
                throw invalid("configure a valid token or the three OAuth options");
            }
            tokenFile = null;
        } else {
            if (!validCredential(clientId)
                    || clientId.indexOf(':') >= 0
                    || !validCredential(clientSecret)
                    || rawTokenFile == null) {
                throw invalid("OAuth requires client_id, client_secret and oauth_token_file");
            }
            tokenFile = absolutePath(rawTokenFile, "oauth_token_file");
        }
        String cert = options.getOptional(CalendlySourceOptions.TLS_TRUST_CERTIFICATE).orElse(null);
        trustCertificate = cert == null ? null : absolutePath(cert, "tls_trust_certificate");
        start = date(options.get(CalendlySourceOptions.START_DATE), "start_date");
        end =
                options.getOptional(CalendlySourceOptions.END_DATE)
                        .map(value -> date(value, "end_date"))
                        .orElse(null);
        incrementalStart =
                options.getOptional(CalendlySourceOptions.INCREMENTAL_START)
                        .map(value -> date(value, "incremental_start"))
                        .orElse(null);
        lookbackDays = range(options, CalendlySourceOptions.LOOKBACK_DAYS, 0, 3650);
        if (incrementalStart != null && resource.cursorField == null) {
            throw invalid("incremental_start requires a resource with a date cursor");
        }
        if (lookbackDays != 0 && resource != CalendlyResource.SCHEDULED_EVENTS) {
            throw invalid("lookback_days is applicable only to scheduled_events");
        }
        if (end != null
                && (end.isBefore(start)
                        || (incrementalStart != null && end.isBefore(incrementalStart)))) {
            throw invalid("end_date must not precede start_date or incremental_start");
        }
        lower = lowerBound();
        pageSize = range(options, CalendlySourceOptions.PAGE_SIZE, 1, 100);
        maxPages = range(options, CalendlySourceOptions.MAX_PAGES, 1, 20000);
        connectTimeout = range(options, CalendlySourceOptions.CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(options, CalendlySourceOptions.READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(options, CalendlySourceOptions.REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(options, CalendlySourceOptions.MAX_ATTEMPTS, 1, 10);
        maxBytes = range(options, CalendlySourceOptions.MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        String auth =
                token != null
                        ? CalendlyJson.fingerprint(List.of("pat", token))
                        : CalendlyJson.fingerprint(
                                List.of("oauth", clientId, clientSecret, tokenFile));
        scope =
                CalendlyJson.fingerprint(
                        List.of(
                                "calendly-v1",
                                auth,
                                resource.id,
                                start.toString(),
                                String.valueOf(end),
                                String.valueOf(incrementalStart),
                                Integer.toString(lookbackDays),
                                Integer.toString(pageSize)));
    }

    private Instant lowerBound() {
        Instant floor =
                resource == CalendlyResource.EVENT_TYPES
                                || resource == CalendlyResource.SCHEDULED_EVENTS
                        ? Instant.EPOCH
                        : start;
        if (incrementalStart == null) {
            return floor;
        }
        Duration overlap = Duration.ZERO;
        if (resource == CalendlyResource.SCHEDULED_EVENTS) {
            overlap = Duration.ofDays(lookbackDays).plusMinutes(5);
        } else if (resource == CalendlyResource.MEETING_RECAPS) {
            overlap = Duration.ofDays(3);
        }
        Instant candidate = incrementalStart.minus(overlap);
        return candidate.isBefore(floor) ? floor : candidate;
    }

    private static Instant date(String value, String key) {
        try {
            if (value != null && CURSOR_DATE.matcher(value).matches()) {
                return OffsetDateTime.parse(value).toInstant();
            }
        } catch (DateTimeException ignored) {
            // Keep configuration values out of exceptions.
        }
        throw invalid(key + " must be an RFC 3339 instant");
    }

    private static String absolutePath(String value, String key) {
        try {
            if (!value.isBlank() && value.length() <= 4096) {
                Path path = Path.of(value);
                if (path.isAbsolute() && path.normalize().getFileName() != null) {
                    return path.normalize().toString();
                }
            }
        } catch (InvalidPathException ignored) {
            // Invalid user paths can contain secrets.
        }
        throw invalid(key + " must be an absolute engine-local file path");
    }

    static boolean validCredential(String value) {
        return value != null && CREDENTIAL.matcher(value).matches();
    }

    /**
     * Accepts bounded opaque UTF-8 continuation text without normalization.
     *
     * @param value the provider continuation
     * @return whether it can be transmitted safely
     */
    static boolean validCursor(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= 8192
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.getBytes(StandardCharsets.UTF_8).length <= 8192;
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        Integer value = config.get(option);
        if (value == null || value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Calendly configuration: " + reason);
    }
}
