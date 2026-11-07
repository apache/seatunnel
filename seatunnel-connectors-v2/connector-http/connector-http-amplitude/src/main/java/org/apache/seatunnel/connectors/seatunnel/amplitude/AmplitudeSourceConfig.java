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
package org.apache.seatunnel.connectors.seatunnel.amplitude;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonPointer;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.API_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.FIELD_MAPPING;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_ARCHIVE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_EVENT_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_UNCOMPRESSED_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.SECRET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.START;

final class AmplitudeSourceConfig implements Serializable {
    private static final DateTimeFormatter HOUR =
            DateTimeFormatter.ofPattern("uuuuMMdd'T'HH").withResolverStyle(ResolverStyle.STRICT);
    final String apiKey;
    final String secretKey;
    final String start;
    final String end;
    final URI endpoint;
    final int connectTimeout;
    final int readTimeout;
    final int maxRetries;
    final int backoff;
    final int maxBackoff;
    final int maxEventBytes;
    final long maxArchiveBytes;
    final long maxUncompressedBytes;
    final Map<String, String> fieldMapping;

    AmplitudeSourceConfig(ReadonlyConfig options) {
        this(options, Clock.systemUTC());
    }

    AmplitudeSourceConfig(ReadonlyConfig options, Clock clock) {
        apiKey = credential(options, API_KEY);
        secretKey = credential(options, SECRET_KEY);
        if (apiKey.contains(":")) {
            throw invalid("api_key must not contain a colon");
        }
        start = options.get(START);
        end = options.get(END);
        LocalDateTime first = hour(start);
        LocalDateTime last = hour(end);
        if (first.isAfter(last) || ChronoUnit.HOURS.between(first, last) >= 365L * 24) {
            throw invalid("start/end must define an ordered window of at most 365 days");
        }
        // The end is inclusive: allow two hours after the END of the final hour.
        if (last.plusHours(3).toInstant(ZoneOffset.UTC).isAfter(clock.instant())) {
            throw invalid("end hour must have fully elapsed at least two hours ago");
        }
        String region = options.get(REGION);
        if (!"US".equals(region) && !"EU".equals(region)) {
            throw invalid("region must be US or EU");
        }
        endpoint =
                endpoint(
                        options.getOptional(BASE_URL)
                                .orElse(
                                        "US".equals(region)
                                                ? "https://amplitude.com"
                                                : "https://analytics.eu.amplitude.com"),
                        options.get(ALLOW_INSECURE_HTTP));
        connectTimeout = bounded(options, CONNECT_TIMEOUT_MS, 1, 300000);
        readTimeout = bounded(options, READ_TIMEOUT_MS, 1, 600000);
        maxRetries = bounded(options, MAX_RETRIES, 0, 10);
        backoff = bounded(options, RETRY_BACKOFF_MS, 1, 60000);
        maxBackoff = bounded(options, MAX_RETRY_BACKOFF_MS, 1, 600000);
        if (backoff > maxBackoff) {
            throw invalid("retry_backoff_ms must not exceed max_retry_backoff_ms");
        }
        maxEventBytes = bounded(options, MAX_EVENT_BYTES, 1, 16777216);
        maxArchiveBytes = size(options, MAX_ARCHIVE_BYTES);
        maxUncompressedBytes = size(options, MAX_UNCOMPRESSED_BYTES);
        fieldMapping =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                options.getOptional(FIELD_MAPPING).orElse(Collections.emptyMap())));
        for (String pointer : fieldMapping.values()) {
            try {
                JsonPointer.compile(pointer);
            } catch (RuntimeException e) {
                throw invalid("field_mapping values must be RFC 6901 JSON Pointers");
            }
        }
    }

    private static String credential(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null
                || value.trim().isEmpty()
                || value.chars().anyMatch(Character::isISOControl)) {
            throw invalid(option.key() + " must be nonempty and contain no control characters");
        }
        return value;
    }

    private static LocalDateTime hour(String value) {
        try {
            if (value == null || !value.matches("[0-9]{8}T[0-9]{2}")) {
                throw invalid("start/end must be valid uuuuMMddTHH hours");
            }
            return LocalDateTime.parse(value, HOUR);
        } catch (DateTimeParseException e) {
            throw invalid("start/end must be valid uuuuMMddTHH hours");
        }
    }

    private static int bounded(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " is outside its allowed range");
        }
        return value;
    }

    private static long size(ReadonlyConfig options, Option<Long> option) {
        long value = options.get(option);
        if (value < 1 || value > 68719476736L) {
            throw invalid(option.key() + " must be between 1 and 68719476736");
        }
        return value;
    }

    private static URI endpoint(String origin, boolean allowHttp) {
        try {
            URI uri = new URI(origin);
            if (uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (uri.getRawPath() != null
                            && !uri.getRawPath().isEmpty()
                            && !"/".equals(uri.getRawPath()))
                    || (uri.getPort() != -1 && (uri.getPort() < 1 || uri.getPort() > 65535))
                    || (!"https".equals(uri.getScheme())
                            && !(allowHttp && "http".equals(uri.getScheme())))) {
                throw invalid(
                        "base_url must be a trusted HTTPS origin; fixture HTTP requires allow_insecure_http");
            }
            return new URI(
                    uri.getScheme(),
                    null,
                    uri.getHost(),
                    uri.getPort(),
                    "/api/2/export",
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw invalid("base_url must be a valid trusted origin");
        }
    }

    String authorization() {
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString(
                                (apiKey + ":" + secretKey).getBytes(StandardCharsets.UTF_8));
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Amplitude: " + message);
    }
}
