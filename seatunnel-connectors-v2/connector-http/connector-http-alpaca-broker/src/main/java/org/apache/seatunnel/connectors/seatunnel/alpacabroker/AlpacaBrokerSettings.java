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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.ASSET_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.CALENDAR_DATE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.MAX_REQUESTS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.START_TIME;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.USERNAME;

/**
 * Validated immutable request settings. Checkpoint identities exclude secret values.
 *
 * <p>Account and query identity are bound at restore while a rotated secret can continue the same
 * scan.
 */
final class AlpacaBrokerSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String SANDBOX = "https://broker-api.sandbox.alpaca.markets";
    private static final String LIVE = "https://broker-api.alpaca.markets";

    /**
     * Each selector has one specific, fixed Broker API route and response shape.
     *
     * <p>There is no user-controlled HTTP route or method for a resource.
     */
    enum Resource {
        ASSETS("assets"),
        ACCOUNTS_ACTIVITIES("accounts_activities"),
        ACCOUNTS("accounts"),
        ACCOUNT_DOCUMENTS("account_documents"),
        CALENDAR("calendar"),
        CLOCK("clock"),
        COUNTRY_INFO("country-info");

        final String key;

        Resource(String key) {
            this.key = key;
        }

        static Resource parse(String label) {
            for (Resource value : values()) {
                if (value.key.equals(label)) {
                    return value;
                }
            }
            throw invalid("resource", "unknown Broker API resource");
        }
    }

    final Resource resource;
    final String origin;
    final String username;
    final String password;
    final String trustCertificate;
    final Instant startTime;
    final Instant endTime;
    final LocalDate startDate;
    final LocalDate endDate;
    final String assetClass;
    final String calendarDateType;
    final int pageSize;
    final int maxRequests;
    final int timeoutMs;
    final int maxAttempts;
    final int maxResponseBytes;
    final String stateKey;

    AlpacaBrokerSettings(ReadonlyConfig options) {
        resource = Resource.parse(options.get(RESOURCE));
        String environment = options.get(ENVIRONMENT);
        if (!Set.of("SANDBOX", "LIVE").contains(environment)) {
            throw invalid("environment", "expected SANDBOX or LIVE");
        }
        String override = options.getOptional(BASE_URL).orElse(null);
        trustCertificate = options.getOptional(TRUST_CERTIFICATE).orElse(null);
        if ((override != null && (!"SANDBOX".equals(environment) || trustCertificate == null))
                || (override == null && trustCertificate != null)) {
            throw invalid(
                    "base_url",
                    "custom sandbox origin and trust_certificate must be supplied together");
        }
        origin =
                override == null
                        ? ("LIVE".equals(environment) ? LIVE : SANDBOX)
                        : sandboxOrigin(override);
        username = credentials(options.get(USERNAME), "username");
        password = credentials(options.get(PASSWORD), "password");
        if (username.indexOf(':') >= 0) {
            throw invalid("username", "HTTP Basic key ID must exclude a colon");
        }

        startTime = instant(options.getOptional(START_TIME).orElse(null), "start_time");
        endTime = instant(options.getOptional(END_TIME).orElse(null), "end_time");
        startDate = date(options.getOptional(START_DATE).orElse(null), "start_date");
        endDate = date(options.getOptional(END_DATE).orElse(null), "end_date");
        assetClass = options.getOptional(ASSET_CLASS).orElse(null);
        calendarDateType = options.get(CALENDAR_DATE_TYPE);
        validateSelection();

        pageSize = range(options.get(PAGE_SIZE), 1, 100, "page_size");
        maxRequests = range(options.get(MAX_REQUESTS), 1, 100000, "max_requests");
        timeoutMs = range(options.get(REQUEST_TIMEOUT_MS), 100, 300000, "request_timeout_ms");
        maxAttempts = range(options.get(MAX_ATTEMPTS), 1, 8, "max_attempts");
        maxResponseBytes =
                range(
                        options.get(MAX_RESPONSE_BYTES),
                        1024,
                        32 * 1024 * 1024,
                        "max_response_bytes");
        stateKey =
                digest(
                        String.join(
                                "\0",
                                resource.key,
                                origin,
                                digest(username),
                                text(startTime),
                                text(endTime),
                                text(startDate),
                                text(endDate),
                                text(assetClass),
                                calendarDateType,
                                Integer.toString(pageSize)));
    }

    private void validateSelection() {
        if ((startTime != null || endTime != null)
                && resource != Resource.ACCOUNTS
                && resource != Resource.ACCOUNTS_ACTIVITIES) {
            throw invalid("start_time/end_time", "valid only for accounts and accounts_activities");
        }
        if ((startDate != null || endDate != null)
                && resource != Resource.CALENDAR
                && resource != Resource.ACCOUNT_DOCUMENTS) {
            throw invalid("start_date/end_date", "valid only for calendar and account_documents");
        }
        if (startTime != null
                && endTime != null
                && (startTime.isAfter(endTime)
                        || (resource == Resource.ACCOUNTS_ACTIVITIES
                                && startTime.equals(endTime)))) {
            throw invalid("end_time", "invalid timestamp window");
        }
        if (resource == Resource.ACCOUNTS_ACTIVITIES
                && ((startTime != null && startTime.getNano() != 0)
                        || (endTime != null && endTime.getNano() != 0))) {
            throw invalid(
                    "start_time/end_time", "activity bounds must have whole-second precision");
        }
        if (startDate != null && endDate != null && startDate.isAfter(endDate)) {
            throw invalid("end_date", "invalid date window");
        }
        if (!Set.of("trading", "settlement").contains(calendarDateType)
                || (resource != Resource.CALENDAR && !"trading".equals(calendarDateType))) {
            throw invalid(
                    "calendar_date_type",
                    "expected trading or settlement for the calendar resource");
        }
        if (assetClass != null
                && (resource != Resource.ASSETS
                        || !Set.of("us_equity", "crypto", "us_index", "crypto_perp")
                                .contains(assetClass))) {
            throw invalid("asset_class", "expected a supported class for assets");
        }
    }

    private static String sandboxOrigin(String text) {
        try {
            URI uri = URI.create(text);
            String host = uri.getHost();
            String path = uri.getRawPath();
            if (text.length() <= 512
                    && "https".equals(uri.getScheme())
                    && host != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (path.isEmpty() || "/".equals(path))
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() <= 65535))) {
                host = host.toLowerCase(Locale.ROOT);
                if (host.endsWith(".")) {
                    host = host.substring(0, host.length() - 1);
                }
                if (host.equals("alpaca.markets") || host.endsWith(".alpaca.markets")) {
                    throw invalid(
                            "base_url", "select the official Alpaca endpoint with environment");
                }
                return "https://" + host + (uri.getPort() == -1 ? "" : ":" + uri.getPort());
            }
        } catch (IllegalArgumentException ignored) {
            // URI parsing errors can include the submitted address.
        }
        throw invalid("base_url", "expected a custom HTTPS origin without path or credentials");
    }

    private static int range(int value, int min, int max, String key) {
        if (value < min || value > max) {
            throw invalid(key, "must be between " + min + " and " + max);
        }
        return value;
    }

    private static String credentials(String value, String key) {
        if (value == null
                || value.isBlank()
                || value.length() > 2048
                || value.chars().anyMatch(c -> c < 33 || c > 126)) {
            throw invalid(key, "expected non-empty printable ASCII credentials");
        }
        return value;
    }

    private static Instant instant(String text, String key) {
        try {
            return text == null ? null : OffsetDateTime.parse(text).toInstant();
        } catch (DateTimeException e) {
            throw invalid(key, "expected an RFC 3339 timestamp with offset");
        }
    }

    private static LocalDate date(String text, String key) {
        try {
            return text == null ? null : LocalDate.parse(text);
        } catch (DateTimeException e) {
            throw invalid(key, "expected a date in yyyy-MM-dd format");
        }
    }

    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    static String digest(String value) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK does not support SHA-256");
        }
    }

    static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("AlpacaBroker option " + key + ": " + message);
    }
}
