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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import org.apache.seatunnel.api.configuration.Option;
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
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.ANALYTICS_START;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.API_ORIGIN;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.DECODED_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.DOWNLOAD_HOSTS;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.FINANCE_START;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.ISSUER_ID;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.MIN_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.RESPONSE_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.REVIEWS_START;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.ROW_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.SALES_START;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.SECRET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.SUBSCRIPTION_EVENT_SKIP;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.SUBSCRIPTION_EVENT_START;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.VENDOR_NUMBER;
import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectSourceOptions.WIRE_LIMIT;

/**
 * Validates and serializes provider settings for the reader.
 *
 * <p>Checkpoint scope excludes all private key bytes and can survive key rotation for the same
 * issuer. A fresh batch freezes its UTC clock before assigning a reader.
 */
final class AppStoreConnectConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // Signed API requests can use only this fixed origin.
    final URI api;

    // Team identity and P-256 key material used only to sign provider requests.
    final String issuer;

    final String keyId;
    final String secretKey;

    // Vendor number is required only for account-wide reports.
    final String vendor;

    final List<AppStoreConnectResource> resources;
    final Set<String> downloadHosts;
    final LocalDate salesStart;
    final LocalDate eventStart;
    final YearMonth financeStart;
    final Set<LocalDate> eventSkip;
    final LocalDate analyticsStart;
    final Instant reviewsStart;

    // Limits apply to a relationship, complete request attempt, or decoded report as named.
    final int pageSize;

    final int maxPages;
    final int interval;
    final int timeout;
    final int operationTimeout;
    final int retries;
    final int jsonLimit;
    final int wireLimit;
    final int decodedLimit;
    final int rowLimit;

    // Nonsecret fingerprint binds a restore to the same logical dataset and pagination.
    final String scope;

    AppStoreConnectConfig(ReadonlyConfig options) {
        api = apiOrigin(options.get(API_ORIGIN));
        issuer = validate(ISSUER_ID, options.get(ISSUER_ID), "[A-Za-z0-9_-]{1,128}");
        keyId = validate(KEY_ID, options.get(KEY_ID), "[A-Za-z0-9_-]{1,128}");
        secretKey = options.get(SECRET_KEY);
        AppStoreConnectJwt.loadKey(secretKey);

        resources = readResources(options.get(RESOURCES));
        String configuredVendor = options.getOptional(VENDOR_NUMBER).orElse(null);
        vendor =
                configuredVendor != null
                                || resources.stream().anyMatch(AppStoreConnectResource::dated)
                        ? validate(VENDOR_NUMBER, configuredVendor, "[0-9]{1,64}")
                        : null;
        Set<String> hosts = new LinkedHashSet<>();
        for (String value : options.get(DOWNLOAD_HOSTS)) {
            hosts.add(
                    validate(DOWNLOAD_HOSTS, value, "[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)*")
                            .toLowerCase(Locale.ROOT));
        }
        downloadHosts = Collections.unmodifiableSet(hosts);
        salesStart = date(options, SALES_START);
        eventStart = date(options, SUBSCRIPTION_EVENT_START);
        financeStart = month(options);
        analyticsStart = date(options, ANALYTICS_START);
        reviewsStart = reviewTime(options);
        Set<LocalDate> skipped = new LinkedHashSet<>();
        List<String> eventDates = options.get(SUBSCRIPTION_EVENT_SKIP);
        if (eventDates.size() > 366) {
            throw invalid(SUBSCRIPTION_EVENT_SKIP);
        }
        for (String raw : eventDates) {
            skipped.add(localDate(SUBSCRIPTION_EVENT_SKIP, raw));
        }
        eventSkip = Collections.unmodifiableSet(skipped);
        pageSize = number(options, PAGE_SIZE, 1, 200);
        maxPages = number(options, MAX_PAGES, 1, 10000);
        interval = number(options, MIN_INTERVAL, 0, 60000);
        timeout = number(options, TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, timeout, 600000);
        retries = number(options, RETRIES, 0, 8);
        jsonLimit = number(options, RESPONSE_LIMIT, 256, 16777216);
        wireLimit = number(options, WIRE_LIMIT, 256, 268435456);
        decodedLimit = number(options, DECODED_LIMIT, 256, 536870912);
        rowLimit = number(options, ROW_LIMIT, 64, Math.min(decodedLimit, 4194304));
        scope =
                hash(
                        String.join(
                                "\n",
                                api.toString(),
                                issuer,
                                String.valueOf(vendor),
                                resources.toString(),
                                String.valueOf(salesStart),
                                String.valueOf(eventStart),
                                String.valueOf(financeStart),
                                eventSkip.toString(),
                                String.valueOf(analyticsStart),
                                String.valueOf(reviewsStart),
                                String.valueOf(pageSize)));
    }

    /**
     * Computes closed period bounds from the saved job clock rather than the restore time.
     *
     * <p>Daily and fiscal month sources enforce independent retention and upper-bound rules.
     */
    LocalDate firstDate(AppStoreConnectResource resource, LocalDate today) {
        if (resource == AppStoreConnectResource.FINANCE_REPORT) {
            YearMonth minimum = YearMonth.from(today).minusMonths(11);
            return (financeStart == null || financeStart.isBefore(minimum) ? minimum : financeStart)
                    .atDay(1);
        }
        LocalDate minimum = today.minusDays(365);
        LocalDate configured =
                resource == AppStoreConnectResource.SUBSCRIPTION_EVENT_REPORT
                        ? eventStart
                        : salesStart;
        return configured == null || configured.isBefore(minimum) ? minimum : configured;
    }

    LocalDate lastDate(AppStoreConnectResource resource, LocalDate today) {
        if (resource == AppStoreConnectResource.FINANCE_REPORT) {
            return today.minusDays(35).withDayOfMonth(1);
        }
        return today.minusDays(
                resource == AppStoreConnectResource.SUBSCRIPTION_EVENT_REPORT ? 2 : 1);
    }

    LocalDate analyticsFirst(LocalDate today) {
        return analyticsStart == null ? today.minusDays(35) : analyticsStart;
    }

    static LocalDate utcDay(Instant time) {
        return time.atOffset(ZoneOffset.UTC).toLocalDate();
    }

    static boolean https(URI uri) {
        return "https".equalsIgnoreCase(uri.getScheme())
                && uri.getHost() != null
                && uri.getRawUserInfo() == null
                && uri.getRawFragment() == null
                && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() <= 65535));
    }

    static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid AppStoreConnect option " + option.key());
    }

    static String hash(String value) {
        return hex(sha256().digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] output = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            output[2 * i] = digits[(bytes[i] & 0xff) >>> 4];
            output[2 * i + 1] = digits[bytes[i] & 0xf];
        }
        return new String(output);
    }

    private static URI apiOrigin(String value) {
        try {
            URI uri = URI.create(value);
            if (https(uri)
                    && uri.getRawQuery() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))) {
                return new URI(
                        "https",
                        null,
                        uri.getHost().toLowerCase(Locale.ROOT),
                        uri.getPort() == 443 ? -1 : uri.getPort(),
                        null,
                        null,
                        null);
            }
        } catch (Exception ignored) {
            // A URI parser exception could include the supplied URI.
        }
        throw invalid(API_ORIGIN);
    }

    private static List<AppStoreConnectResource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(AppStoreConnectResource.values());
        }
        Set<AppStoreConnectResource> result = new LinkedHashSet<>();
        for (String name : names) {
            try {
                if (!result.add(AppStoreConnectResource.valueOf(name.toUpperCase(Locale.ROOT)))) {
                    throw invalid(RESOURCES);
                }
            } catch (IllegalArgumentException ex) {
                throw invalid(RESOURCES);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(result));
    }

    private static String validate(Option<?> key, String value, String pattern) {
        if (value == null || !value.matches(pattern)) {
            throw invalid(key);
        }
        return value;
    }

    private static int number(ReadonlyConfig options, Option<Integer> key, int min, int max) {
        int value = options.get(key);
        if (value < min || value > max) {
            throw invalid(key);
        }
        return value;
    }

    private static LocalDate localDate(Option<?> key, String value) {
        try {
            return LocalDate.parse(validate(key, value, "[0-9]{4}-[0-9]{2}-[0-9]{2}"));
        } catch (DateTimeException ex) {
            throw invalid(key);
        }
    }

    private static LocalDate date(ReadonlyConfig options, Option<String> key) {
        return options.getOptional(key).map(value -> localDate(key, value)).orElse(null);
    }

    private static YearMonth month(ReadonlyConfig options) {
        try {
            return options.getOptional(FINANCE_START)
                    .map(
                            value ->
                                    YearMonth.parse(
                                            validate(FINANCE_START, value, "[0-9]{4}-[0-9]{2}")))
                    .orElse(null);
        } catch (DateTimeException ex) {
            throw invalid(FINANCE_START);
        }
    }

    private static Instant reviewTime(ReadonlyConfig options) {
        try {
            return options.getOptional(REVIEWS_START)
                    .map(OffsetDateTime::parse)
                    .map(OffsetDateTime::toInstant)
                    .orElse(null);
        } catch (DateTimeException ex) {
            throw invalid(REVIEWS_START);
        }
    }
}
