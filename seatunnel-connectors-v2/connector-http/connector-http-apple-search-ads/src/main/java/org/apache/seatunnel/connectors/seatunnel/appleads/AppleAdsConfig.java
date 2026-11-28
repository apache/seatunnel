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
package org.apache.seatunnel.connectors.seatunnel.appleads;

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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.ACCOUNT;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.API;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.CURSOR_DATE;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.LOOKBACK;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.MODE;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.OAUTH;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.ORG;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.ORG_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.TIMEZONE;
import static org.apache.seatunnel.connectors.seatunnel.appleads.AppleAdsSourceOptions.WINDOW;

/**
 * Validates explicit API protocol, account namespace and batch calendar settings.
 *
 * <p>Changing the reporting scope rejects an old split. OAuth credentials are independent of that
 * scope and can rotate between requests or restored jobs.
 */
final class AppleAdsConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Platform and legacy API namespaces use different account IDs and wire contracts.
     *
     * <p>The reader never switches mode automatically in response to an API error.
     */
    enum Mode {
        PLATFORM_V1("https://api.ads.apple.com", "adAccountId"),
        LEGACY_V5("https://api.searchads.apple.com", "orgId");

        final String defaultOrigin;
        final String contextKey;

        Mode(String defaultOrigin, String contextKey) {
            this.defaultOrigin = defaultOrigin;
            this.contextKey = contextKey;
        }
    }

    // TLS auth and Apple Ads use independent authorities and phase-specific headers.
    final Mode mode;
    final URI origin;
    final URI oauth;
    final String accountId;
    final String clientId;
    final String clientSecret;

    // Report periods follow a closed UTC or validated organizational calendar.
    final List<AppleAdsResource> resources;
    final LocalDate start;
    final LocalDate end;
    final LocalDate cursor;
    final String timezone;
    final ZoneId calendar;
    final int lookback;
    final int window;

    // Per-partition and per-operation limits cap retained data and time spent on remote reads.
    final int pageSize;
    final int maxPages;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int bodyLimit;

    // A state fingerprint binds accepted offsets to their source mode and account.
    final String scope;

    AppleAdsConfig(ReadonlyConfig options) {
        mode = mode(options.get(MODE));
        Option<String> account = mode == Mode.PLATFORM_V1 ? ACCOUNT : ORG;
        Option<String> other = mode == Mode.PLATFORM_V1 ? ORG : ACCOUNT;
        if (options.getOptional(other).isPresent()) {
            throw invalid(other);
        }
        accountId = account(options.getOptional(account).orElse(null), account);
        origin = origin(options.getOptional(API).orElse(mode.defaultOrigin), API);
        oauth = origin(options.get(OAUTH), OAUTH);
        clientId = text(options.get(CLIENT_ID), 1024, CLIENT_ID);
        clientSecret = text(options.get(CLIENT_SECRET), 16384, CLIENT_SECRET);
        resources = resources(options.get(RESOURCES));
        start = date(options, START_DATE);
        end = date(options, END_DATE);
        cursor = date(options, CURSOR_DATE);
        if (resources.stream().anyMatch(AppleAdsResource::report) && start == null) {
            throw invalid(START_DATE);
        }
        if (start != null && end != null && end.isBefore(start)) {
            throw invalid(END_DATE);
        }
        if (cursor != null && start == null) {
            throw invalid(CURSOR_DATE);
        }
        timezone = options.get(TIMEZONE);
        calendar = calendar(timezone, options.getOptional(ORG_ZONE).orElse(null));
        lookback = number(options, LOOKBACK, 0, 30);
        window = number(options, WINDOW, 1, 90);
        pageSize = number(options, PAGE_SIZE, 1, 1000);
        maxPages = number(options, MAX_PAGES, 1, 100000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        bodyLimit = number(options, BODY_LIMIT, 1024, 33554432);
        scope =
                fingerprint(
                        String.join(
                                "\n",
                                mode.name(),
                                origin.toString(),
                                accountId,
                                resources.toString(),
                                String.valueOf(start),
                                String.valueOf(end),
                                String.valueOf(cursor),
                                timezone,
                                calendar.getId(),
                                String.valueOf(lookback),
                                String.valueOf(window),
                                String.valueOf(pageSize)));
    }

    boolean platform() {
        return mode == Mode.PLATFORM_V1;
    }

    /**
     * Freezes the effective first daily-report date using a caller-supplied lookback cursor.
     *
     * <p>A fresh batch with no cursor starts at start_date. Same-job recovery resumes saved
     * positions without applying a second lookback.
     */
    LocalDate first(Instant cutoff) {
        if (start == null) {
            return null;
        }
        LocalDate lagged = cursor == null ? start : cursor.minusDays(lookback);
        LocalDate first = lagged.isBefore(start) ? start : lagged;
        if (resources.stream().anyMatch(AppleAdsResource::report)
                && first.isBefore(LocalDate.ofInstant(cutoff, calendar).minusDays(90))) {
            throw invalid(START_DATE);
        }
        return first;
    }

    LocalDate last(Instant cutoff) {
        LocalDate closed = LocalDate.ofInstant(cutoff, calendar).minusDays(1);
        if (end != null && end.isAfter(closed)) {
            throw invalid(END_DATE);
        }
        if (cursor != null && cursor.isAfter(closed)) {
            throw invalid(CURSOR_DATE);
        }
        return end == null ? closed : end;
    }

    static String fingerprint(String text) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(text.getBytes(StandardCharsets.UTF_8));
            char[] hex = "0123456789abcdef".toCharArray();
            char[] characters = new char[digest.length * 2];
            for (int i = 0; i < digest.length; i++) {
                characters[2 * i] = hex[(digest[i] & 255) >>> 4];
                characters[2 * i + 1] = hex[digest[i] & 15];
            }
            return new String(characters);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("AppleAds SHA-256 is unavailable");
        }
    }

    private static URI origin(String value, Option<String> key) {
        try {
            URI uri = URI.create(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawFragment() == null
                    && uri.getRawQuery() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (uri.getPort() == -1 || uri.getPort() > 0 && uri.getPort() <= 65535)) {
                return new URI(
                        "https",
                        null,
                        uri.getHost().toLowerCase(Locale.ROOT),
                        uri.getPort() == 443 ? -1 : uri.getPort(),
                        null,
                        null,
                        null);
            }
        } catch (Exception ex) {
            // A URI parser exception could include its original value.
        }
        throw invalid(key);
    }

    private static Mode mode(String value) {
        try {
            return Mode.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw invalid(MODE);
        }
    }

    private static List<AppleAdsResource> resources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(AppleAdsResource.values());
        }
        Set<AppleAdsResource> parsed = new LinkedHashSet<>();
        for (String name : names) {
            try {
                if (!parsed.add(AppleAdsResource.valueOf(name.toUpperCase(Locale.ROOT)))) {
                    throw invalid(RESOURCES);
                }
            } catch (IllegalArgumentException ex) {
                throw invalid(RESOURCES);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(parsed));
    }

    private static ZoneId calendar(String timezone, String zone) {
        if ("UTC".equals(timezone)) {
            if (zone != null) {
                throw invalid(ORG_ZONE);
            }
            return ZoneOffset.UTC;
        }
        if (!"ORTZ".equals(timezone)) {
            throw invalid(TIMEZONE);
        }
        if (zone == null || !ZoneId.getAvailableZoneIds().contains(zone)) {
            throw invalid(ORG_ZONE);
        }
        try {
            return ZoneId.of(zone);
        } catch (DateTimeException ex) {
            throw invalid(ORG_ZONE);
        }
    }

    private static LocalDate date(ReadonlyConfig options, Option<String> option) {
        String input = options.getOptional(option).orElse(null);
        if (input == null) {
            return null;
        }
        try {
            if (input.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                return LocalDate.parse(input);
            }
        } catch (DateTimeException ex) {
            // Keep raw option values out of diagnostics.
        }
        throw invalid(option);
    }

    private static String account(String value, Option<String> setting) {
        try {
            if (value != null && value.matches("[1-9][0-9]{0,18}") && Long.parseLong(value) > 0) {
                return value;
            }
        } catch (NumberFormatException ex) {
            // Account IDs must fit Apple's int64 type.
        }
        throw invalid(setting);
    }

    private static String text(String value, int limit, Option<String> setting) {
        if (value == null
                || value.isEmpty()
                || value.length() > limit
                || !value.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw invalid(setting);
        }
        return value;
    }

    private static int number(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option);
        }
        return value;
    }

    private static IllegalArgumentException invalid(Option<?> setting) {
        return new IllegalArgumentException("Invalid AppleAds option " + setting.key());
    }
}
