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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerResource.Report;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * Validates the App ID, timezone, endpoint policy and byte and row limits before requesting data.
 * Checkpoint identity binds the query selection and reporting window without the API token.
 */
final class AppsFlyerConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String HOST = "hq1.appsflyer.com";
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AppsFlyer Pull API v2 bearer token.");
    static final Option<String> APP_ID =
            Options.key("app_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AppsFlyer application ID.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive report start date or raw report timestamp.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive report end date or raw report timestamp.");
    static final Option<String> TIMEZONE =
            Options.key("timezone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("IANA timezone matching the AppsFlyer app setting.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(AppsFlyerResource.names())
                    .withDescription("AppsFlyer resource views to emit.");
    static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://" + HOST)
                    .withDescription("HTTPS origin of the AppsFlyer Pull API.");
    static final Option<List<String>> DOWNLOAD_ORIGINS =
            Options.key("download_origins")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription("Explicit HTTPS origins for delegated report downloads.");
    static final Option<Integer> MAX_WINDOW_RECORDS =
            Options.key("max_window_records")
                    .intType()
                    .defaultValue(100000)
                    .withDescription(
                            "Threshold for splitting raw input or expanded output records within a request window.");
    static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(33554432)
                    .withDescription(
                            "Independent encoded and decoded byte limits per report response.");
    static final Option<Integer> MAX_BUFFER_BYTES =
            Options.key("max_buffer_bytes")
                    .intType()
                    .defaultValue(33554432)
                    .withDescription("UTF-8 payload byte limit for one validated output batch.");
    static final Option<Integer> MAX_FIELD_CHARS =
            Options.key("max_field_chars")
                    .intType()
                    .defaultValue(262144)
                    .withDescription("Maximum characters in a single CSV field.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum HTTP attempts including downloads and retries per reader execution.");
    static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TCP connection timeout in milliseconds.");
    static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription(
                            "TLS handshake and response inactivity timeout in milliseconds.");
    static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(120000)
                    .withDescription(
                            "Total request budget including download redirects and retries.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum attempts per window including the first.");

    final URI origin;
    final Set<String> downloadOrigins;
    final String token;
    final String appId;
    final String encodedApp;
    final ZoneId zone;
    final Instant start;
    final Instant configuredEnd;
    final Set<AppsFlyerResource> selected;
    final List<Report> reports;
    final int maxWindowRecords;
    final int maxResponse;
    final int maxBuffer;
    final int maxField;
    final int maxRequests;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int maxAttempts;
    final String fingerprint;

    AppsFlyerConfig(ReadonlyConfig config) {
        origin = origin(config.get(API_URL));
        if (!HOST.equals(origin.getHost())) {
            throw invalid("api_url must use the AppsFlyer Pull API HTTPS origin");
        }
        Set<String> approved = new TreeSet<>();
        for (String value : config.get(DOWNLOAD_ORIGINS)) {
            approved.add(originKey(origin(value)));
        }
        if (approved.size() > 8) {
            throw invalid("download_origins accepts at most eight HTTPS origins");
        }
        downloadOrigins = Collections.unmodifiableSet(approved);
        token = config.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(c -> c <= 32 || c >= 127)) {
            throw invalid("token must be a nonempty AppsFlyer v2 token");
        }
        appId = config.get(APP_ID);
        if (appId == null
                || appId.isEmpty()
                || appId.length() > 256
                || ".".equals(appId)
                || "..".equals(appId)
                || appId.codePoints()
                        .anyMatch(
                                c ->
                                        c == '/'
                                                || c == '\\'
                                                || c == '%'
                                                || Character.isISOControl(c)
                                                || Character.isWhitespace(c)
                                                || (c >= 0xd800 && c <= 0xdfff))) {
            throw invalid("app_id must be a single application path segment");
        }
        encodedApp = AppsFlyerResource.encode(appId);
        zone = timezone(config.get(TIMEZONE));
        EnumSet<AppsFlyerResource> resources = EnumSet.noneOf(AppsFlyerResource.class);
        EnumSet<Report> orderedReports = EnumSet.noneOf(Report.class);
        for (String name : config.get(RESOURCES)) {
            AppsFlyerResource resource = AppsFlyerResource.named(name);
            if (!resources.add(resource)) {
                throw invalid("resources must not contain duplicates");
            }
            orderedReports.add(resource.report);
        }
        if (resources.isEmpty()) {
            throw invalid("resources must select at least one resource");
        }
        selected = Collections.unmodifiableSet(resources);
        reports = Collections.unmodifiableList(new ArrayList<>(orderedReports));
        boolean aggregate = reports.stream().anyMatch(report -> !report.raw);
        start = time(config.get(START_DATE), "start_date", zone, aggregate);
        configuredEnd =
                config.getOptional(END_DATE)
                        .map(v -> time(v, "end_date", zone, aggregate))
                        .orElse(null);
        if (configuredEnd != null && !configuredEnd.isAfter(start)) {
            throw invalid("end_date must be later than start_date");
        }
        maxWindowRecords = number(config, MAX_WINDOW_RECORDS, 2, 1000000);
        maxResponse = number(config, MAX_RESPONSE_BYTES, 1024, 134217728);
        maxBuffer = number(config, MAX_BUFFER_BYTES, 1024, 134217728);
        maxField = number(config, MAX_FIELD_CHARS, 16, 1048576);
        maxRequests = number(config, MAX_REQUESTS, 1, 100000);
        connectTimeout = number(config, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = number(config, READ_TIMEOUT, 1, 300000);
        requestTimeout = number(config, REQUEST_TIMEOUT, 1, 300000);
        maxAttempts = number(config, MAX_ATTEMPTS, 1, 10);
        String query =
                origin
                        + "|"
                        + downloadOrigins
                        + "|"
                        + encodedApp
                        + "|"
                        + zone
                        + "|"
                        + selected
                        + "|"
                        + start
                        + "|"
                        + configuredEnd
                        + "|"
                        + maxWindowRecords
                        + "|"
                        + maxResponse
                        + "|"
                        + maxBuffer
                        + "|"
                        + maxField;
        fingerprint = digestId(digest().digest(query.getBytes(StandardCharsets.UTF_8)));
    }

    private static Instant time(String value, String name, ZoneId zone, boolean dateOnly) {
        if (value == null) {
            throw invalid(name + " is required");
        }
        try {
            if (value.length() == 10) {
                return LocalDate.parse(value).atStartOfDay(zone).toInstant();
            }
            if (dateOnly) {
                throw invalid(name + " must be yyyy-MM-dd when selecting an aggregate report");
            }
            String iso = value.replace(' ', 'T');
            try {
                OffsetDateTime absolute = OffsetDateTime.parse(iso);
                if (absolute.getNano() == 0) {
                    return absolute.toInstant();
                }
            } catch (DateTimeException ignored) {
                LocalDateTime local = LocalDateTime.parse(iso);
                if (local.getNano() == 0 && zone.getRules().getValidOffsets(local).size() == 1) {
                    return local.toInstant(zone.getRules().getValidOffsets(local).get(0));
                }
            }
        } catch (DateTimeException e) {
            throw invalid(name + " must be a valid report date or timestamp");
        }
        throw invalid(name + " must have an unambiguous local time and second precision");
    }

    private static ZoneId timezone(String value) {
        if (value == null || !ZoneId.getAvailableZoneIds().contains(value)) {
            throw invalid("timezone must be an IANA timezone ID");
        }
        return ZoneId.of(value);
    }

    private static int number(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int n = config.get(option);
        if (n < min || n > max) {
            throw invalid(option.key() + " must be between " + min + " and " + max);
        }
        return n;
    }

    private static URI origin(String value) {
        try {
            URI address = new URI(value);
            if (!validHttps(address)
                    || address.getRawQuery() != null
                    || (!address.getRawPath().isEmpty() && !"/".equals(address.getRawPath()))) {
                throw invalid("API and download origins must be valid HTTPS DNS origins");
            }
            return new URI(
                    "https",
                    null,
                    address.getHost().toLowerCase(Locale.ROOT),
                    address.getPort(),
                    null,
                    null,
                    null);
        } catch (URISyntaxException | NullPointerException e) {
            throw invalid("API and download origins must be valid HTTPS DNS origins");
        }
    }

    static boolean validHttps(URI address) {
        String host = address.getHost();
        return "https".equalsIgnoreCase(address.getScheme())
                && host != null
                && host.contains(".")
                && !host.endsWith(".")
                && host.chars().anyMatch(Character::isLetter)
                && address.getRawUserInfo() == null
                && address.getRawFragment() == null
                && address.getPort() != 0
                && address.getPort() <= 65535;
    }

    static String originKey(URI address) {
        int port = address.getPort() < 0 ? 443 : address.getPort();
        return address.getHost().toLowerCase(Locale.ROOT) + ":" + port;
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String digestId(byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("AppsFlyer source: " + reason);
    }
}
