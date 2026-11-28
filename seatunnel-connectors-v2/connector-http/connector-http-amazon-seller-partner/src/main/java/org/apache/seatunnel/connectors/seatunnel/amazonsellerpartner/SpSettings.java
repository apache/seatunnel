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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.ACCOUNT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.ASIN_GRANULARITY;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.FINANCIAL_EVENTS_STEP;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.FINANCIAL_PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_END_DATETIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_REPLICATION_MODE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_ROLLING_DAYS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INBOUND_START_DATETIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.INCLUDE_PII;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_DONE_REPORT_AGE_HOURS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_RATE_LIMIT_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_REPORT_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_REQUESTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.PERIOD_IN_DAYS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.POLL_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REPORT_LOOKBACK_HOURS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REPORT_OPTIONS_LIST;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REPORT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.SANDBOX_ORIGIN;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.START_TIME;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.STOP_SYNC_ON_RATE_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.TREAT_CANCELLED_AS_EMPTY;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.AmazonSellerPartnerSourceOptions.TRUST_CERTIFICATE;

/**
 * Validated provider, resource, and window settings for a single source split.
 *
 * <p>Restore compares a settings fingerprint. No account credential or report option value is
 * stored in the checkpoint itself.
 */
final class SpSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Map<String, String> MARKETS =
            Map.ofEntries(
                    Map.entry("US", "ATVPDKIKX0DER"),
                    Map.entry("CA", "A2EUQ1WTGCTBG2"),
                    Map.entry("MX", "A1AM78C64UM0Y8"),
                    Map.entry("BR", "A2Q3Y263D00KWC"),
                    Map.entry("AE", "A2VIGQ35RCS4UG"),
                    Map.entry("AU", "A39IBJ37TRP1C6"),
                    Map.entry("BE", "AMEN7PMS3EDWL"),
                    Map.entry("DE", "A1PA6795UKMFR9"),
                    Map.entry("EG", "ARBP9OOSHTCHU"),
                    Map.entry("ES", "A1RKKUPIHCS9HS"),
                    Map.entry("FR", "A13V1IB3VIYZZH"),
                    Map.entry("GB", "A1F83G8C2ARO7P"),
                    Map.entry("UK", "A1F83G8C2ARO7P"),
                    Map.entry("IE", "A28R8C7NBKEWEA"),
                    Map.entry("IN", "A21TJRUUN4KGV"),
                    Map.entry("IT", "APJ6JRA9NG5V4"),
                    Map.entry("JP", "A1VC38T7YXB528"),
                    Map.entry("NL", "A1805IZSGTT6HS"),
                    Map.entry("PL", "A1C3SOZRARQ6R3"),
                    Map.entry("SA", "A17E79C6D8DWNP"),
                    Map.entry("SE", "A2NODRKZP88ZB9"),
                    Map.entry("SG", "A19VAU5U5O7RUS"),
                    Map.entry("TR", "A33AVAJ2PDY3EV"));

    final SpResource resource;
    final String region;
    final String marketplace;
    final String origin;
    final String tokenOrigin;
    final String customOrigin;
    final String trustCertificate;
    final String clientId;
    final String clientSecret;
    final String token;
    final boolean includePii;
    final Instant start;
    final Instant end;
    final int period;
    final Duration financialStep;
    final int financialPageSize;
    final boolean inboundFixed;
    final Instant inboundStart;
    final Instant inboundEnd;
    final int inboundDays;
    final Map<String, String> reportOptions;
    final int reportLookback;
    final int maxDoneAge;
    final boolean cancelledEmpty;
    final boolean stopThrottled;
    final int maxRequests;
    final int maxAttempts;
    final int rateRetries;
    final int requestTimeout;
    final int reportTimeout;
    final int pollInterval;
    final int maxResponseBytes;
    final int maxRows;
    final String fingerprint;

    SpSettings(ReadonlyConfig config) {
        resource = SpResource.select(config.get(RESOURCE));
        String account = config.get(ACCOUNT_TYPE).toUpperCase(Locale.ROOT);
        if (!resource.account.name().equals(account)) {
            throw invalid("account_type", "the selected resource belongs to another account type");
        }
        region = config.get(REGION).toUpperCase(Locale.ROOT);
        marketplace = MARKETS.get(region);
        if (marketplace == null) {
            throw invalid("region", "unsupported marketplace");
        }
        String environment = config.get(ENVIRONMENT);
        boolean sandbox = "SANDBOX".equals(environment);
        if (!sandbox && !"PRODUCTION".equals(environment)) {
            throw invalid("environment", "expected PRODUCTION or SANDBOX");
        }
        trustCertificate = config.getOptional(TRUST_CERTIFICATE).orElse(null);
        String override = config.getOptional(SANDBOX_ORIGIN).orElse(null);
        if ((override == null) != (trustCertificate == null) || (override != null && !sandbox)) {
            throw invalid(
                    "sandbox_origin",
                    "custom sandbox HTTPS and a worker trust certificate are required together");
        }
        customOrigin = override == null ? null : httpsOrigin(override);
        String regionalHost =
                Set.of("US", "CA", "MX", "BR").contains(region)
                        ? "na"
                        : Set.of("AU", "JP", "SG").contains(region) ? "fe" : "eu";
        origin =
                customOrigin != null
                        ? customOrigin
                        : "https://"
                                + (sandbox ? "sandbox." : "")
                                + "sellingpartnerapi-"
                                + regionalHost
                                + ".amazon.com";
        tokenOrigin = customOrigin != null ? customOrigin : "https://api.amazon.com";
        clientId = secret(config.get(CLIENT_ID), "client_id");
        clientSecret = secret(config.get(CLIENT_SECRET), "client_secret");
        token = secret(config.get(TOKEN), "token");
        includePii = config.get(INCLUDE_PII);
        if ((resource.restrictedDocument || resource.api == SpResource.Api.VENDOR_SHIPPING)
                && !includePii) {
            throw invalid("include_pii", "the selected resource requires restricted data access");
        }

        start = instant(config.getOptional(START_TIME).orElse(null), "start_time");
        end = instant(config.getOptional(END_TIME).orElse(null), "end_time");
        validateWindow(start, end, "end_time");
        period = range(config.get(PERIOD_IN_DAYS), 1, 365, "period_in_days");
        financialStep = financialStep(config.get(FINANCIAL_EVENTS_STEP));
        financialPageSize = range(config.get(FINANCIAL_PAGE_SIZE), 1, 100, "financial_page_size");
        String inbound = config.get(INBOUND_REPLICATION_MODE).toUpperCase(Locale.ROOT);
        if (!Set.of("FIXED", "ROLLING").contains(inbound)) {
            throw invalid("inbound_replication_mode", "expected ROLLING or FIXED");
        }
        inboundFixed = "FIXED".equals(inbound);
        inboundStart =
                instant(
                        config.getOptional(INBOUND_START_DATETIME).orElse(null),
                        "inbound_start_datetime");
        inboundEnd =
                instant(
                        config.getOptional(INBOUND_END_DATETIME).orElse(null),
                        "inbound_end_datetime");
        inboundDays = range(config.get(INBOUND_ROLLING_DAYS), 1, 730, "inbound_rolling_days");
        if ((inboundFixed && inboundStart == null)
                || (!inboundFixed && (inboundStart != null || inboundEnd != null))) {
            throw invalid(
                    "inbound_replication_mode",
                    "FIXED requires a start, and only FIXED accepts explicit dates");
        }
        validateWindow(inboundStart, inboundEnd, "inbound_end_datetime");
        reportOptions =
                reportOptions(config.get(REPORT_OPTIONS_LIST), config.get(ASIN_GRANULARITY));
        reportLookback = range(config.get(REPORT_LOOKBACK_HOURS), 0, 720, "report_lookback_hours");
        maxDoneAge =
                range(config.get(MAX_DONE_REPORT_AGE_HOURS), 0, 72, "max_done_report_age_hours");
        cancelledEmpty = config.get(TREAT_CANCELLED_AS_EMPTY);
        stopThrottled = config.get(STOP_SYNC_ON_RATE_LIMIT);
        maxRequests = range(config.get(MAX_REQUESTS), 1, 100000, "max_requests");
        maxAttempts = range(config.get(MAX_ATTEMPTS), 1, 10, "max_attempts");
        rateRetries = range(config.get(MAX_RATE_LIMIT_RETRIES), 0, 9, "max_rate_limit_retries");
        requestTimeout = range(config.get(REQUEST_TIMEOUT_MS), 100, 300000, "request_timeout_ms");
        reportTimeout = range(config.get(REPORT_TIMEOUT_MS), 1000, 14400000, "report_timeout_ms");
        pollInterval = range(config.get(POLL_INTERVAL_MS), 100, 300000, "poll_interval_ms");
        maxResponseBytes =
                range(config.get(MAX_RESPONSE_BYTES), 1024, 64 * 1024 * 1024, "max_response_bytes");
        maxRows = range(config.get(MAX_REPORT_ROWS), 1, 500000, "max_report_rows");

        fingerprint =
                hash(
                        String.join(
                                "\0",
                                resource.name(),
                                account,
                                origin,
                                marketplace,
                                hash(clientId),
                                hash(token),
                                Boolean.toString(includePii),
                                text(start),
                                text(end),
                                Integer.toString(period),
                                financialStep.toString(),
                                Integer.toString(financialPageSize),
                                inbound,
                                text(inboundStart),
                                text(inboundEnd),
                                Integer.toString(inboundDays),
                                optionsFingerprint(reportOptions),
                                Integer.toString(reportLookback),
                                Boolean.toString(cancelledEmpty)));
    }

    private Map<String, String> reportOptions(String raw, String granularity) {
        if (raw.length() > 65536) {
            throw invalid("report_options_list", "exceeds its size limit");
        }
        if (!Set.of("PARENT", "CHILD", "SKU").contains(granularity)) {
            throw invalid("asin_granularity", "expected PARENT, CHILD, or SKU");
        }
        Map<String, String> active = new TreeMap<>(resource.fixedOptions);
        if (resource == SpResource.GET_SALES_AND_TRAFFIC_REPORT
                || resource == SpResource.GET_SALES_AND_TRAFFIC_REPORT_BY_MONTH) {
            active.put("asinGranularity", granularity);
        }
        try {
            JsonNode declarations = SpJson.JSON.readTree(raw);
            if (declarations == null
                    || !declarations.isArray()
                    || declarations.size() > SpResource.values().length) {
                throw invalid("report_options_list", "expected a bounded JSON array");
            }
            Set<SpResource> names = new HashSet<>();
            for (JsonNode declaration : declarations) {
                SpResource report = SpResource.select(declaration.path("stream_name").asText());
                if (!report.isReport()
                        || report.window == SpResource.Window.GENERATED
                        || !names.add(report)
                        || !declaration.path("options_list").isArray()) {
                    throw invalid(
                            "report_options_list", "unsupported report or duplicate stream entry");
                }
                Set<String> options = new HashSet<>();
                for (JsonNode option : declaration.path("options_list")) {
                    String key = option.path("option_name").asText();
                    JsonNode value = option.path("option_value");
                    if (!key.matches("[A-Za-z][A-Za-z0-9_]{0,63}")
                            || !value.isTextual()
                            || value.asText().length() > 8192
                            || !options.add(key)) {
                        throw invalid("report_options_list", "invalid or duplicate option");
                    }
                    if (report == resource) {
                        String fixed = resource.fixedOptions.get(key);
                        if (fixed != null && !fixed.equals(value.asText())) {
                            throw invalid(
                                    "report_options_list",
                                    "cannot override the report view identity");
                        }
                        active.put(key, value.asText());
                    }
                }
            }
        } catch (IOException e) {
            throw invalid("report_options_list", "could not decode JSON");
        }
        if (resource.isVendorDailyAnalytics()) {
            String period = active.putIfAbsent("reportPeriod", "DAY");
            if (period != null && !"DAY".equals(period)) {
                throw invalid("report_options_list", "vendor daily views require reportPeriod DAY");
            }
            if (resource == SpResource.GET_VENDOR_SALES_REPORT
                    || resource == SpResource.GET_VENDOR_INVENTORY_REPORT) {
                requireReportOption(active, "distributorView", Set.of("MANUFACTURING", "SOURCING"));
                requireReportOption(
                        active,
                        "sellingProgram",
                        resource == SpResource.GET_VENDOR_SALES_REPORT
                                ? Set.of("RETAIL", "BUSINESS", "FRESH")
                                : Set.of("RETAIL", "FRESH"));
            }
        }
        return Map.copyOf(active);
    }

    private static void requireReportOption(
            Map<String, String> options, String key, Set<String> valid) {
        String value = options.get(key);
        if (value == null || !valid.contains(value)) {
            throw invalid("report_options_list", "requires a supported " + key);
        }
    }

    private static Duration financialStep(String text) {
        if (!text.matches("[1-9][0-9]{0,2}[HD]")) {
            throw invalid("financial_events_step", "expected a positive hour or day duration");
        }
        long count = Long.parseLong(text.substring(0, text.length() - 1));
        Duration value = text.endsWith("H") ? Duration.ofHours(count) : Duration.ofDays(count);
        if (value.compareTo(Duration.ofDays(180)) > 0) {
            throw invalid("financial_events_step", "maximum is 180 days");
        }
        return value;
    }

    private static void validateWindow(Instant from, Instant to, String key) {
        if (from != null && to != null && !from.isBefore(to)) {
            throw invalid(key, "must be after the start");
        }
    }

    private static String httpsOrigin(String raw) {
        try {
            URI parsed = URI.create(raw);
            if ("https".equals(parsed.getScheme())
                    && parsed.getHost() != null
                    && parsed.getRawUserInfo() == null
                    && parsed.getRawQuery() == null
                    && parsed.getRawFragment() == null
                    && (parsed.getRawPath().isEmpty() || "/".equals(parsed.getRawPath()))
                    && (parsed.getPort() == -1
                            || (parsed.getPort() > 0 && parsed.getPort() <= 65535))) {
                return "https://"
                        + parsed.getHost().toLowerCase(Locale.ROOT)
                        + (parsed.getPort() == -1 ? "" : ":" + parsed.getPort());
            }
        } catch (IllegalArgumentException ignored) {
            // Avoid quoting an operator-supplied URL.
        }
        throw invalid("sandbox_origin", "expected an HTTPS origin without path or credentials");
    }

    private static String secret(String value, String field) {
        if (value == null
                || value.isBlank()
                || value.length() > 8192
                || value.chars().anyMatch(Character::isISOControl)) {
            throw invalid(field, "missing or invalid credential");
        }
        return value;
    }

    private static Instant instant(String value, String field) {
        try {
            Instant time = value == null ? null : OffsetDateTime.parse(value).toInstant();
            if (time != null && time.getNano() != 0) {
                throw invalid(field, "expected whole-second precision");
            }
            return time;
        } catch (DateTimeException e) {
            throw invalid(field, "expected RFC 3339 with an offset");
        }
    }

    static int range(int value, int min, int max, String field) {
        if (value < min || value > max) {
            throw invalid(field, "must be between " + min + " and " + max);
        }
        return value;
    }

    static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    private static String optionsFingerprint(Map<String, String> options) {
        try {
            return SpJson.write(new TreeMap<>(options), "report options");
        } catch (SpFailure e) {
            throw invalid("report_options_list", "could not normalize report options");
        }
    }

    static String hash(String data) {
        try {
            return Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(data.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String detail) {
        return new IllegalArgumentException("AmazonSellerPartner option " + key + ": " + detail);
    }
}
